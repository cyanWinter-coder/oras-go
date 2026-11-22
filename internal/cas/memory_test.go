/*
Copyright The ORAS Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cas

import (
	"bytes"
	"context"
	_ "crypto/sha256"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/errdef"
)

func TestMemorySuccess(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}

	s := NewMemory()
	ctx := context.Background()

	err := s.Push(ctx, desc, bytes.NewReader(content))
	if err != nil {
		t.Fatal("Memory.Push() error =", err)
	}

	exists, err := s.Exists(ctx, desc)
	if err != nil {
		t.Fatal("Memory.Exists() error =", err)
	}
	if !exists {
		t.Errorf("Memory.Exists() = %v, want %v", exists, true)
	}

	rc, err := s.Fetch(ctx, desc)
	if err != nil {
		t.Fatal("Memory.Fetch() error =", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal("Memory.Fetch().Read() error =", err)
	}
	err = rc.Close()
	if err != nil {
		t.Error("Memory.Fetch().Close() error =", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("Memory.Fetch() = %v, want %v", got, content)
	}
	if got := len(s.Map()); got != 1 {
		t.Errorf("Memory.Map() = %v, want %v", got, 1)
	}
}

func TestMemoryNotFound(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}

	s := NewMemory()
	ctx := context.Background()

	exists, err := s.Exists(ctx, desc)
	if err != nil {
		t.Error("Memory.Exists() error =", err)
	}
	if exists {
		t.Errorf("Memory.Exists() = %v, want %v", exists, false)
	}

	_, err = s.Fetch(ctx, desc)
	if !errors.Is(err, errdef.ErrNotFound) {
		t.Errorf("Memory.Fetch() error = %v, want %v", err, errdef.ErrNotFound)
	}
}

func TestMemoryAlreadyExists(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}

	s := NewMemory()
	ctx := context.Background()

	err := s.Push(ctx, desc, bytes.NewReader(content))
	if err != nil {
		t.Fatal("Memory.Push() error =", err)
	}

	err = s.Push(ctx, desc, bytes.NewReader(content))
	if !errors.Is(err, errdef.ErrAlreadyExists) {
		t.Errorf("Memory.Push() error = %v, want %v", err, errdef.ErrAlreadyExists)
	}
}

func TestMemoryBadPush(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}

	s := NewMemory()
	ctx := context.Background()

	err := s.Push(ctx, desc, strings.NewReader("foobar"))
	if err == nil {
		t.Errorf("Memory.Push() error = %v, wantErr %v", err, true)
	}
}

// countingReader reports how many times its content was read to completion.
type countingReader struct {
	data    []byte
	pos     int
	started *atomic.Int32
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.pos == 0 && r.started != nil {
		r.started.Add(1)
	}
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func TestMemoryPush_ConcurrentSingleFlight(t *testing.T) {
	blob := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(blob),
		Size:      int64(len(blob)),
	}

	s := NewMemory()
	ctx := context.Background()

	// Start the leader first and hold its write in flight. Every reader
	// reports through started, but single-flight means only the leader's
	// reader is ever consumed.
	release := make(chan struct{})
	var leaderReads atomic.Int32
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_ = s.Push(ctx, desc, &heldReader{
			data:    blob,
			release: release,
			started: &leaderReads,
		})
	}()

	// The leader's reader is consumed only after it became the leader and its
	// write is in flight, so once started is observed the entry exists.
	waitFor(t, func() bool { return leaderReads.Load() == 1 })

	// While that write is in flight, concurrent pushes of the same descriptor
	// must join it instead of reading their own content.
	const waiters = 31
	var waiterReads atomic.Int32
	var wg sync.WaitGroup
	errs := make([]error, waiters)
	for i := range waiters {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.Push(ctx, desc, &heldReader{
				data:    blob,
				release: release,
				started: &waiterReads,
			})
		}(i)
	}

	// Give the waiters a chance to subscribe, then let the shared write finish.
	waitFor(t, func() bool { return inFlightPushes(s, desc) >= waiters })
	close(release)
	wg.Wait()
	select {
	case <-leaderDone:
	case <-time.After(time.Second):
		t.Fatal("leader push never completed")
	}

	successes, alreadyExists := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, errdef.ErrAlreadyExists):
			alreadyExists++
		default:
			t.Fatalf("unexpected waiter error: %v", err)
		}
	}
	if successes+alreadyExists != waiters {
		t.Errorf("accounted waiters = %d, want %d", successes+alreadyExists, waiters)
	}
	// exactly one write happened: the leader's reader, no waiter reader.
	if got := leaderReads.Load(); got != 1 {
		t.Errorf("leader reader consumed %d times, want 1", got)
	}
	if got := waiterReads.Load(); got != 0 {
		t.Errorf("waiter readers consumed %d, want 0 (shared write)", got)
	}

	// the committed content is complete and fetchable.
	exists, err := s.Exists(ctx, desc)
	if err != nil {
		t.Fatal("Memory.Exists() error =", err)
	}
	if !exists {
		t.Fatal("Memory.Exists() = false, want true")
	}
	rc, err := s.Fetch(ctx, desc)
	if err != nil {
		t.Fatal("Memory.Fetch() error =", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal("Memory.Fetch().Read() error =", err)
	}
	if !bytes.Equal(got, blob) {
		t.Errorf("fetched content = %q, want %q", got, blob)
	}
}

// inFlightPushes reports how many goroutines are currently parked in a push
// for desc, by counting the matching Flight.Do frames across all stacks.
func inFlightPushes(s *Memory, _ ocispec.Descriptor) int {
	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	for n == len(buf) {
		buf = make([]byte, len(buf)*2)
		n = runtime.Stack(buf, true)
	}
	return bytes.Count(buf[:n], []byte("syncutil.(*Flight[...]).Do"))
}

// waitFor polls cond until it becomes true or the timeout elapses.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not met in time")
		}
		runtime.Gosched()
	}
}

// heldReader blocks on release before serving its first byte and reports that
// it was consumed through started.
type heldReader struct {
	data    []byte
	pos     int
	release chan struct{}
	started *atomic.Int32
	once    sync.Once
}

func (r *heldReader) Read(p []byte) (int, error) {
	r.once.Do(func() {
		if r.started != nil {
			r.started.Add(1)
		}
		<-r.release
	})
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func TestMemoryPush_FailureLeavesNoReachableContent(t *testing.T) {
	blob := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(blob),
		Size:      int64(len(blob)),
	}

	s := NewMemory()
	ctx := context.Background()

	// a short write fails verification and must publish nothing.
	if err := s.Push(ctx, desc, strings.NewReader("short")); err == nil {
		t.Fatal("Memory.Push() with short content error = nil, want error")
	}

	exists, err := s.Exists(ctx, desc)
	if err != nil {
		t.Fatal("Memory.Exists() error =", err)
	}
	if exists {
		t.Fatal("Memory.Exists() = true after failed push, want false")
	}
	if _, err := s.Fetch(ctx, desc); !errors.Is(err, errdef.ErrNotFound) {
		t.Errorf("Memory.Fetch() error = %v, want %v", err, errdef.ErrNotFound)
	}
	if len(s.Map()) != 0 {
		t.Errorf("Memory.Map() = %d entries, want 0", len(s.Map()))
	}

	// the failed attempt is repeatable: the correct content can be committed.
	if err := s.Push(ctx, desc, bytes.NewReader(blob)); err != nil {
		t.Fatalf("retry Memory.Push() error = %v", err)
	}
	exists, err = s.Exists(ctx, desc)
	if err != nil {
		t.Fatal("Memory.Exists() error =", err)
	}
	if !exists {
		t.Error("Memory.Exists() = false after retry, want true")
	}
}

func TestMemoryPush_CancelledWaiterDoesNotAbortSharedWrite(t *testing.T) {
	blob := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(blob),
		Size:      int64(len(blob)),
	}

	s := NewMemory()

	release := make(chan struct{})
	var started atomic.Int32
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		// the leader blocks on release, keeping the shared write in flight.
		_ = s.Push(context.Background(), desc, &heldReader{
			data:    blob,
			release: release,
			started: &started,
		})
	}()

	// wait until the leader's reader is consumed, which proves its fn (and
	// thus the in-flight entry) exists.
	deadline := time.Now().Add(5 * time.Second)
	for started.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("leader write never started")
		}
		runtime.Gosched()
	}

	// a waiter gives up before the shared write completes.
	waiterCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Push(waiterCtx, desc, bytes.NewReader(blob)); !errors.Is(err, context.Canceled) {
		t.Errorf("waiter Push() error = %v, want %v", err, context.Canceled)
	}

	// the leader's shared write is unaffected and still commits.
	close(release)
	select {
	case <-leaderDone:
	case <-time.After(time.Second):
		t.Fatal("leader push never completed")
	}
	exists, err := s.Exists(context.Background(), desc)
	if err != nil {
		t.Fatal("Memory.Exists() error =", err)
	}
	if !exists {
		t.Error("Memory.Exists() = false, want true after shared write committed")
	}
}
