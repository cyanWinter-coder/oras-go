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

package auth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRefreshGroup_CoalescesConcurrentCalls proves that concurrent calls with
// the same key share a single fetch and its result.
func TestRefreshGroup_CoalescesConcurrentCalls(t *testing.T) {
	const callers = 10
	var fetches atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})

	group := newRefreshGroup()
	fetch := func(context.Context) (string, error) {
		if fetches.Add(1) == 1 {
			close(started)
		}
		<-release
		return "shared-token", nil
	}

	var wg sync.WaitGroup
	results := make(chan string, callers)
	start := make(chan struct{})
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			token, err := group.do(context.Background(), "registry Bearer scope", fetch)
			if err != nil {
				t.Errorf("group.do() error = %v", err)
				return
			}
			results <- token
		}()
	}
	close(start)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the shared fetch never started")
	}
	// give every caller time to join before releasing; a second fetch would
	// block on the same channel and be visible as another fetch count
	time.Sleep(100 * time.Millisecond)
	if got := fetches.Load(); got != 1 {
		t.Errorf("expected one fetch, got %d", got)
	}
	close(release)
	wg.Wait()
	close(results)

	for token := range results {
		if token != "shared-token" {
			t.Errorf("shared result = %q, want %q", token, "shared-token")
		}
	}
	if got := fetches.Load(); got != 1 {
		t.Errorf("expected one fetch in total, got %d", got)
	}
}

// TestRefreshGroup_DistinctKeysRunSeparately proves that different keys do not
// share a fetch.
func TestRefreshGroup_DistinctKeysRunSeparately(t *testing.T) {
	var fetches atomic.Int32
	group := newRefreshGroup()
	fetch := func(_ context.Context) (string, error) {
		n := fetches.Add(1)
		return fmt.Sprintf("token-%d", n), nil
	}

	tokenA, err := group.do(context.Background(), "key-a", fetch)
	if err != nil {
		t.Fatalf("group.do(key-a) error = %v", err)
	}
	tokenB, err := group.do(context.Background(), "key-b", fetch)
	if err != nil {
		t.Fatalf("group.do(key-b) error = %v", err)
	}
	if tokenA == tokenB {
		t.Errorf("expected distinct results for distinct keys, got %q twice", tokenA)
	}
	if got := fetches.Load(); got != 2 {
		t.Errorf("expected 2 fetches, got %d", got)
	}

	// once a generation completes, the same key starts a fresh generation
	tokenC, err := group.do(context.Background(), "key-a", fetch)
	if err != nil {
		t.Fatalf("group.do(key-a) retry error = %v", err)
	}
	if tokenC == tokenA {
		t.Errorf("expected a new generation for key-a, got stale result %q", tokenC)
	}
	if got := fetches.Load(); got != 3 {
		t.Errorf("expected 3 fetches, got %d", got)
	}
}

// TestRefreshGroup_CancelledWaiterDoesNotCancelFetch proves requirement 5 at
// the group level: a caller whose context ends returns its own error, while the
// shared fetch survives and still serves another caller.
func TestRefreshGroup_CancelledWaiterDoesNotCancelFetch(t *testing.T) {
	var fetches atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})

	group := newRefreshGroup()
	fetch := func(ctx context.Context) (string, error) {
		fetches.Add(1)
		close(started)
		<-release
		if ctx.Err() != nil {
			t.Errorf("shared fetch context ended: %v", ctx.Err())
		}
		return "surviving-token", nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 2)
	go func() {
		_, err := group.do(ctx, "same-key", fetch)
		errs <- err
	}()
	go func() {
		_, err := group.do(context.Background(), "same-key", fetch)
		errs <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the shared fetch never started")
	}
	cancel()

	select {
	case err := <-errs:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("cancelled waiter error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled waiter did not return")
	}

	close(release)
	select {
	case err := <-errs:
		if err != nil {
			t.Errorf("remaining waiter error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remaining waiter did not complete")
	}
	if got := fetches.Load(); got != 1 {
		t.Errorf("expected the fetch to survive once, got %d", got)
	}
}

// TestRefreshGroup_FetchErrorIsShared proves that every waiter receives the
// fetch error and the failed generation can be followed by a successful one.
func TestRefreshGroup_FetchErrorIsShared(t *testing.T) {
	const callers = 5
	wantErr := errors.New("token endpoint down")
	var fetches atomic.Int32
	release := make(chan struct{})

	group := newRefreshGroup()
	fetch := func(context.Context) (string, error) {
		fetches.Add(1)
		<-release
		return "", wantErr
	}

	var wg sync.WaitGroup
	errs := make(chan error, callers)
	start := make(chan struct{})
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := group.do(context.Background(), "failing-key", fetch)
			errs <- err
		}()
	}
	close(start)
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)

	for err := range errs {
		if !errors.Is(err, wantErr) {
			t.Errorf("waiter error = %v, want %v", err, wantErr)
		}
	}
	if got := fetches.Load(); got != 1 {
		t.Errorf("expected one failed fetch, got %d", got)
	}

	// the failed generation is cleaned up, so a later call fetches again
	token, err := group.do(context.Background(), "failing-key", func(context.Context) (string, error) {
		return "recovered-token", nil
	})
	if err != nil {
		t.Fatalf("retry after failed fetch error = %v", err)
	}
	if token != "recovered-token" {
		t.Errorf("retry result = %q, want %q", token, "recovered-token")
	}
}
