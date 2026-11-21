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

package oras_test

import (
	"bytes"
	"context"
	_ "crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3"
	"github.com/oras-project/oras-go/v3/content/memory"
	"github.com/oras-project/oras-go/v3/registry/remote/errcode"
)

// hookedTarget wraps a Target with hooks intercepting selected operations.
// A nil hook delegates to the wrapped target.
type hookedTarget struct {
	oras.Target
	resolveHook func(ctx context.Context, reference string) (ocispec.Descriptor, error)
	fetchHook   func(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error)
	pushHook    func(ctx context.Context, expected ocispec.Descriptor, r io.Reader) error
	tagHook     func(ctx context.Context, desc ocispec.Descriptor, reference string) error
}

func (t *hookedTarget) Resolve(ctx context.Context, reference string) (ocispec.Descriptor, error) {
	if t.resolveHook != nil {
		return t.resolveHook(ctx, reference)
	}
	return t.Target.Resolve(ctx, reference)
}

func (t *hookedTarget) Fetch(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	if t.fetchHook != nil {
		return t.fetchHook(ctx, target)
	}
	return t.Target.Fetch(ctx, target)
}

func (t *hookedTarget) Push(ctx context.Context, expected ocispec.Descriptor, r io.Reader) error {
	if t.pushHook != nil {
		return t.pushHook(ctx, expected, r)
	}
	return t.Target.Push(ctx, expected, r)
}

func (t *hookedTarget) Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error {
	if t.tagHook != nil {
		return t.tagHook(ctx, desc, reference)
	}
	return t.Target.Tag(ctx, desc, reference)
}

// pushCopierManifest seeds store with a config blob, a layer blob and a
// manifest referencing them, tagged with tag if tag is not empty. id makes
// the content unique. It returns the manifest descriptor and the layer
// descriptor.
func pushCopierManifest(t *testing.T, store *memory.Store, tag, id string) (manifest, layer ocispec.Descriptor) {
	t.Helper()
	ctx := context.Background()
	push := func(mediaType string, b []byte) ocispec.Descriptor {
		desc := ocispec.Descriptor{
			MediaType: mediaType,
			Digest:    digest.FromBytes(b),
			Size:      int64(len(b)),
		}
		if err := store.Push(ctx, desc, bytes.NewReader(b)); err != nil {
			t.Fatalf("failed to seed store: %v", err)
		}
		return desc
	}

	config := push(ocispec.MediaTypeImageConfig, []byte("config-"+id))
	layer = push(ocispec.MediaTypeImageLayer, []byte("layer-"+id))
	manifestJSON, err := json.Marshal(ocispec.Manifest{
		Config: config,
		Layers: []ocispec.Descriptor{layer},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest = push(ocispec.MediaTypeImageManifest, manifestJSON)
	if tag != "" {
		if err := store.Tag(ctx, manifest, tag); err != nil {
			t.Fatalf("failed to tag store: %v", err)
		}
	}
	return manifest, layer
}

func TestCopier_NilTargets(t *testing.T) {
	ctx := context.Background()
	var c oras.Copier
	if _, err := c.Copy(ctx, "latest", "", oras.CopierOptions{}); err == nil {
		t.Error("Copy() with nil source error = nil, want an error")
	}
	c.Source = memory.New()
	if _, err := c.Copy(ctx, "latest", "", oras.CopierOptions{}); err == nil {
		t.Error("Copy() with nil destination error = nil, want an error")
	}
}

func TestCopier_SharesInFlightCopy(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	manifest, layer := pushCopierManifest(t, src, "latest", "v1")

	// block the layer push until every caller has joined the copy
	pushStarted := make(chan struct{})
	release := make(chan struct{})
	var pushCount sync.Map // digest -> *atomic.Int64
	countPush := func(d digest.Digest) *atomic.Int64 {
		v, _ := pushCount.LoadOrStore(d, &atomic.Int64{})
		return v.(*atomic.Int64)
	}
	dst := &hookedTarget{Target: memory.New()}
	dst.pushHook = func(ctx context.Context, expected ocispec.Descriptor, r io.Reader) error {
		countPush(expected.Digest).Add(1)
		if expected.Digest == layer.Digest {
			select {
			case <-pushStarted:
			default:
				close(pushStarted)
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return dst.Target.Push(ctx, expected, r)
	}

	c := &oras.Copier{Source: src, Destination: dst}
	opts := oras.CopierOptions{CopyGraphOptions: oras.CopyGraphOptions{Concurrency: 1}}

	const callers = 4
	descs := make([]ocispec.Descriptor, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			descs[i], errs[i] = c.Copy(ctx, "latest", "", opts)
		}()
	}

	// wait for the shared copy to reach the layer push, then give the other
	// callers a chance to subscribe before releasing the push
	<-pushStarted
	time.Sleep(200 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: Copy() error = %v, want nil", i, err)
		}
		if !contentEqual(descs[i], manifest) {
			t.Errorf("caller %d: Copy() = %v, want %v", i, descs[i].Digest, manifest.Digest)
		}
	}

	// the shared copy probed and uploaded every node exactly once
	for _, d := range []ocispec.Descriptor{manifest, layer} {
		if got := countPush(d.Digest).Load(); got != 1 {
			t.Errorf("Push(%s) called %d times, want 1", d.Digest, got)
		}
	}
	if _, err := dst.Resolve(ctx, "latest"); err != nil {
		t.Errorf("destination tag missing after copy: %v", err)
	}
}

func TestCopier_SubscriberCancelDoesNotCancelSharedWork(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	manifest, layer := pushCopierManifest(t, src, "latest", "v1")

	pushStarted := make(chan struct{})
	release := make(chan struct{})
	var pushCancelled atomic.Bool
	dst := &hookedTarget{Target: memory.New()}
	dst.pushHook = func(ctx context.Context, expected ocispec.Descriptor, r io.Reader) error {
		if expected.Digest == layer.Digest {
			close(pushStarted)
			select {
			case <-release:
			case <-ctx.Done():
				pushCancelled.Store(true)
				return ctx.Err()
			}
		}
		return dst.Target.Push(ctx, expected, r)
	}

	c := &oras.Copier{Source: src, Destination: dst}
	opts := oras.CopierOptions{CopyGraphOptions: oras.CopyGraphOptions{Concurrency: 1}}

	// the first caller cancels while the shared copy is in flight
	cancelCtx, cancel := context.WithCancel(ctx)
	firstErr := make(chan error, 1)
	go func() {
		_, err := c.Copy(cancelCtx, "latest", "", opts)
		firstErr <- err
	}()

	// the second caller keeps waiting
	secondResult := make(chan ocispec.Descriptor, 1)
	secondErr := make(chan error, 1)
	go func() {
		desc, err := c.Copy(ctx, "latest", "", opts)
		secondResult <- desc
		secondErr <- err
	}()

	<-pushStarted
	cancel()
	if err := <-firstErr; !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled caller error = %v, want %v", err, context.Canceled)
	}

	// the shared copy must still be running for the remaining caller
	select {
	case err := <-secondErr:
		t.Fatalf("shared copy ended early with error %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)

	if err := <-secondErr; err != nil {
		t.Fatalf("remaining caller error = %v, want nil", err)
	}
	if desc := <-secondResult; !contentEqual(desc, manifest) {
		t.Errorf("remaining caller Copy() = %v, want %v", desc.Digest, manifest.Digest)
	}
	if pushCancelled.Load() {
		t.Error("shared copy was cancelled by the leaving subscriber")
	}
}

func TestCopier_LastSubscriberCancelCancelsSharedWork(t *testing.T) {
	src := memory.New()
	_, layer := pushCopierManifest(t, src, "latest", "v1")

	pushStarted := make(chan struct{})
	workCancelled := make(chan struct{})
	dst := &hookedTarget{Target: memory.New()}
	dst.pushHook = func(ctx context.Context, expected ocispec.Descriptor, r io.Reader) error {
		if expected.Digest == layer.Digest {
			close(pushStarted)
			select {
			case <-ctx.Done():
				close(workCancelled)
				return ctx.Err()
			case <-time.After(10 * time.Second):
				return errors.New("push timed out")
			}
		}
		return dst.Target.Push(ctx, expected, r)
	}

	c := &oras.Copier{Source: src, Destination: dst}
	opts := oras.CopierOptions{CopyGraphOptions: oras.CopyGraphOptions{Concurrency: 1}}

	ctx, cancel := context.WithCancel(context.Background())
	copyErr := make(chan error, 1)
	go func() {
		_, err := c.Copy(ctx, "latest", "", opts)
		copyErr <- err
	}()

	<-pushStarted
	cancel()
	select {
	case <-workCancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("shared copy was not cancelled after the last subscriber left")
	}
	if err := <-copyErr; !errors.Is(err, context.Canceled) {
		t.Errorf("Copy() error = %v, want %v", err, context.Canceled)
	}
}

func TestCopier_RefreshesSourceAuthOnce(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	manifest, _ := pushCopierManifest(t, src, "latest", "v1")

	var refreshes atomic.Int64
	unauthorized := &errcode.ErrorResponse{
		StatusCode: http.StatusUnauthorized,
		Errors:     errcode.Errors{{Code: errcode.ErrorCodeUnauthorized}},
	}
	hookedSrc := &hookedTarget{
		Target: src,
		resolveHook: func(ctx context.Context, reference string) (ocispec.Descriptor, error) {
			if refreshes.Load() == 0 {
				return ocispec.Descriptor{}, unauthorized
			}
			return src.Resolve(ctx, reference)
		},
	}

	c := &oras.Copier{
		Source:      hookedSrc,
		Destination: memory.New(),
		RefreshSource: func(ctx context.Context) error {
			refreshes.Add(1)
			return nil
		},
	}

	const callers = 5
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var desc ocispec.Descriptor
			desc, errs[i] = c.Copy(ctx, "latest", "", oras.CopierOptions{})
			if errs[i] == nil && !contentEqual(desc, manifest) {
				errs[i] = errors.New("unexpected descriptor")
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: Copy() error = %v, want nil", i, err)
		}
	}
	if got := refreshes.Load(); got != 1 {
		t.Errorf("RefreshSource called %d times, want 1", got)
	}
}

func TestCopier_SourceAuthErrorStaysClassifiable(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	pushCopierManifest(t, src, "latest", "v1")

	unauthorized := &errcode.ErrorResponse{
		StatusCode: http.StatusUnauthorized,
		Errors:     errcode.Errors{{Code: errcode.ErrorCodeUnauthorized}},
	}
	hookedSrc := &hookedTarget{
		Target: src,
		resolveHook: func(ctx context.Context, reference string) (ocispec.Descriptor, error) {
			return ocispec.Descriptor{}, unauthorized
		},
	}

	// without RefreshSource the classified error is returned as is
	c := &oras.Copier{Source: hookedSrc, Destination: memory.New()}
	_, err := c.Copy(ctx, "latest", "", oras.CopierOptions{})
	assertClassifiedAuthError(t, err)

	// with RefreshSource the refresh happens once and the still-failing
	// retry surfaces the classified error
	var refreshes atomic.Int64
	c.RefreshSource = func(ctx context.Context) error {
		refreshes.Add(1)
		return nil
	}
	_, err = c.Copy(ctx, "latest", "", oras.CopierOptions{})
	assertClassifiedAuthError(t, err)
	if got := refreshes.Load(); got != 1 {
		t.Errorf("RefreshSource called %d times, want 1", got)
	}
}

// assertClassifiedAuthError checks that err keeps its recognizable form: a
// source-side *oras.CopyError wrapping the registry's 401 error response.
func assertClassifiedAuthError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Copy() error = nil, want an authentication error")
	}
	var copyErr *oras.CopyError
	if !errors.As(err, &copyErr) {
		t.Fatalf("Copy() error = %v, want a *oras.CopyError", err)
	}
	if copyErr.Origin != oras.CopyErrorOriginSource {
		t.Errorf("CopyError.Origin = %v, want %v", copyErr.Origin, oras.CopyErrorOriginSource)
	}
	var errResp *errcode.ErrorResponse
	if !errors.As(err, &errResp) {
		t.Fatalf("Copy() error = %v, want an *errcode.ErrorResponse", err)
	}
	if errResp.StatusCode != http.StatusUnauthorized {
		t.Errorf("ErrorResponse.StatusCode = %d, want %d", errResp.StatusCode, http.StatusUnauthorized)
	}
}

func TestCopier_RefreshErrorPropagated(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	pushCopierManifest(t, src, "latest", "v1")

	unauthorized := &errcode.ErrorResponse{StatusCode: http.StatusUnauthorized}
	hookedSrc := &hookedTarget{
		Target: src,
		resolveHook: func(ctx context.Context, reference string) (ocispec.Descriptor, error) {
			return ocispec.Descriptor{}, unauthorized
		},
	}
	refreshErr := errors.New("credentials rejected")
	c := &oras.Copier{
		Source:      hookedSrc,
		Destination: memory.New(),
		RefreshSource: func(ctx context.Context) error {
			return refreshErr
		},
	}

	_, err := c.Copy(ctx, "latest", "", oras.CopierOptions{})
	if !errors.Is(err, refreshErr) {
		t.Errorf("Copy() error = %v, want it to wrap %v", err, refreshErr)
	}
	var copyErr *oras.CopyError
	if !errors.As(err, &copyErr) || copyErr.Op != "RefreshSource" {
		t.Errorf("Copy() error = %v, want a *oras.CopyError with Op %q", err, "RefreshSource")
	}
}

func TestCopier_StaleGenerationNotCommitted(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	_, layerV1 := pushCopierManifest(t, src, "latest", "v1")

	// The first fetch of the V1 layer fails with an authentication error.
	// The refresh swaps the source to V2, simulating the source moving to a
	// new generation while the copy of V1 is in flight.
	var fetchFailed atomic.Bool
	hookedSrc := &hookedTarget{Target: src}
	hookedSrc.fetchHook = func(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
		if target.Digest == layerV1.Digest && fetchFailed.CompareAndSwap(false, true) {
			return nil, &errcode.ErrorResponse{
				StatusCode: http.StatusUnauthorized,
				Errors:     errcode.Errors{{Code: errcode.ErrorCodeUnauthorized}},
			}
		}
		return src.Fetch(ctx, target)
	}

	var refreshes atomic.Int64
	manifestV2, _ := pushCopierManifest(t, src, "", "v2")
	c := &oras.Copier{
		Source:      hookedSrc,
		Destination: memory.New(),
		RefreshSource: func(ctx context.Context) error {
			refreshes.Add(1)
			// the source moved on: "latest" now points to V2
			return src.Tag(ctx, manifestV2, "latest")
		},
	}

	desc, err := c.Copy(ctx, "latest", "", oras.CopierOptions{})
	if err != nil {
		t.Fatalf("Copy() error = %v, want nil", err)
	}
	if !contentEqual(desc, manifestV2) {
		t.Errorf("Copy() = %v, want the V2 descriptor %v", desc.Digest, manifestV2.Digest)
	}
	if got := refreshes.Load(); got != 1 {
		t.Errorf("RefreshSource called %d times, want 1", got)
	}

	// the destination tag must point to the current generation, not to the
	// stale V1 result
	got, err := c.Destination.Resolve(ctx, "latest")
	if err != nil {
		t.Fatalf("destination tag missing: %v", err)
	}
	if !contentEqual(got, manifestV2) {
		t.Errorf("destination tag = %v, want the V2 descriptor %v", got.Digest, manifestV2.Digest)
	}
}

func TestCopier_FailureKeepsDestinationTag(t *testing.T) {
	ctx := context.Background()

	// the destination already has an older version tagged
	dstStore := memory.New()
	oldManifest, _ := pushCopierManifest(t, dstStore, "latest", "old")

	src := memory.New()
	_, newLayer := pushCopierManifest(t, src, "latest", "new")

	pushErr := errors.New("push failed")
	dst := &hookedTarget{
		Target: dstStore,
		pushHook: func(ctx context.Context, expected ocispec.Descriptor, r io.Reader) error {
			if expected.Digest == newLayer.Digest {
				return pushErr
			}
			return dstStore.Push(ctx, expected, r)
		},
	}

	c := &oras.Copier{Source: src, Destination: dst}
	if _, err := c.Copy(ctx, "latest", "", oras.CopierOptions{}); !errors.Is(err, pushErr) {
		t.Fatalf("Copy() error = %v, want it to wrap %v", err, pushErr)
	}

	// the failed copy must not have moved the tag
	got, err := dstStore.Resolve(ctx, "latest")
	if err != nil {
		t.Fatalf("destination tag missing after failed copy: %v", err)
	}
	if !contentEqual(got, oldManifest) {
		t.Errorf("destination tag = %v, want the old descriptor %v", got.Digest, oldManifest.Digest)
	}
}

func TestCopier_NoDuplicateUploadAfterSuccess(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	if _, layer := pushCopierManifest(t, src, "latest", "v1"); layer.Digest == "" {
		t.Fatal("failed to seed source")
	}

	var pushes atomic.Int64
	dst := &hookedTarget{Target: memory.New()}
	dst.pushHook = func(ctx context.Context, expected ocispec.Descriptor, r io.Reader) error {
		pushes.Add(1)
		return dst.Target.Push(ctx, expected, r)
	}

	c := &oras.Copier{Source: src, Destination: dst}
	if _, err := c.Copy(ctx, "latest", "", oras.CopierOptions{}); err != nil {
		t.Fatalf("first Copy() error = %v, want nil", err)
	}
	firstPushes := pushes.Load()
	if firstPushes == 0 {
		t.Fatal("first Copy() pushed nothing")
	}

	if _, err := c.Copy(ctx, "latest", "", oras.CopierOptions{}); err != nil {
		t.Fatalf("second Copy() error = %v, want nil", err)
	}
	if got := pushes.Load(); got != firstPushes {
		t.Errorf("second Copy() pushed %d more nodes, want 0", got-firstPushes)
	}
}

func TestCopier_PublishRevalidatesDigest(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	manifestV1, _ := pushCopierManifest(t, src, "", "v1")
	manifestV2, _ := pushCopierManifest(t, src, "", "v2")

	// The source reference moves from V1 to V2 between the copy and the
	// publish: the first resolve returns V1, every later resolve returns V2.
	var resolves atomic.Int64
	hookedSrc := &hookedTarget{
		Target: src,
		resolveHook: func(ctx context.Context, reference string) (ocispec.Descriptor, error) {
			if resolves.Add(1) == 1 {
				return manifestV1, nil
			}
			return manifestV2, nil
		},
	}

	c := &oras.Copier{Source: hookedSrc, Destination: memory.New()}
	desc, err := c.Copy(ctx, "latest", "", oras.CopierOptions{})
	if err != nil {
		t.Fatalf("Copy() error = %v, want nil", err)
	}
	if !contentEqual(desc, manifestV2) {
		t.Errorf("Copy() = %v, want the V2 descriptor %v", desc.Digest, manifestV2.Digest)
	}
	got, err := c.Destination.Resolve(ctx, "latest")
	if err != nil {
		t.Fatalf("destination tag missing: %v", err)
	}
	if !contentEqual(got, manifestV2) {
		t.Errorf("destination tag = %v, want the V2 descriptor %v", got.Digest, manifestV2.Digest)
	}
}

// contentEqual reports whether a and b describe the same content.
func contentEqual(a, b ocispec.Descriptor) bool {
	return a.Digest == b.Digest && a.MediaType == b.MediaType && a.Size == b.Size
}
