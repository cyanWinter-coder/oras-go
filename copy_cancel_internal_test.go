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

package oras

import (
	"bytes"
	"context"
	_ "crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/content/memory"
	"github.com/oras-project/oras-go/v3/internal/cas"
	"github.com/oras-project/oras-go/v3/internal/status"
	"golang.org/x/sync/semaphore"
)

// latePushStorage wraps a storage and blocks the push of gated digests until
// released. The gated push then completes anyway, simulating an upload result
// that lands after the copy has been cancelled.
type latePushStorage struct {
	content.Storage
	gated   map[digest.Digest]bool
	entered chan digest.Digest
	release chan struct{}
}

func (s *latePushStorage) Push(ctx context.Context, expected ocispec.Descriptor, r io.Reader) error {
	if s.gated[expected.Digest] {
		s.entered <- expected.Digest
		<-s.release
	}
	return s.Storage.Push(ctx, expected, r)
}

// pushTestManifest builds a manifest for the given blobs and pushes everything
// into store, returning the manifest descriptor.
func pushTestManifest(t *testing.T, ctx context.Context, store content.Storage, blobs ...[]byte) (ocispec.Descriptor, []ocispec.Descriptor) {
	t.Helper()
	var layers []ocispec.Descriptor
	for i, b := range blobs {
		desc := ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageLayer,
			Digest:    digest.FromBytes(b),
			Size:      int64(len(b)),
		}
		if err := store.Push(ctx, desc, bytes.NewReader(b)); err != nil {
			t.Fatalf("failed to push layer %d: %v", i, err)
		}
		layers = append(layers, desc)
	}
	config := []byte(`{"architecture":"amd64","os":"linux"}`)
	configDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageConfig,
		Digest:    digest.FromBytes(config),
		Size:      int64(len(config)),
	}
	if err := store.Push(ctx, configDesc, bytes.NewReader(config)); err != nil {
		t.Fatalf("failed to push config: %v", err)
	}
	manifest := ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    configDesc,
		Layers:    layers,
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(manifestJSON),
		Size:      int64(len(manifestJSON)),
	}
	if err := store.Push(ctx, manifestDesc, bytes.NewReader(manifestJSON)); err != nil {
		t.Fatalf("failed to push manifest: %v", err)
	}
	return manifestDesc, append([]ocispec.Descriptor{configDesc}, layers...)
}

// TestCopyGraph_CancelDoesNotAdvanceTracker verifies that a copy cancelled
// while an upload is in flight does not count the still-uploading node as
// done when its result lands late, does not start new nodes, and releases
// the concurrency slots it holds.
func TestCopyGraph_CancelDoesNotAdvanceTracker(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	root, nodes := pushTestManifest(t, ctx, src, []byte("layer-0"), []byte("layer-1"))
	configDesc, layer0, layer1 := nodes[0], nodes[1], nodes[2]

	// gate the push of layer-0: it blocks until the copy is cancelled and
	// then completes anyway, simulating a late upload result
	dst := &latePushStorage{
		Storage: cas.NewMemory(),
		gated:   map[digest.Digest]bool{layer0.Digest: true},
		entered: make(chan digest.Digest, 1),
		release: make(chan struct{}),
	}

	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	tracker := status.NewTracker()
	limiter := semaphore.NewWeighted(1)

	go func() {
		<-dst.entered // layer-0 push is in flight
		cancel()
		close(dst.release) // the upload finishes late, after the cancellation
	}()

	err := copyGraph(copyCtx, src, dst, root, nil, limiter, tracker, CopyGraphOptions{Concurrency: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("copyGraph() error = %v, want %v", err, context.Canceled)
	}

	// the config completed before the cancellation: it is done
	if got := tracker.StatusOf(configDesc); got != status.StatusDone {
		t.Errorf("StatusOf(config) = %v, want %v", got, status.StatusDone)
	}
	// layer-0 finished uploading after the cancellation: the late result
	// must not advance the tracker
	if got := tracker.StatusOf(layer0); got != status.StatusCommitted {
		t.Errorf("StatusOf(layer-0) = %v, want %v", got, status.StatusCommitted)
	}
	// layer-1 was never started after the cancellation
	if got := tracker.StatusOf(layer1); got != status.StatusPending {
		t.Errorf("StatusOf(layer-1) = %v, want %v", got, status.StatusPending)
	}
	// the root never completed
	if got := tracker.StatusOf(root); got != status.StatusCommitted {
		t.Errorf("StatusOf(root) = %v, want %v", got, status.StatusCommitted)
	}

	// the cancelled copy must release every concurrency slot it held
	if !limiter.TryAcquire(1) {
		t.Error("concurrency slot was not released after cancellation")
	} else {
		limiter.Release(1)
	}
}
