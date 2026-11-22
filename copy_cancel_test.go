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
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3"
	"github.com/oras-project/oras-go/v3/content/memory"
	"github.com/oras-project/oras-go/v3/errdef"
)

// cancelTestImage holds the descriptors of a test image: a config, a set of
// layers and a manifest referencing them.
type cancelTestImage struct {
	manifest ocispec.Descriptor
	config   ocispec.Descriptor
	layers   []ocispec.Descriptor
}

// nodes returns the descriptors of every node of the image except the
// manifest itself.
func (img cancelTestImage) nodes() []ocispec.Descriptor {
	return append([]ocispec.Descriptor{img.config}, img.layers...)
}

// buildCancelTestImage pushes an image with the given layer contents into
// store and tags the manifest with ref.
func buildCancelTestImage(t *testing.T, ctx context.Context, store *memory.Store, ref string, layerBlobs ...[]byte) cancelTestImage {
	t.Helper()
	img := cancelTestImage{}
	for i, b := range layerBlobs {
		desc := ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageLayer,
			Digest:    digest.FromBytes(b),
			Size:      int64(len(b)),
		}
		if err := store.Push(ctx, desc, bytes.NewReader(b)); err != nil {
			t.Fatalf("failed to push layer %d: %v", i, err)
		}
		img.layers = append(img.layers, desc)
	}
	config := []byte(`{"architecture":"amd64","os":"linux"}`)
	img.config = ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageConfig,
		Digest:    digest.FromBytes(config),
		Size:      int64(len(config)),
	}
	if err := store.Push(ctx, img.config, bytes.NewReader(config)); err != nil {
		t.Fatalf("failed to push config: %v", err)
	}
	manifest := ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    img.config,
		Layers:    img.layers,
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	img.manifest = ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(manifestJSON),
		Size:      int64(len(manifestJSON)),
	}
	if err := store.Push(ctx, img.manifest, bytes.NewReader(manifestJSON)); err != nil {
		t.Fatalf("failed to push manifest: %v", err)
	}
	if ref != "" {
		if err := store.Tag(ctx, img.manifest, ref); err != nil {
			t.Fatalf("failed to tag manifest: %v", err)
		}
	}
	return img
}

// pushCountingTarget wraps a target and counts the pushes of every digest.
type pushCountingTarget struct {
	oras.Target
	mu     sync.Mutex
	pushes map[digest.Digest]int
}

func newPushCountingTarget(target oras.Target) *pushCountingTarget {
	return &pushCountingTarget{Target: target, pushes: make(map[digest.Digest]int)}
}

func (t *pushCountingTarget) Push(ctx context.Context, expected ocispec.Descriptor, content io.Reader) error {
	t.mu.Lock()
	t.pushes[expected.Digest]++
	t.mu.Unlock()
	return t.Target.Push(ctx, expected, content)
}

func (t *pushCountingTarget) pushCount(desc ocispec.Descriptor) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pushes[desc.Digest]
}

// existsGateTarget wraps a target and blocks the Exists call of gated
// digests until released, simulating in-flight node processing at the moment
// a copy is cancelled.
type existsGateTarget struct {
	oras.Target
	gated   map[digest.Digest]bool
	entered chan digest.Digest
	release chan struct{}
}

func newExistsGateTarget(target oras.Target, descs ...ocispec.Descriptor) *existsGateTarget {
	gated := make(map[digest.Digest]bool, len(descs))
	for _, d := range descs {
		gated[d.Digest] = true
	}
	return &existsGateTarget{
		Target:  target,
		gated:   gated,
		entered: make(chan digest.Digest, len(descs)),
		release: make(chan struct{}),
	}
}

func (t *existsGateTarget) Exists(ctx context.Context, target ocispec.Descriptor) (bool, error) {
	if t.gated[target.Digest] {
		t.entered <- target.Digest
		<-t.release
	}
	return t.Target.Exists(ctx, target)
}

// waitBlocked waits until all gated nodes are blocked, then cancels the copy
// and releases the gate.
func (t *existsGateTarget) waitBlocked(cancel context.CancelFunc) {
	for range t.gated {
		<-t.entered
	}
	cancel()
	close(t.release)
}

// latePushTarget wraps a target and blocks the push of gated digests until
// released. The gated push then completes anyway, simulating an upload
// result that lands after the copy has been cancelled.
type latePushTarget struct {
	oras.Target
	gated   map[digest.Digest]bool
	entered chan digest.Digest
	release chan struct{}
}

func newLatePushTarget(target oras.Target, descs ...ocispec.Descriptor) *latePushTarget {
	gated := make(map[digest.Digest]bool, len(descs))
	for _, d := range descs {
		gated[d.Digest] = true
	}
	return &latePushTarget{
		Target:  target,
		gated:   gated,
		entered: make(chan digest.Digest, len(descs)),
		release: make(chan struct{}),
	}
}

func (t *latePushTarget) Push(ctx context.Context, expected ocispec.Descriptor, content io.Reader) error {
	if t.gated[expected.Digest] {
		t.entered <- expected.Digest
		<-t.release
	}
	return t.Target.Push(ctx, expected, content)
}

// TestCopy_CancelStopsNewLayerUploads verifies that once a copy is
// cancelled, nodes that have not started uploading are not uploaded: the
// cancellation stops new layer uploads instead of letting every in-flight
// node run to completion.
func TestCopy_CancelStopsNewLayerUploads(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	img := buildCancelTestImage(t, ctx, src, "latest",
		[]byte("layer-0"), []byte("layer-1"), []byte("layer-2"), []byte("layer-3"))

	// the destination already has one layer
	dstBase := memory.New()
	if rc, err := src.Fetch(ctx, img.layers[0]); err != nil {
		t.Fatal(err)
	} else if err := dstBase.Push(ctx, img.layers[0], rc); err != nil {
		t.Fatal(err)
	}

	counter := newPushCountingTarget(dstBase)
	// gate the existence check of every node that is not already in the
	// destination, so the copy is cancelled while they are being processed
	var pending []ocispec.Descriptor
	pending = append(pending, img.config)
	pending = append(pending, img.layers[1:]...)
	dst := newExistsGateTarget(counter, pending...)

	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go dst.waitBlocked(cancel)

	_, err := oras.Copy(copyCtx, src, "latest", dst, "latest", oras.CopyOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Copy() error = %v, want %v", err, context.Canceled)
	}

	// no new uploads were started for the nodes blocked by the cancellation
	for _, desc := range pending {
		if got := counter.pushCount(desc); got != 0 {
			t.Errorf("Push() called %d times for %s after cancellation, want 0", got, desc.Digest)
		}
	}
	// the root manifest was never copied, so the tag must not be set
	if _, err := dstBase.Resolve(ctx, "latest"); !errors.Is(err, errdef.ErrNotFound) {
		t.Errorf("Resolve() error = %v, want %v", err, errdef.ErrNotFound)
	}
}

// TestCopy_LateUploadKeepsOldTag verifies that upload results landing after
// a cancellation do not complete the copy: the destination reference keeps
// pointing at the version that was tagged before the copy started.
func TestCopy_LateUploadKeepsOldTag(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	img := buildCancelTestImage(t, ctx, src, "latest",
		[]byte("layer-0"), []byte("layer-1"), []byte("layer-2"), []byte("layer-3"))

	// the destination already serves an older version under the same tag
	dstBase := memory.New()
	oldImg := buildCancelTestImage(t, ctx, dstBase, "latest", []byte("old-layer"))

	// the layer uploads finish only after the copy is cancelled
	dst := newLatePushTarget(dstBase, img.layers...)

	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for range img.layers {
			<-dst.entered
		}
		cancel()
		close(dst.release)
	}()

	_, err := oras.Copy(copyCtx, src, "latest", dst, "latest", oras.CopyOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Copy() error = %v, want %v", err, context.Canceled)
	}

	// the tag must still resolve to the old manifest
	got, err := dstBase.Resolve(ctx, "latest")
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got.Digest != oldImg.manifest.Digest {
		t.Errorf("tag moved to %s, want it to stay at %s", got.Digest, oldImg.manifest.Digest)
	}
}

// TestCopy_SkipsExistingLayerByDigestNotSize verifies that a layer already
// in the destination is skipped based on its digest: a destination blob of
// the same size but a different digest must not make the copy skip the
// layer, while a blob with the same digest is not uploaded again.
func TestCopy_SkipsExistingLayerByDigestNotSize(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	img := buildCancelTestImage(t, ctx, src, "latest", []byte("foo"))

	// the destination has the config (same digest) and a same-size blob
	// with a different digest
	dstBase := memory.New()
	if rc, err := src.Fetch(ctx, img.config); err != nil {
		t.Fatal(err)
	} else if err := dstBase.Push(ctx, img.config, rc); err != nil {
		t.Fatal(err)
	}
	imposter := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageLayer,
		Digest:    digest.FromBytes([]byte("bar")), // same size as "foo"
		Size:      3,
	}
	if err := dstBase.Push(ctx, imposter, bytes.NewReader([]byte("bar"))); err != nil {
		t.Fatal(err)
	}
	dst := newPushCountingTarget(dstBase)

	if _, err := oras.Copy(ctx, src, "latest", dst, "latest", oras.CopyOptions{}); err != nil {
		t.Fatalf("Copy() error = %v", err)
	}

	// the layer digest does not match the existing same-size blob, so the
	// layer must have been uploaded
	if got := dst.pushCount(img.layers[0]); got != 1 {
		t.Errorf("Push() called %d times for the layer, want 1", got)
	}
	// the config digest already exists, so it must not have been uploaded
	if got := dst.pushCount(img.config); got != 0 {
		t.Errorf("Push() called %d times for the existing config, want 0", got)
	}
	if ok, err := dstBase.Exists(ctx, img.layers[0]); err != nil || !ok {
		t.Errorf("Exists() = %v, %v, want the layer to be present", ok, err)
	}
}

// corruptFetchTarget wraps a target and serves corrupted content for the
// digest of a single node, simulating source content that changed after the
// graph was resolved.
type corruptFetchTarget struct {
	oras.ReadOnlyTarget
	corrupt digest.Digest
}

func (t *corruptFetchTarget) Fetch(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	if target.Digest == t.corrupt {
		return io.NopCloser(bytes.NewReader([]byte("corrupted content"))), nil
	}
	return t.ReadOnlyTarget.Fetch(ctx, target)
}

// TestCopy_SourceDigestMismatchKeepsOldTag verifies that when the source
// serves content that does not match the digest frozen at the start of the
// copy, the copy fails and the destination reference keeps the old version.
func TestCopy_SourceDigestMismatchKeepsOldTag(t *testing.T) {
	ctx := context.Background()
	srcBase := memory.New()
	img := buildCancelTestImage(t, ctx, srcBase, "latest", []byte("layer-0"), []byte("layer-1"))
	src := &corruptFetchTarget{ReadOnlyTarget: srcBase, corrupt: img.layers[0].Digest}

	// the destination already serves an older version under the same tag
	dst := memory.New()
	oldImg := buildCancelTestImage(t, ctx, dst, "latest", []byte("old-layer"))

	_, err := oras.Copy(ctx, src, "latest", dst, "latest", oras.CopyOptions{})
	if err == nil {
		t.Fatal("Copy() should fail on a digest mismatch")
	}

	// the tag must still resolve to the old manifest
	got, err := dst.Resolve(ctx, "latest")
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got.Digest != oldImg.manifest.Digest {
		t.Errorf("tag moved to %s, want it to stay at %s", got.Digest, oldImg.manifest.Digest)
	}
	// the corrupted layer must not be stored under its expected digest
	if ok, err := dst.Exists(ctx, img.layers[0]); err != nil || ok {
		t.Errorf("Exists() = %v, %v, want the mismatched layer to be absent", ok, err)
	}
}

// TestExtendedCopy_CancelStopsCopy verifies that cancelling an extended
// copy stops the remaining uploads and leaves the destination untagged.
func TestExtendedCopy_CancelStopsCopy(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	img := buildCancelTestImage(t, ctx, src, "latest",
		[]byte("layer-0"), []byte("layer-1"), []byte("layer-2"))

	dstBase := memory.New()
	counter := newPushCountingTarget(dstBase)
	dst := newExistsGateTarget(counter, img.nodes()...)

	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go dst.waitBlocked(cancel)

	_, err := oras.ExtendedCopy(copyCtx, src, "latest", dst, "latest", oras.ExtendedCopyOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ExtendedCopy() error = %v, want %v", err, context.Canceled)
	}

	// no uploads were started for the nodes blocked by the cancellation
	for _, desc := range img.nodes() {
		if got := counter.pushCount(desc); got != 0 {
			t.Errorf("Push() called %d times for %s after cancellation, want 0", got, desc.Digest)
		}
	}
	if _, err := dstBase.Resolve(ctx, "latest"); !errors.Is(err, errdef.ErrNotFound) {
		t.Errorf("Resolve() error = %v, want %v", err, errdef.ErrNotFound)
	}
}
