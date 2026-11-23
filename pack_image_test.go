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
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/content/memory"
	"github.com/oras-project/oras-go/v3/content/oci"
	"github.com/oras-project/oras-go/v3/errdef"
)

// countingStorage counts the Push calls made to the underlying storage.
type countingStorage struct {
	content.Storage
	pushes int
}

func (s *countingStorage) Push(ctx context.Context, expected ocispec.Descriptor, reader io.Reader) error {
	s.pushes++
	return s.Storage.Push(ctx, expected, reader)
}

// failStorage fails the n-th Push call (0-based) with err.
type failStorage struct {
	content.Storage
	n     int
	err   error
	calls int
}

func (s *failStorage) Push(ctx context.Context, expected ocispec.Descriptor, reader io.Reader) error {
	if s.calls == s.n {
		s.calls++
		return s.err
	}
	s.calls++
	return s.Storage.Push(ctx, expected, reader)
}

// deletePassthrough exposes the Delete method of deleter on top of Storage.
type deletePassthrough struct {
	content.Storage
	deleter content.Deleter
}

func (s *deletePassthrough) Delete(ctx context.Context, target ocispec.Descriptor) error {
	return s.deleter.Delete(ctx, target)
}

// cancelAfterFirstPushStorage cancels the context and fails after the first
// successful Push.
type cancelAfterFirstPushStorage struct {
	content.Storage
	cancel context.CancelFunc
	pushes int
}

func (s *cancelAfterFirstPushStorage) Push(ctx context.Context, expected ocispec.Descriptor, reader io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.pushes > 0 {
		s.cancel()
		return context.Canceled
	}
	s.pushes++
	return s.Storage.Push(ctx, expected, reader)
}

func (s *cancelAfterFirstPushStorage) Exists(ctx context.Context, target ocispec.Descriptor) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return s.Storage.Exists(ctx, target)
}

// fetchJSON fetches the content described by desc and unmarshals it into v.
func fetchJSON(t *testing.T, ctx context.Context, fetcher content.Fetcher, desc ocispec.Descriptor, v any) {
	t.Helper()
	data, err := content.FetchAll(ctx, fetcher, desc)
	if err != nil {
		t.Fatalf("failed to fetch %s: %v", desc.Digest, err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("failed to unmarshal %s: %v", desc.Digest, err)
	}
}

// untarFiles extracts the files of an uncompressed tar layer into a map of
// path to content.
func untarFiles(t *testing.T, data []byte) map[string]string {
	t.Helper()
	files := map[string]string{}
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to read tar header: %v", err)
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("failed to read tar content of %q: %v", hdr.Name, err)
		}
		files[hdr.Name] = string(content)
	}
	return files
}

// blobCount returns the number of blobs in the OCI layout rooted at root.
func blobCount(t *testing.T, root string) int {
	t.Helper()
	entries, err := filepath.Glob(filepath.Join(root, "blobs", "sha256", "*"))
	if err != nil {
		t.Fatalf("failed to list blobs: %v", err)
	}
	return len(entries)
}

func TestPackImage_MultiPlatform(t *testing.T) {
	ctx := context.Background()
	store, err := oci.New(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create oci store: %v", err)
	}
	platforms := []PackImagePlatform{
		{
			// mixed case on purpose to verify normalization
			Platform: ocispec.Platform{OS: "Linux", Architecture: "AMD64"},
			Files: []PackImageFile{
				{Path: "etc/config", Content: []byte("cfg")},
				{Path: "bin/app", Content: []byte("app-amd64")},
			},
			ManifestAnnotations: map[string]string{"foo": "bar"},
		},
		{
			Platform: ocispec.Platform{
				OS:           "linux",
				Architecture: "arm64",
				Variant:      "V8",
				// whitespace, duplicates and empty entries on purpose to
				// verify normalization
				OSFeatures: []string{" f2 ", "f1", "f2", ""},
			},
			Files: []PackImageFile{
				{Path: "bin/app", Content: []byte("app-arm64")},
			},
		},
	}
	desc, err := PackImage(ctx, store, platforms, PackImageOptions{
		LayerAnnotations: map[string]string{"layer": "yes"},
	})
	if err != nil {
		t.Fatalf("PackImage() error = %v", err)
	}
	if desc.MediaType != ocispec.MediaTypeImageIndex {
		t.Errorf("PackImage() mediaType = %v, want %v", desc.MediaType, ocispec.MediaTypeImageIndex)
	}

	// verify the top-level index
	var index ocispec.Index
	fetchJSON(t, ctx, store, desc, &index)
	if len(index.Manifests) != 2 {
		t.Fatalf("len(index.Manifests) = %v, want 2", len(index.Manifests))
	}
	if _, ok := index.Annotations[ocispec.AnnotationCreated]; !ok {
		t.Errorf("index.Annotations[%q] missing", ocispec.AnnotationCreated)
	}
	wantPlatforms := []ocispec.Platform{
		{OS: "linux", Architecture: "amd64"},
		{OS: "linux", Architecture: "arm64", Variant: "v8", OSFeatures: []string{"f1", "f2"}},
	}
	wantFiles := []map[string]string{
		{"bin/app": "app-amd64", "etc/config": "cfg"},
		{"bin/app": "app-arm64"},
	}
	for i, manifestDesc := range index.Manifests {
		if manifestDesc.Platform == nil {
			t.Fatalf("index.Manifests[%d].Platform is nil", i)
		}
		if !reflect.DeepEqual(*manifestDesc.Platform, wantPlatforms[i]) {
			t.Errorf("index.Manifests[%d].Platform = %+v, want %+v", i, *manifestDesc.Platform, wantPlatforms[i])
		}

		// verify the platform manifest
		var manifest ocispec.Manifest
		fetchJSON(t, ctx, store, manifestDesc, &manifest)
		if manifest.Config.MediaType != ocispec.MediaTypeImageConfig {
			t.Errorf("manifests[%d].Config.MediaType = %v, want %v", i, manifest.Config.MediaType, ocispec.MediaTypeImageConfig)
		}
		if len(manifest.Layers) != 1 {
			t.Fatalf("len(manifests[%d].Layers) = %v, want 1", i, len(manifest.Layers))
		}
		layerDesc := manifest.Layers[0]
		if layerDesc.MediaType != ocispec.MediaTypeImageLayer {
			t.Errorf("manifests[%d].Layers[0].MediaType = %v, want %v", i, layerDesc.MediaType, ocispec.MediaTypeImageLayer)
		}
		if got := layerDesc.Annotations["layer"]; got != "yes" {
			t.Errorf("manifests[%d].Layers[0].Annotations[\"layer\"] = %q, want %q", i, got, "yes")
		}
		if _, ok := manifest.Annotations[ocispec.AnnotationCreated]; !ok {
			t.Errorf("manifests[%d].Annotations[%q] missing", i, ocispec.AnnotationCreated)
		}

		// verify the image config: the diffID must match the layer digest
		var image ocispec.Image
		fetchJSON(t, ctx, store, manifest.Config, &image)
		if !reflect.DeepEqual(image.Platform, wantPlatforms[i]) {
			t.Errorf("configs[%d].Platform = %+v, want %+v", i, image.Platform, wantPlatforms[i])
		}
		if len(image.RootFS.DiffIDs) != 1 || image.RootFS.DiffIDs[0] != layerDesc.Digest {
			t.Errorf("configs[%d].RootFS.DiffIDs = %v, want [%v]", i, image.RootFS.DiffIDs, layerDesc.Digest)
		}

		// verify the layer content
		layerData, err := content.FetchAll(ctx, store, layerDesc)
		if err != nil {
			t.Fatalf("failed to fetch layer of manifests[%d]: %v", i, err)
		}
		if got := untarFiles(t, layerData); !mapsEqual(got, wantFiles[i]) {
			t.Errorf("manifests[%d] layer files = %v, want %v", i, got, wantFiles[i])
		}
	}
	if got := index.Manifests[0].Annotations["foo"]; got != "bar" {
		t.Errorf("index.Manifests[0].Annotations[\"foo\"] = %q, want %q", got, "bar")
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestPackImage_SinglePlatform(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := oci.New(root)
	if err != nil {
		t.Fatalf("failed to create oci store: %v", err)
	}
	platforms := []PackImagePlatform{
		{
			Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
			Files:    []PackImageFile{{Path: "app", Content: []byte("app")}},
		},
	}
	desc, err := PackImage(ctx, store, platforms, PackImageOptions{})
	if err != nil {
		t.Fatalf("PackImage() error = %v", err)
	}
	// the layout is consistent: a single platform still yields an index
	if desc.MediaType != ocispec.MediaTypeImageIndex {
		t.Errorf("PackImage() mediaType = %v, want %v", desc.MediaType, ocispec.MediaTypeImageIndex)
	}
	var index ocispec.Index
	fetchJSON(t, ctx, store, desc, &index)
	if len(index.Manifests) != 1 {
		t.Fatalf("len(index.Manifests) = %v, want 1", len(index.Manifests))
	}
	// layer, config, manifest, index
	if got := blobCount(t, root); got != 4 {
		t.Errorf("blob count = %v, want 4", got)
	}
}

func TestPackImage_Reproducible(t *testing.T) {
	ctx := context.Background()
	created := "2024-01-01T00:00:00Z"
	platforms := []PackImagePlatform{
		{
			Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
			Files: []PackImageFile{
				{Path: "b.txt", Content: []byte("b")},
				{Path: "a.txt", Content: []byte("a")},
			},
			ManifestAnnotations: map[string]string{ocispec.AnnotationCreated: created},
		},
	}
	opts := PackImageOptions{
		IndexAnnotations: map[string]string{ocispec.AnnotationCreated: created},
	}
	store1, err := oci.New(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create oci store: %v", err)
	}
	desc1, err := PackImage(ctx, store1, platforms, opts)
	if err != nil {
		t.Fatalf("PackImage() error = %v", err)
	}
	store2, err := oci.New(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create oci store: %v", err)
	}
	desc2, err := PackImage(ctx, store2, platforms, opts)
	if err != nil {
		t.Fatalf("PackImage() error = %v", err)
	}
	if desc1.Digest != desc2.Digest {
		t.Errorf("PackImage() is not reproducible: %v != %v", desc1.Digest, desc2.Digest)
	}
}

func TestPackImage_NoPlatform(t *testing.T) {
	ctx := context.Background()
	cs := &countingStorage{Storage: memory.New()}
	if _, err := PackImage(ctx, cs, nil, PackImageOptions{}); err == nil {
		t.Fatal("PackImage() error = nil, want error")
	}
	if cs.pushes != 0 {
		t.Errorf("PackImage() pushed %d blobs, want 0", cs.pushes)
	}
}

func TestPackImage_InvalidPlatform(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name     string
		platform ocispec.Platform
	}{
		{"empty OS", ocispec.Platform{Architecture: "amd64"}},
		{"blank OS", ocispec.Platform{OS: "  ", Architecture: "amd64"}},
		{"empty architecture", ocispec.Platform{OS: "linux"}},
		{"blank architecture", ocispec.Platform{OS: "linux", Architecture: "  "}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := &countingStorage{Storage: memory.New()}
			platforms := []PackImagePlatform{{Platform: tt.platform}}
			_, err := PackImage(ctx, cs, platforms, PackImageOptions{})
			if !errors.Is(err, ErrInvalidPlatform) {
				t.Errorf("PackImage() error = %v, want %v", err, ErrInvalidPlatform)
			}
			if cs.pushes != 0 {
				t.Errorf("PackImage() pushed %d blobs, want 0", cs.pushes)
			}
		})
	}
}

func TestPackImage_DuplicatePlatform(t *testing.T) {
	ctx := context.Background()
	cs := &countingStorage{Storage: memory.New()}
	platforms := []PackImagePlatform{
		{Platform: ocispec.Platform{OS: "Linux", Architecture: "AMD64"}},
		// duplicates the first platform after normalization
		{Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}},
	}
	_, err := PackImage(ctx, cs, platforms, PackImageOptions{})
	if !errors.Is(err, errdef.ErrAlreadyExists) {
		t.Errorf("PackImage() error = %v, want %v", err, errdef.ErrAlreadyExists)
	}
	if cs.pushes != 0 {
		t.Errorf("PackImage() pushed %d blobs, want 0", cs.pushes)
	}
}

func TestPackImage_DuplicatePath(t *testing.T) {
	ctx := context.Background()
	store, err := oci.New(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create oci store: %v", err)
	}
	platforms := []PackImagePlatform{
		{
			Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
			Files: []PackImageFile{
				{Path: "a/b.txt", Content: []byte("first")},
				// all paths below normalize to "a/b.txt"; the last one wins
				{Path: "./a/b.txt", Content: []byte("second")},
				{Path: "a//b.txt", Content: []byte("third")},
				{Path: "c.txt", Content: []byte("c")},
			},
		},
	}
	desc, err := PackImage(ctx, store, platforms, PackImageOptions{})
	if err != nil {
		t.Fatalf("PackImage() error = %v", err)
	}
	var index ocispec.Index
	fetchJSON(t, ctx, store, desc, &index)
	var manifest ocispec.Manifest
	fetchJSON(t, ctx, store, index.Manifests[0], &manifest)
	if len(manifest.Layers) != 1 {
		t.Fatalf("len(manifest.Layers) = %v, want 1", len(manifest.Layers))
	}
	layerData, err := content.FetchAll(ctx, store, manifest.Layers[0])
	if err != nil {
		t.Fatalf("failed to fetch layer: %v", err)
	}
	want := map[string]string{"a/b.txt": "third", "c.txt": "c"}
	if got := untarFiles(t, layerData); !mapsEqual(got, want) {
		t.Errorf("layer files = %v, want %v", got, want)
	}
	// the diffID recorded in the config must match the final layer
	var image ocispec.Image
	fetchJSON(t, ctx, store, manifest.Config, &image)
	if len(image.RootFS.DiffIDs) != 1 || image.RootFS.DiffIDs[0] != manifest.Layers[0].Digest {
		t.Errorf("RootFS.DiffIDs = %v, want [%v]", image.RootFS.DiffIDs, manifest.Layers[0].Digest)
	}
}

func TestPackImage_InvalidPath(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		path string
	}{
		{"empty", ""},
		{"absolute", "/abs"},
		{"dot", "."},
		{"dot dot", ".."},
		{"escapes root", "../x"},
		{"resolves outside root", "a/../../b"},
		{"contains NUL", "a\x00b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := &countingStorage{Storage: memory.New()}
			platforms := []PackImagePlatform{
				{
					Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
					Files:    []PackImageFile{{Path: tt.path, Content: []byte("x")}},
				},
			}
			_, err := PackImage(ctx, cs, platforms, PackImageOptions{})
			if !errors.Is(err, errInvalidFilePath) {
				t.Errorf("PackImage() error = %v, want %v", err, errInvalidFilePath)
			}
			if cs.pushes != 0 {
				t.Errorf("PackImage() pushed %d blobs, want 0", cs.pushes)
			}
		})
	}
}

func TestPackImage_InvalidAnnotation(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		platforms []PackImagePlatform
		opts      PackImageOptions
	}{
		{
			name: "invalid manifest created annotation",
			platforms: []PackImagePlatform{
				{
					Platform:            ocispec.Platform{OS: "linux", Architecture: "amd64"},
					ManifestAnnotations: map[string]string{ocispec.AnnotationCreated: "not-a-time"},
				},
			},
		},
		{
			name: "invalid index created annotation",
			platforms: []PackImagePlatform{
				{Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}},
			},
			opts: PackImageOptions{
				IndexAnnotations: map[string]string{ocispec.AnnotationCreated: "not-a-time"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := &countingStorage{Storage: memory.New()}
			_, err := PackImage(ctx, cs, tt.platforms, tt.opts)
			if !errors.Is(err, ErrInvalidDateTimeFormat) {
				t.Errorf("PackImage() error = %v, want %v", err, ErrInvalidDateTimeFormat)
			}
			if cs.pushes != 0 {
				t.Errorf("PackImage() pushed %d blobs, want 0", cs.pushes)
			}
		})
	}
}

func TestPackImage_UnsupportedDigestAlgorithm(t *testing.T) {
	ctx := context.Background()
	cs := &countingStorage{Storage: memory.New()}
	platforms := []PackImagePlatform{
		{Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}},
	}
	_, err := PackImage(ctx, cs, platforms, PackImageOptions{DigestAlgorithm: "sha1"})
	if !errors.Is(err, errdef.ErrUnsupported) {
		t.Errorf("PackImage() error = %v, want %v", err, errdef.ErrUnsupported)
	}
	if cs.pushes != 0 {
		t.Errorf("PackImage() pushed %d blobs, want 0", cs.pushes)
	}
}

func TestPackImage_Cancel(t *testing.T) {
	root := t.TempDir()
	store, err := oci.New(root)
	if err != nil {
		t.Fatalf("failed to create oci store: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs := &cancelAfterFirstPushStorage{Storage: store, cancel: cancel}
	target := &deletePassthrough{Storage: cs, deleter: store}
	platforms := []PackImagePlatform{
		{
			Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
			Files:    []PackImageFile{{Path: "app", Content: []byte("app")}},
		},
		{
			Platform: ocispec.Platform{OS: "linux", Architecture: "arm64"},
			Files:    []PackImageFile{{Path: "app", Content: []byte("app")}},
		},
	}
	_, err = PackImage(ctx, target, platforms, PackImageOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("PackImage() error = %v, want %v", err, context.Canceled)
	}
	// the content pushed before the cancellation must be cleaned up
	if got := blobCount(t, root); got != 0 {
		t.Errorf("blob count after cancellation = %v, want 0", got)
	}
}

func TestPackImage_FailureCleansUpAndKeepsSharedContent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := oci.New(root)
	if err != nil {
		t.Fatalf("failed to create oci store: %v", err)
	}
	platformA := PackImagePlatform{
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
		Files:    []PackImageFile{{Path: "app", Content: []byte("app-amd64")}},
	}
	platformB := PackImagePlatform{
		Platform: ocispec.Platform{OS: "linux", Architecture: "arm64"},
		Files:    []PackImageFile{{Path: "app", Content: []byte("app-arm64")}},
	}

	// pack platform A first; its content becomes shared content
	indexDesc, err := PackImage(ctx, store, []PackImagePlatform{platformA}, PackImageOptions{})
	if err != nil {
		t.Fatalf("PackImage() error = %v", err)
	}
	wantBlobs := blobCount(t, root)

	// pack A and B together, and fail the push of B's config: the pushes of
	// A's content are skipped as they already exist, B's layer is pushed
	// (push #1 after A's manifest at #0), and B's config push (#2) fails
	injected := errors.New("injected push failure")
	fs := &failStorage{Storage: store, n: 2, err: injected}
	target := &deletePassthrough{Storage: fs, deleter: store}
	_, err = PackImage(ctx, target, []PackImagePlatform{platformA, platformB}, PackImageOptions{})
	if !errors.Is(err, injected) {
		t.Errorf("PackImage() error = %v, want %v", err, injected)
	}

	// B's layer introduced by the failed call must be removed, while the
	// shared content of A must be kept
	if got := blobCount(t, root); got != wantBlobs {
		t.Errorf("blob count after failure = %v, want %v", got, wantBlobs)
	}
	// the previously packed index must still be resolvable
	var index ocispec.Index
	fetchJSON(t, ctx, store, indexDesc, &index)
	if len(index.Manifests) != 1 {
		t.Errorf("len(index.Manifests) = %v, want 1", len(index.Manifests))
	}
}

func TestPackImage_FailureWithoutDeleter(t *testing.T) {
	ctx := context.Background()
	// memory.Store does not implement content.Deleter; cleanup is skipped
	// gracefully
	injected := errors.New("injected push failure")
	fs := &failStorage{Storage: memory.New(), n: 1, err: injected}
	platforms := []PackImagePlatform{
		{
			Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
			Files:    []PackImageFile{{Path: "app", Content: []byte("app")}},
		},
	}
	_, err := PackImage(ctx, fs, platforms, PackImageOptions{})
	if !errors.Is(err, injected) {
		t.Errorf("PackImage() error = %v, want %v", err, injected)
	}
}
