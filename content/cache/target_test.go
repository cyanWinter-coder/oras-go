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

package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"testing"

	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/content/memory"
	"github.com/oras-project/oras-go/v3/registry"
	"github.com/oras-project/oras-go/v3/registry/remote"
	"github.com/oras-project/oras-go/v3/registry/remote/properties"
)

func TestNew(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}

	// create source and push content
	source := memory.New()
	ctx := context.Background()
	if err := source.Push(ctx, desc, bytes.NewReader(content)); err != nil {
		t.Fatal("failed to push content to source:", err)
	}

	// create cache
	cache := memory.New()

	// wrap source with cache
	cachedTarget := CacheReadOnlyTarget(source, cache)

	// verify it returns a ReadOnlyTarget
	if _, ok := cachedTarget.(oras.ReadOnlyTarget); !ok {
		t.Error("CacheReadOnlyTarget() did not return oras.ReadOnlyTarget")
	}
}

func TestTarget_Fetch(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}

	// create source and push content
	source := memory.New()
	ctx := context.Background()
	if err := source.Push(ctx, desc, bytes.NewReader(content)); err != nil {
		t.Fatal("failed to push content to source:", err)
	}

	// create cache
	cache := memory.New()

	// wrap source with cache
	cachedTarget := CacheReadOnlyTarget(source, cache)

	// first fetch - should cache the content
	rc, err := cachedTarget.Fetch(ctx, desc)
	if err != nil {
		t.Fatal("Fetch() error =", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal("Fetch().Read() error =", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal("Fetch().Close() error =", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("Fetch() = %v, want %v", got, content)
	}

	// verify content is now in cache
	exists, err := cache.Exists(ctx, desc)
	if err != nil {
		t.Fatal("cache.Exists() error =", err)
	}
	if !exists {
		t.Error("content was not cached")
	}

	// second fetch - should come from cache
	rc, err = cachedTarget.Fetch(ctx, desc)
	if err != nil {
		t.Fatal("second Fetch() error =", err)
	}
	got, err = io.ReadAll(rc)
	if err != nil {
		t.Fatal("second Fetch().Read() error =", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal("second Fetch().Close() error =", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("second Fetch() = %v, want %v", got, content)
	}
}

func TestTarget_FetchCached(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}

	// create source and push content
	source := memory.New()
	ctx := context.Background()
	if err := source.Push(ctx, desc, bytes.NewReader(content)); err != nil {
		t.Fatal("failed to push content to source:", err)
	}

	// create cache and pre-populate it
	cache := memory.New()
	if err := cache.Push(ctx, desc, bytes.NewReader(content)); err != nil {
		t.Fatal("failed to push content to cache:", err)
	}

	// wrap source with cache
	cachedTarget := CacheReadOnlyTarget(source, cache)

	// fetch should come from cache without hitting source
	rc, err := cachedTarget.Fetch(ctx, desc)
	if err != nil {
		t.Fatal("Fetch() error =", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal("Fetch().Read() error =", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal("Fetch().Close() error =", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("Fetch() = %v, want %v", got, content)
	}
}

func TestTarget_Exists(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}

	// create source and push content
	source := memory.New()
	ctx := context.Background()
	if err := source.Push(ctx, desc, bytes.NewReader(content)); err != nil {
		t.Fatal("failed to push content to source:", err)
	}

	// create empty cache
	cache := memory.New()

	// wrap source with cache
	cachedTarget := CacheReadOnlyTarget(source, cache)

	// content should exist (from source)
	exists, err := cachedTarget.Exists(ctx, desc)
	if err != nil {
		t.Fatal("Exists() error =", err)
	}
	if !exists {
		t.Error("Exists() = false, want true")
	}

	// content not in source but in cache
	content2 := []byte("cached only")
	desc2 := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content2),
		Size:      int64(len(content2)),
	}
	if err := cache.Push(ctx, desc2, bytes.NewReader(content2)); err != nil {
		t.Fatal("failed to push content to cache:", err)
	}

	exists, err = cachedTarget.Exists(ctx, desc2)
	if err != nil {
		t.Fatal("Exists() for cached content error =", err)
	}
	if !exists {
		t.Error("Exists() for cached content = false, want true")
	}

	// content not in source or cache
	content3 := []byte("nonexistent")
	desc3 := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content3),
		Size:      int64(len(content3)),
	}

	exists, err = cachedTarget.Exists(ctx, desc3)
	if err != nil {
		t.Fatal("Exists() for nonexistent content error =", err)
	}
	if exists {
		t.Error("Exists() for nonexistent content = true, want false")
	}
}

func TestTarget_Resolve(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}
	ref := "latest"

	// create source and push content with tag
	source := memory.New()
	ctx := context.Background()
	if err := source.Push(ctx, desc, bytes.NewReader(content)); err != nil {
		t.Fatal("failed to push content to source:", err)
	}
	if err := source.Tag(ctx, desc, ref); err != nil {
		t.Fatal("failed to tag content:", err)
	}

	// create empty cache
	cache := memory.New()

	// wrap source with cache
	cachedTarget := CacheReadOnlyTarget(source, cache)

	// resolve should work (delegates to source)
	gotDesc, err := cachedTarget.Resolve(ctx, ref)
	if err != nil {
		t.Fatal("Resolve() error =", err)
	}
	if !reflect.DeepEqual(gotDesc, desc) {
		t.Errorf("Resolve() = %v, want %v", gotDesc, desc)
	}
}

func TestReferenceTarget_FetchReference(t *testing.T) {
	// setup test data
	manifest := ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config: ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageConfig,
			Digest:    digest.FromBytes([]byte("{}")),
			Size:      2,
		},
		Layers: []ocispec.Descriptor{},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal("failed to marshal manifest:", err)
	}
	manifestDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(manifestBytes),
		Size:      int64(len(manifestBytes)),
	}

	// setup mock registry, counting GET requests to verify cache hits avoid downloads
	var getCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/test/manifests/latest":
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
				w.Header().Set("Docker-Content-Digest", manifestDesc.Digest.String())
				w.Header().Set("Content-Length", strconv.FormatInt(manifestDesc.Size, 10))
			} else if r.Method == http.MethodGet {
				getCount++
				w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
				w.Header().Set("Docker-Content-Digest", manifestDesc.Digest.String())
				w.Write(manifestBytes)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal("failed to parse test server URL:", err)
	}

	// create remote repository
	repo, err := remote.NewRepository(u.Host + "/test")
	if err != nil {
		t.Fatal("failed to create repository:", err)
	}
	repo.Registry.PlainHTTP = true

	// create cache
	cache := memory.New()
	ctx := context.Background()

	// wrap repository with cache
	cachedTarget := CacheReadOnlyTarget(repo, cache)

	// verify it implements ReferenceFetcher
	refFetcher, ok := cachedTarget.(registry.ReferenceFetcher)
	if !ok {
		t.Fatal("cached target does not implement registry.ReferenceFetcher")
	}

	// first fetch (cache miss) - resolves via HEAD then GETs from source and caches
	gotDesc, rc, err := refFetcher.FetchReference(ctx, "latest")
	if err != nil {
		t.Fatal("FetchReference() error =", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal("FetchReference().Read() error =", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal("FetchReference().Close() error =", err)
	}
	if gotDesc.Digest != manifestDesc.Digest {
		t.Errorf("FetchReference() descriptor digest = %v, want %v", gotDesc.Digest, manifestDesc.Digest)
	}
	if !bytes.Equal(got, manifestBytes) {
		t.Errorf("FetchReference() content = %v, want %v", got, manifestBytes)
	}
	if getCount != 1 {
		t.Errorf("expected 1 GET request on cache miss, got %d", getCount)
	}

	// verify content is now in cache
	exists, err := cache.Exists(ctx, manifestDesc)
	if err != nil {
		t.Fatal("cache.Exists() error =", err)
	}
	if !exists {
		t.Error("content was not cached")
	}

	// second fetch (cache hit) - resolves via HEAD only, no GET to source
	gotDesc, rc, err = refFetcher.FetchReference(ctx, "latest")
	if err != nil {
		t.Fatal("second FetchReference() error =", err)
	}
	got, err = io.ReadAll(rc)
	if err != nil {
		t.Fatal("second FetchReference().Read() error =", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal("second FetchReference().Close() error =", err)
	}
	if gotDesc.Digest != manifestDesc.Digest {
		t.Errorf("second FetchReference() descriptor digest = %v, want %v", gotDesc.Digest, manifestDesc.Digest)
	}
	if !bytes.Equal(got, manifestBytes) {
		t.Errorf("second FetchReference() content = %v, want %v", got, manifestBytes)
	}
	if getCount != 1 {
		t.Errorf("expected no additional GET on cache hit, total GET count = %d", getCount)
	}
}

func TestNew_WithReferenceFetcher(t *testing.T) {
	manifest := ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config: ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageConfig,
			Digest:    digest.FromBytes([]byte("{}")),
			Size:      2,
		},
		Layers: []ocispec.Descriptor{},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal("failed to marshal manifest:", err)
	}
	manifestDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(manifestBytes),
		Size:      int64(len(manifestBytes)),
	}

	// setup mock registry
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/test/manifests/latest":
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
				w.Header().Set("Docker-Content-Digest", manifestDesc.Digest.String())
				w.Header().Set("Content-Length", strconv.FormatInt(manifestDesc.Size, 10))
			} else if r.Method == http.MethodGet {
				w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
				w.Header().Set("Docker-Content-Digest", manifestDesc.Digest.String())
				w.Write(manifestBytes)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal("failed to parse test server URL:", err)
	}

	// create remote repository (implements ReferenceFetcher)
	repo, err := remote.NewRepository(u.Host + "/test")
	if err != nil {
		t.Fatal("failed to create repository:", err)
	}
	repo.Registry.PlainHTTP = true

	// create cache
	cache := memory.New()

	// wrap repository with cache
	cachedTarget := CacheReadOnlyTarget(repo, cache)

	// verify it returns a referenceTarget (implements ReferenceFetcher)
	if _, ok := cachedTarget.(registry.ReferenceFetcher); !ok {
		t.Error("CacheReadOnlyTarget() with ReferenceFetcher source did not return registry.ReferenceFetcher")
	}
}

func TestNew_WithoutReferenceFetcher(t *testing.T) {
	// memory.Store does not implement ReferenceFetcher
	source := memory.New()

	// create cache
	cache := memory.New()

	// wrap source with cache
	cachedTarget := CacheReadOnlyTarget(source, cache)

	// verify it does NOT return a referenceTarget
	if _, ok := cachedTarget.(registry.ReferenceFetcher); ok {
		t.Error("CacheReadOnlyTarget() with non-ReferenceFetcher source returned registry.ReferenceFetcher")
	}
}

// errorStorage is a mock storage that returns errors for testing
type errorStorage struct {
	fetchErr  error
	pushErr   error
	existsErr error
	exists    bool
}

func (e *errorStorage) Fetch(_ context.Context, _ ocispec.Descriptor) (io.ReadCloser, error) {
	if e.fetchErr != nil {
		return nil, e.fetchErr
	}
	return io.NopCloser(bytes.NewReader([]byte("cached"))), nil
}

func (e *errorStorage) Push(_ context.Context, _ ocispec.Descriptor, r io.Reader) error {
	if e.pushErr != nil {
		return e.pushErr
	}
	// Drain the reader so a staged content is fully consumed.
	_, _ = io.Copy(io.Discard, r)
	return nil
}

func (e *errorStorage) Exists(_ context.Context, _ ocispec.Descriptor) (bool, error) {
	if e.existsErr != nil {
		return false, e.existsErr
	}
	return e.exists, nil
}

// errorReadCloser is a reader that returns an error on Close
type errorReadCloser struct {
	io.Reader
	closeErr error
}

func (e *errorReadCloser) Close() error {
	return e.closeErr
}

// errorSource is a mock source that returns errors for testing
type errorSource struct {
	fetchErr   error
	resolveErr error
	existsErr  error
}

func (e *errorSource) Fetch(_ context.Context, _ ocispec.Descriptor) (io.ReadCloser, error) {
	return nil, e.fetchErr
}

func (e *errorSource) Resolve(_ context.Context, _ string) (ocispec.Descriptor, error) {
	return ocispec.Descriptor{}, e.resolveErr
}

func (e *errorSource) Exists(_ context.Context, _ ocispec.Descriptor) (bool, error) {
	return false, e.existsErr
}

func TestTarget_Fetch_SourceError(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}

	expectedErr := errors.New("source fetch error")
	source := &errorSource{fetchErr: expectedErr}
	cache := memory.New()

	cachedTarget := CacheReadOnlyTarget(source, cache)
	ctx := context.Background()

	_, err := cachedTarget.Fetch(ctx, desc)
	if err == nil {
		t.Error("Fetch() expected error, got nil")
	}
	if err != expectedErr {
		t.Errorf("Fetch() error = %v, want %v", err, expectedErr)
	}
}

func TestReferenceTarget_FetchReference_Error(t *testing.T) {
	// setup mock registry that returns error
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal("failed to parse test server URL:", err)
	}

	repo, err := remote.NewRepository(u.Host + "/test")
	if err != nil {
		t.Fatal("failed to create repository:", err)
	}
	repo.Registry.PlainHTTP = true

	cache := memory.New()
	ctx := context.Background()

	cachedTarget := CacheReadOnlyTarget(repo, cache)
	refFetcher := cachedTarget.(registry.ReferenceFetcher)

	_, _, err = refFetcher.FetchReference(ctx, "latest")
	if err == nil {
		t.Error("FetchReference() expected error, got nil")
	}
}

func TestTarget_Fetch_CachePushError(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}

	// create source and push content
	source := memory.New()
	ctx := context.Background()
	if err := source.Push(ctx, desc, bytes.NewReader(content)); err != nil {
		t.Fatal("failed to push content to source:", err)
	}

	// cache that fails on push
	errCache := &errorStorage{
		fetchErr: errors.New("not in cache"),
		pushErr:  errors.New("push error"),
	}

	cachedTarget := CacheReadOnlyTarget(source, errCache)

	// fetch - cache push will fail
	rc, err := cachedTarget.Fetch(ctx, desc)
	if err != nil {
		t.Fatal("Fetch() error =", err)
	}

	// Reading the content succeeds: caching no longer interleaves a write
	// with the caller's read.
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal("Fetch().Read() error =", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("Fetch() = %v, want %v", got, content)
	}

	// The cache error is reported by Close, when the staging is committed.
	if err := rc.Close(); err == nil {
		t.Error("Fetch().Close() expected error from failed push, got nil")
	}
}

func TestTarget_Fetch_ReaderCloseError(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}

	closeErr := errors.New("close error")

	// create a source that returns a reader that errors on close
	source := &mockSourceWithCloseError{
		content:  content,
		closeErr: closeErr,
	}

	cache := memory.New()
	cachedTarget := CacheReadOnlyTarget(source, cache)
	ctx := context.Background()

	// fetch
	rc, err := cachedTarget.Fetch(ctx, desc)
	if err != nil {
		t.Fatal("Fetch() error =", err)
	}

	// Read all content
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal("Fetch().Read() error =", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("Fetch() = %v, want %v", got, content)
	}

	// Close should return the reader close error
	err = rc.Close()
	if err == nil {
		t.Error("Fetch().Close() expected error, got nil")
	}
	if err != closeErr {
		t.Errorf("Fetch().Close() error = %v, want %v", err, closeErr)
	}
}

// mockSourceWithCloseError is a source that returns readers that error on close
type mockSourceWithCloseError struct {
	content  []byte
	closeErr error
}

func (m *mockSourceWithCloseError) Fetch(_ context.Context, _ ocispec.Descriptor) (io.ReadCloser, error) {
	return &errorReadCloser{
		Reader:   bytes.NewReader(m.content),
		closeErr: m.closeErr,
	}, nil
}

func (m *mockSourceWithCloseError) Resolve(_ context.Context, _ string) (ocispec.Descriptor, error) {
	return ocispec.Descriptor{}, errors.New("not implemented")
}

func (m *mockSourceWithCloseError) Exists(_ context.Context, _ ocispec.Descriptor) (bool, error) {
	return true, nil
}

// countedSource is a source whose reader records whether it was closed, so
// that a failed cache commit or a cancellation can be shown to release the
// underlying stream.
type countedSource struct {
	content []byte
	reader  *countingReadCloser
}

func (s *countedSource) Fetch(_ context.Context, _ ocispec.Descriptor) (io.ReadCloser, error) {
	s.reader = &countingReadCloser{Reader: bytes.NewReader(s.content)}
	return s.reader, nil
}

func (s *countedSource) Resolve(_ context.Context, _ string) (ocispec.Descriptor, error) {
	return ocispec.Descriptor{}, errors.New("not implemented")
}

func (s *countedSource) Exists(_ context.Context, _ ocispec.Descriptor) (bool, error) {
	return true, nil
}

type countingReadCloser struct {
	io.Reader
	closed int
}

func (r *countingReadCloser) Close() error {
	r.closed++
	return nil
}

func TestTarget_Fetch_Truncated(t *testing.T) {
	content := []byte("hello world")
	// The source provides less content than the descriptor declares.
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)) + 1,
	}
	src := &countedSource{content: content}
	cache := memory.New()
	ctx := context.Background()

	cachedTarget := CacheReadOnlyTarget(src, cache)
	rc, err := cachedTarget.Fetch(ctx, desc)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if _, err := io.ReadAll(rc); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadAll() error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
	if err := rc.Close(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("Close() error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
	if exists, err := cache.Exists(ctx, desc); err != nil || exists {
		t.Errorf("cache.Exists() = %v, %v, want false, nil", exists, err)
	}
	if src.reader.closed == 0 {
		t.Error("source reader was not closed")
	}
}

func TestTarget_Fetch_DigestMismatch(t *testing.T) {
	actual := []byte("hello world")
	declared := []byte("HELLO WORLD") // same size, different digest
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(declared),
		Size:      int64(len(declared)),
	}
	src := &countedSource{content: actual}
	cache := memory.New()
	ctx := context.Background()

	cachedTarget := CacheReadOnlyTarget(src, cache)
	rc, err := cachedTarget.Fetch(ctx, desc)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if _, err := io.ReadAll(rc); !errors.Is(err, content.ErrMismatchedDigest) {
		t.Fatalf("ReadAll() error = %v, want %v", err, content.ErrMismatchedDigest)
	}
	if err := rc.Close(); !errors.Is(err, content.ErrMismatchedDigest) {
		t.Errorf("Close() error = %v, want %v", err, content.ErrMismatchedDigest)
	}
	if exists, err := cache.Exists(ctx, desc); err != nil || exists {
		t.Errorf("cache.Exists() = %v, %v, want false, nil", exists, err)
	}
	if src.reader.closed == 0 {
		t.Error("source reader was not closed")
	}
}

func TestTarget_Fetch_Canceled(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}
	src := &countedSource{content: content}
	cache := memory.New()
	ctx, cancel := context.WithCancel(context.Background())

	cachedTarget := CacheReadOnlyTarget(src, cache)
	rc, err := cachedTarget.Fetch(ctx, desc)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if _, err := io.ReadFull(rc, make([]byte, 4)); err != nil {
		t.Fatalf("ReadFull() error = %v", err)
	}
	cancel()

	if _, err := io.ReadAll(rc); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadAll() error = %v, want %v", err, context.Canceled)
	}
	if err := rc.Close(); !errors.Is(err, context.Canceled) {
		t.Errorf("Close() error = %v, want %v", err, context.Canceled)
	}
	if exists, err := cache.Exists(context.Background(), desc); err != nil || exists {
		t.Errorf("cache.Exists() = %v, %v, want false, nil", exists, err)
	}
	if src.reader.closed == 0 {
		t.Error("source reader was not closed")
	}
}

func TestTarget_Fetch_StagedNotVisible(t *testing.T) {
	content := []byte("hello world")
	desc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}
	src := &countedSource{content: content}
	cache := memory.New()
	ctx := context.Background()

	cachedTarget := CacheReadOnlyTarget(src, cache)

	// A reader that is opened but not closed has not committed anything.
	rc, err := cachedTarget.Fetch(ctx, desc)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if exists, err := cache.Exists(ctx, desc); err != nil || exists {
		t.Errorf("cache.Exists() before Close = %v, %v, want false, nil", exists, err)
	}

	// A second Fetch while the first is still open misses the cache and goes
	// to the source again.
	rc2, err := cachedTarget.Fetch(ctx, desc)
	if err != nil {
		t.Fatalf("second Fetch() error = %v", err)
	}
	if got, err := io.ReadAll(rc2); err != nil || !bytes.Equal(got, content) {
		t.Errorf("second Fetch() = %q, %v, want %q, nil", got, err, content)
	}
	if err := rc2.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	// The second commit makes the content visible; the first is then
	// idempotent.
	if err := rc.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if exists, err := cache.Exists(ctx, desc); err != nil || !exists {
		t.Errorf("cache.Exists() after Close = %v, %v, want true, nil", exists, err)
	}

	// A following Fetch is served from the cache, not the source.
	before := src.reader
	rc3, err := cachedTarget.Fetch(ctx, desc)
	if err != nil {
		t.Fatalf("third Fetch() error = %v", err)
	}
	if got, err := io.ReadAll(rc3); err != nil || !bytes.Equal(got, content) {
		t.Errorf("third Fetch() = %q, %v, want %q, nil", got, err, content)
	}
	if err := rc3.Close(); err != nil {
		t.Fatalf("third Close() error = %v", err)
	}
	if src.reader != before {
		t.Error("third Fetch() went to the source despite a cache hit")
	}
}

func TestReferenceTarget_FetchReference_CacheFetchFailsFallsBack(t *testing.T) {
	manifest := ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config: ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageConfig,
			Digest:    digest.FromBytes([]byte("{}")),
			Size:      2,
		},
		Layers: []ocispec.Descriptor{},
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal("failed to marshal manifest:", err)
	}
	manifestDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(manifestBytes),
		Size:      int64(len(manifestBytes)),
	}

	// setup mock registry
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/test/manifests/latest":
			if r.Method == http.MethodHead {
				w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
				w.Header().Set("Docker-Content-Digest", manifestDesc.Digest.String())
				w.Header().Set("Content-Length", strconv.FormatInt(manifestDesc.Size, 10))
			} else if r.Method == http.MethodGet {
				w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
				w.Header().Set("Docker-Content-Digest", manifestDesc.Digest.String())
				w.Write(manifestBytes)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal("failed to parse test server URL:", err)
	}

	repo, err := remote.NewRepository(u.Host + "/test")
	if err != nil {
		t.Fatal("failed to create repository:", err)
	}
	repo.Registry.PlainHTTP = true

	// Cache that always fails on Fetch — simulates a corrupt or unavailable cache.
	// FetchReference should fall back to the source and succeed.
	errCache := &errorStorage{
		fetchErr: errors.New("cache fetch error"),
	}

	ctx := context.Background()
	cachedTarget := CacheReadOnlyTarget(repo, errCache)
	refFetcher := cachedTarget.(registry.ReferenceFetcher)

	gotDesc, rc, err := refFetcher.FetchReference(ctx, "latest")
	if err != nil {
		t.Fatal("FetchReference() expected fallback to source, got error:", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal("FetchReference().Read() error =", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal("FetchReference().Close() error =", err)
	}
	if gotDesc.Digest != manifestDesc.Digest {
		t.Errorf("FetchReference() descriptor digest = %v, want %v", gotDesc.Digest, manifestDesc.Digest)
	}
	if !bytes.Equal(got, manifestBytes) {
		t.Errorf("FetchReference() content = %v, want %v", got, manifestBytes)
	}
}

// capabilitySource is a source that exposes a configurable set of optional
// interfaces through capabilitySourceWith.
type capabilitySource struct {
	*memory.Store
	parseCalls     int
	referrersCalls int
}

func (s *capabilitySource) FetchReference(ctx context.Context, reference string) (ocispec.Descriptor, io.ReadCloser, error) {
	desc, err := s.Resolve(ctx, reference)
	if err != nil {
		return ocispec.Descriptor{}, nil, err
	}
	rc, err := s.Fetch(ctx, desc)
	return desc, rc, err
}

func (s *capabilitySource) ParseReference(reference string) (properties.Reference, error) {
	s.parseCalls++
	return properties.Reference{Registry: "registry.example", Repository: "test", Tag: reference}, nil
}

func (s *capabilitySource) Referrers(_ context.Context, desc ocispec.Descriptor, _ string, fn func([]ocispec.Descriptor) error) error {
	s.referrersCalls++
	return fn([]ocispec.Descriptor{{MediaType: "referrer", Digest: desc.Digest}})
}

// capabilitySourceWith returns a source backed by s that implements only the
// requested optional interfaces.
func capabilitySourceWith(s *capabilitySource, fetcher, parser, lister bool) oras.ReadOnlyTarget {
	type (
		F = registry.ReferenceFetcher
		P = registry.ReferenceParser
		L = registry.ReferrerLister
	)
	base := s.Store
	switch {
	case fetcher && parser && lister:
		return struct {
			*memory.Store
			F
			P
			L
		}{base, s, s, s}
	case fetcher && parser:
		return struct {
			*memory.Store
			F
			P
		}{base, s, s}
	case fetcher && lister:
		return struct {
			*memory.Store
			F
			L
		}{base, s, s}
	case parser && lister:
		return struct {
			*memory.Store
			P
			L
		}{base, s, s}
	case fetcher:
		return struct {
			*memory.Store
			F
		}{base, s}
	case parser:
		return struct {
			*memory.Store
			P
		}{base, s}
	case lister:
		return struct {
			*memory.Store
			L
		}{base, s}
	default:
		return struct{ *memory.Store }{base}
	}
}

func TestCacheReadOnlyTarget_OptionalInterfaces(t *testing.T) {
	for _, fetcher := range []bool{false, true} {
		for _, parser := range []bool{false, true} {
			for _, lister := range []bool{false, true} {
				name := "fetcher=" + strconv.FormatBool(fetcher) +
					",parser=" + strconv.FormatBool(parser) +
					",lister=" + strconv.FormatBool(lister)
				t.Run(name, func(t *testing.T) {
					source := capabilitySourceWith(&capabilitySource{Store: memory.New()}, fetcher, parser, lister)
					cached := CacheReadOnlyTarget(source, memory.New())

					// the source must expose exactly the requested interfaces
					_, srcFetcher := source.(registry.ReferenceFetcher)
					_, srcParser := source.(registry.ReferenceParser)
					_, srcLister := source.(registry.ReferrerLister)
					if srcFetcher != fetcher || srcParser != parser || srcLister != lister {
						t.Fatalf("test source exposes fetcher=%v parser=%v lister=%v", srcFetcher, srcParser, srcLister)
					}

					if _, ok := cached.(registry.ReferenceFetcher); ok != fetcher {
						t.Errorf("cached target implements registry.ReferenceFetcher = %v, want %v", ok, fetcher)
					}
					if _, ok := cached.(registry.ReferenceParser); ok != parser {
						t.Errorf("cached target implements registry.ReferenceParser = %v, want %v", ok, parser)
					}
					if _, ok := cached.(registry.ReferrerLister); ok != lister {
						t.Errorf("cached target implements registry.ReferrerLister = %v, want %v", ok, lister)
					}
					// interfaces the cache never passes through must not leak
					if _, ok := cached.(registry.TagLister); ok {
						t.Error("cached target implements registry.TagLister")
					}
					if _, ok := cached.(content.Pusher); ok {
						t.Error("cached target implements content.Pusher")
					}
				})
			}
		}
	}
}

func TestCacheReadOnlyTarget_PassThrough(t *testing.T) {
	ctx := context.Background()
	blob := []byte("hello world")
	blobDesc := ocispec.Descriptor{
		MediaType: "test",
		Digest:    digest.FromBytes(blob),
		Size:      int64(len(blob)),
	}

	src := &capabilitySource{Store: memory.New()}
	if err := src.Push(ctx, blobDesc, bytes.NewReader(blob)); err != nil {
		t.Fatal("failed to push content to source:", err)
	}
	if err := src.Tag(ctx, blobDesc, "latest"); err != nil {
		t.Fatal("failed to tag content in source:", err)
	}
	cached := CacheReadOnlyTarget(capabilitySourceWith(src, true, true, true), memory.New())

	// ParseReference is answered by the source
	ref, err := cached.(registry.ReferenceParser).ParseReference("latest")
	if err != nil {
		t.Fatal("ParseReference() error =", err)
	}
	if want := (properties.Reference{Registry: "registry.example", Repository: "test", Tag: "latest"}); ref != want {
		t.Errorf("ParseReference() = %v, want %v", ref, want)
	}
	if src.parseCalls != 1 {
		t.Errorf("source ParseReference calls = %d, want 1", src.parseCalls)
	}

	// Referrers always go to the source and are not cached
	lister := cached.(registry.ReferrerLister)
	for i := 1; i <= 2; i++ {
		var got []ocispec.Descriptor
		if err := lister.Referrers(ctx, blobDesc, "", func(referrers []ocispec.Descriptor) error {
			got = append(got, referrers...)
			return nil
		}); err != nil {
			t.Fatal("Referrers() error =", err)
		}
		if len(got) != 1 || got[0].Digest != blobDesc.Digest {
			t.Errorf("Referrers() = %v, want one referrer of %s", got, blobDesc.Digest)
		}
		if src.referrersCalls != i {
			t.Errorf("source Referrers calls = %d, want %d", src.referrersCalls, i)
		}
	}

	// FetchReference is still served through the cache
	desc, rc, err := cached.(registry.ReferenceFetcher).FetchReference(ctx, "latest")
	if err != nil {
		t.Fatal("FetchReference() error =", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal("failed to read fetched content:", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal("failed to close fetched content:", err)
	}
	if desc.Digest != blobDesc.Digest || !bytes.Equal(got, blob) {
		t.Errorf("FetchReference() = %v %q, want %v %q", desc, got, blobDesc, blob)
	}
}
