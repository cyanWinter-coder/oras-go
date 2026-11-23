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
	"errors"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/content/memory"
	"github.com/oras-project/oras-go/v3/errdef"
)

// sourceReader is an io.ReadCloser that records whether it was closed and how
// many bytes were served.
type sourceReader struct {
	r      io.Reader
	closed int
	served int64
}

func (s *sourceReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	s.served += int64(n)
	return n, err
}

func (s *sourceReader) Close() error {
	s.closed++
	return nil
}

func descOf(mediaType string, content []byte) ocispec.Descriptor {
	return ocispec.Descriptor{
		MediaType: mediaType,
		Digest:    digest.FromBytes(content),
		Size:      int64(len(content)),
	}
}

// assertCacheState checks whether the cache holds the content and that the
// source reader was closed.
func assertCacheState(t *testing.T, cache content.Storage, desc ocispec.Descriptor, src *sourceReader, wantCached bool) {
	t.Helper()
	exists, err := cache.Exists(context.Background(), desc)
	if err != nil {
		t.Fatalf("cache.Exists() error = %v", err)
	}
	if exists != wantCached {
		t.Errorf("cache.Exists() = %v, want %v", exists, wantCached)
	}
	if src.closed == 0 {
		t.Error("source reader was not closed")
	}
}

func TestCacheReader_Success(t *testing.T) {
	ctx := context.Background()
	contentBytes := []byte("hello world")
	desc := descOf("test", contentBytes)
	src := &sourceReader{r: bytes.NewReader(contentBytes)}
	cache := memory.New()

	rc := newCacheReader(ctx, src, desc, cache)

	// Before Close the staged content must not be visible to ordinary reads.
	if exists, err := cache.Exists(ctx, desc); err != nil || exists {
		t.Fatalf("cache.Exists() before Close = %v, %v, want false, nil", exists, err)
	}

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if !bytes.Equal(got, contentBytes) {
		t.Errorf("ReadAll() = %q, want %q", got, contentBytes)
	}

	// Still not visible before Close.
	if exists, err := cache.Exists(ctx, desc); err != nil || exists {
		t.Fatalf("cache.Exists() after ReadAll = %v, %v, want false, nil", exists, err)
	}

	if err := rc.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	assertCacheState(t, cache, desc, src, true)

	// Close is idempotent.
	if err := rc.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}

	// Read after Close reports the closed reader.
	if _, err := rc.Read(make([]byte, 1)); !errors.Is(err, errReaderClosed) {
		t.Errorf("Read() after Close error = %v, want %v", err, errReaderClosed)
	}

	// The cached content is complete and readable.
	cached, err := content.FetchAll(ctx, cache, desc)
	if err != nil {
		t.Fatalf("content.FetchAll() error = %v", err)
	}
	if !bytes.Equal(cached, contentBytes) {
		t.Errorf("cached content = %q, want %q", cached, contentBytes)
	}
	if src.served != desc.Size {
		t.Errorf("source served %d bytes, want %d", src.served, desc.Size)
	}
}

func TestCacheReader_TrailingData(t *testing.T) {
	ctx := context.Background()
	contentBytes := []byte("hello world")
	desc := descOf("test", contentBytes)
	// The source returns more content than the descriptor declares.
	src := &sourceReader{r: bytes.NewReader(append(append([]byte{}, contentBytes...), "extra"...))}
	cache := memory.New()

	rc := newCacheReader(ctx, src, desc, cache)
	_, err := io.ReadAll(rc)
	if !errors.Is(err, content.ErrTrailingData) {
		t.Fatalf("ReadAll() error = %v, want %v", err, content.ErrTrailingData)
	}
	// Reading stops immediately after the first extra byte.
	if src.served != desc.Size+1 {
		t.Errorf("source served %d bytes, want %d", src.served, desc.Size+1)
	}
	assertCacheState(t, cache, desc, src, false)

	// Close repeats the sticky read error and does not cache.
	if err := rc.Close(); !errors.Is(err, content.ErrTrailingData) {
		t.Errorf("Close() error = %v, want %v", err, content.ErrTrailingData)
	}
	if exists, err := cache.Exists(ctx, desc); err != nil || exists {
		t.Errorf("cache.Exists() = %v, %v, want false, nil", exists, err)
	}
}

func TestCacheReader_Truncated(t *testing.T) {
	ctx := context.Background()
	contentBytes := []byte("hello world")
	desc := descOf("test", contentBytes)
	desc.Size += 5 // declare more than the source provides
	src := &sourceReader{r: bytes.NewReader(contentBytes)}
	cache := memory.New()

	rc := newCacheReader(ctx, src, desc, cache)
	_, err := io.ReadAll(rc)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadAll() error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
	assertCacheState(t, cache, desc, src, false)
	if err := rc.Close(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("Close() error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
}

func TestCacheReader_CloseBeforeReading(t *testing.T) {
	ctx := context.Background()
	contentBytes := []byte("hello world")
	desc := descOf("test", contentBytes)
	src := &sourceReader{r: bytes.NewReader(contentBytes)}
	cache := memory.New()

	rc := newCacheReader(ctx, src, desc, cache)
	if err := rc.Close(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Close() error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
	assertCacheState(t, cache, desc, src, false)
}

func TestCacheReader_DigestMismatch(t *testing.T) {
	ctx := context.Background()
	contentBytes := []byte("hello world")
	otherBytes := []byte("HELLO WORLD") // same size, different digest
	desc := descOf("test", otherBytes)
	src := &sourceReader{r: bytes.NewReader(contentBytes)}
	cache := memory.New()

	rc := newCacheReader(ctx, src, desc, cache)
	_, err := io.ReadAll(rc)
	if !errors.Is(err, content.ErrMismatchedDigest) {
		t.Fatalf("ReadAll() error = %v, want %v", err, content.ErrMismatchedDigest)
	}
	assertCacheState(t, cache, desc, src, false)

	// Close repeats the sticky verification error.
	if err := rc.Close(); !errors.Is(err, content.ErrMismatchedDigest) {
		t.Errorf("Close() error = %v, want %v", err, content.ErrMismatchedDigest)
	}
	if exists, err := cache.Exists(ctx, desc); err != nil || exists {
		t.Errorf("cache.Exists() = %v, %v, want false, nil", exists, err)
	}
}

func TestCacheReader_DigestMismatchOnClose(t *testing.T) {
	ctx := context.Background()
	contentBytes := []byte("hello world")
	otherBytes := []byte("HELLO WORLD")
	desc := descOf("test", otherBytes)
	src := &sourceReader{r: bytes.NewReader(contentBytes)}
	cache := memory.New()

	rc := newCacheReader(ctx, src, desc, cache)
	// Read exactly the declared size without triggering the final probe.
	buf := make([]byte, desc.Size)
	if _, err := io.ReadFull(rc, buf); err != nil {
		t.Fatalf("ReadFull() error = %v", err)
	}
	// Verification happens in Close when the caller stops at the budget.
	if err := rc.Close(); !errors.Is(err, content.ErrMismatchedDigest) {
		t.Fatalf("Close() error = %v, want %v", err, content.ErrMismatchedDigest)
	}
	assertCacheState(t, cache, desc, src, false)
}

func TestCacheReader_CancelDuringRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	contentBytes := []byte("hello world")
	desc := descOf("test", contentBytes)
	src := &sourceReader{r: bytes.NewReader(contentBytes)}
	cache := memory.New()

	rc := newCacheReader(ctx, src, desc, cache)
	if _, err := io.ReadFull(rc, make([]byte, 4)); err != nil {
		t.Fatalf("ReadFull() error = %v", err)
	}
	cancel()

	_, err := io.ReadAll(rc)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadAll() error = %v, want %v", err, context.Canceled)
	}
	assertCacheState(t, cache, desc, src, false)

	if err := rc.Close(); !errors.Is(err, context.Canceled) {
		t.Errorf("Close() error = %v, want %v", err, context.Canceled)
	}
	if exists, err := cache.Exists(ctx, desc); err != nil || exists {
		t.Errorf("cache.Exists() = %v, %v, want false, nil", exists, err)
	}
}

func TestCacheReader_CancelBeforeCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	contentBytes := []byte("hello world")
	desc := descOf("test", contentBytes)
	src := &sourceReader{r: bytes.NewReader(contentBytes)}
	cache := memory.New()

	rc := newCacheReader(ctx, src, desc, cache)
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	cancel()

	// A whole content read with a canceled context must not be committed.
	if err := rc.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close() error = %v, want %v", err, context.Canceled)
	}
	if exists, err := cache.Exists(context.Background(), desc); err != nil || exists {
		t.Errorf("cache.Exists() = %v, %v, want false, nil", exists, err)
	}
	if src.closed == 0 {
		t.Error("source reader was not closed")
	}
}

func TestCacheReader_SourceReadError(t *testing.T) {
	ctx := context.Background()
	contentBytes := []byte("hello world")
	desc := descOf("test", contentBytes)
	readErr := errors.New("read failed")
	src := &sourceReader{r: io.MultiReader(bytes.NewReader(contentBytes[:5]), errorReader{readErr})}
	cache := memory.New()

	rc := newCacheReader(ctx, src, desc, cache)
	_, err := io.ReadAll(rc)
	if !errors.Is(err, readErr) {
		t.Fatalf("ReadAll() error = %v, want %v", err, readErr)
	}
	assertCacheState(t, cache, desc, src, false)
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestCacheReader_CommitIdempotent(t *testing.T) {
	ctx := context.Background()
	contentBytes := []byte("hello world")
	desc := descOf("test", contentBytes)
	cache := memory.New()

	// The first reader commits the content.
	first := &sourceReader{r: bytes.NewReader(contentBytes)}
	rc := newCacheReader(ctx, first, desc, cache)
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// A second reader commits the same content again; the cache reports
	// ErrAlreadyExists, which is not an error for the caller.
	second := &sourceReader{r: bytes.NewReader(contentBytes)}
	rc = newCacheReader(ctx, second, desc, cache)
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil for an already-cached content", err)
	}

	cached, err := content.FetchAll(ctx, cache, desc)
	if err != nil {
		t.Fatalf("content.FetchAll() error = %v", err)
	}
	if !bytes.Equal(cached, contentBytes) {
		t.Errorf("cached content = %q, want %q", cached, contentBytes)
	}
}

func TestCacheReader_ConcurrentCommit(t *testing.T) {
	ctx := context.Background()
	contentBytes := []byte("hello world")
	desc := descOf("test", contentBytes)
	cache := memory.New()

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			src := &sourceReader{r: bytes.NewReader(contentBytes)}
			rc := newCacheReader(ctx, src, desc, cache)
			if _, err := io.ReadAll(rc); err != nil {
				errs[i] = err
				return
			}
			errs[i] = rc.Close()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("reader %d error = %v", i, err)
		}
	}
	cached, err := content.FetchAll(ctx, cache, desc)
	if err != nil {
		t.Fatalf("content.FetchAll() error = %v", err)
	}
	if !bytes.Equal(cached, contentBytes) {
		t.Errorf("cached content = %q, want %q", cached, contentBytes)
	}
}

func TestCacheReader_CommitError(t *testing.T) {
	ctx := context.Background()
	contentBytes := []byte("hello world")
	desc := descOf("test", contentBytes)
	src := &sourceReader{r: bytes.NewReader(contentBytes)}
	pushErr := errors.New("push failed")
	cache := &errorStorage{fetchErr: errdef.ErrNotFound, pushErr: pushErr}

	rc := newCacheReader(ctx, src, desc, cache)
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if err := rc.Close(); !errors.Is(err, pushErr) {
		t.Fatalf("Close() error = %v, want %v", err, pushErr)
	}
	assertCacheState(t, cache, desc, src, false)
}

func TestCacheReader_UnsupportedDescriptor(t *testing.T) {
	ctx := context.Background()
	contentBytes := []byte("hello world")
	cache := memory.New()

	tests := []struct {
		name string
		desc ocispec.Descriptor
	}{
		{
			name: "invalid digest",
			desc: ocispec.Descriptor{MediaType: "test", Digest: "invalid", Size: int64(len(contentBytes))},
		},
		{
			name: "unknown size",
			desc: ocispec.Descriptor{MediaType: "test", Digest: digest.FromBytes(contentBytes), Size: -1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &sourceReader{r: bytes.NewReader(contentBytes)}
			rc := newCacheReader(ctx, src, tt.desc, cache)
			if rc != src {
				t.Fatal("newCacheReader() did not return the source reader unchanged")
			}
		})
	}
}

func TestNewStaging(t *testing.T) {
	tests := []struct {
		name    string
		size    int64
		wantMem bool
	}{
		{name: "zero", size: 0, wantMem: true},
		{name: "at memory limit", size: stagingMemoryLimit, wantMem: true},
		{name: "above memory limit", size: stagingMemoryLimit + 1, wantMem: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := newStaging(tt.size)
			if err != nil {
				t.Fatalf("newStaging() error = %v", err)
			}
			_, isMemory := s.(*memoryStaging)
			if isMemory != tt.wantMem {
				t.Errorf("newStaging(%d) is memory staging = %v, want %v", tt.size, isMemory, tt.wantMem)
			}
			defer s.close()
		})
	}
}

func TestFileStaging(t *testing.T) {
	s, err := newFileStaging()
	if err != nil {
		t.Fatalf("newFileStaging() error = %v", err)
	}
	fs := s.(*fileStaging)
	name := fs.name

	data := []byte("staged content")
	if _, err := s.Write(data); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	got, err := io.ReadAll(s.reader())
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("reader() = %q, want %q", got, data)
	}

	if err := s.close(); err != nil {
		t.Fatalf("close() error = %v", err)
	}
	if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("staging file %s still exists after close: %v", name, err)
	}
}

func TestMemoryStaging(t *testing.T) {
	var s staging = &memoryStaging{}
	data := []byte("staged content")
	if _, err := s.Write(data); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	got, err := io.ReadAll(s.reader())
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("reader() = %q, want %q", got, data)
	}
	if err := s.close(); err != nil {
		t.Errorf("close() error = %v", err)
	}
}

// alreadyExistsStorage is a storage that reports every push as a duplicate.
type alreadyExistsStorage struct{ *memory.Store }

func (s *alreadyExistsStorage) Push(context.Context, ocispec.Descriptor, io.Reader) error {
	return errdef.ErrAlreadyExists
}

func TestCacheReader_CommitAlreadyExists(t *testing.T) {
	ctx := context.Background()
	contentBytes := []byte("hello world")
	desc := descOf("test", contentBytes)
	src := &sourceReader{r: bytes.NewReader(contentBytes)}
	cache := &alreadyExistsStorage{Store: memory.New()}

	rc := newCacheReader(ctx, src, desc, cache)
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil for an already-cached content", err)
	}
}

func TestCacheReader_LargeContentUsesFileStaging(t *testing.T) {
	ctx := context.Background()
	// Larger than the in-memory staging limit, so the content is staged in a
	// temporary file.
	contentBytes := bytes.Repeat([]byte("0123456789abcdef"), stagingMemoryLimit/16+1)
	desc := descOf("test", contentBytes)
	src := &sourceReader{r: bytes.NewReader(contentBytes)}
	cache := memory.New()

	rc := newCacheReader(ctx, src, desc, cache)
	cr := rc.(*cacheReader)
	if _, ok := cr.stage.(*fileStaging); !ok {
		t.Fatalf("stage = %T, want *fileStaging", cr.stage)
	}

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if !bytes.Equal(got, contentBytes) {
		t.Error("ReadAll() returned content different from the source")
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	cached, err := content.FetchAll(ctx, cache, desc)
	if err != nil {
		t.Fatalf("content.FetchAll() error = %v", err)
	}
	if !bytes.Equal(cached, contentBytes) {
		t.Error("cached content differs from the source")
	}
}
