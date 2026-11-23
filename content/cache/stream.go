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
	_ "crypto/sha256" // verify content cached with the default digest algorithm
	_ "crypto/sha512" // verify content cached with SHA-384 or SHA-512
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/errdef"
)

// stagingMemoryLimit is the largest content that is staged in memory while it
// is being verified. Larger content is staged in a temporary file so that a
// large blob does not have to fit in memory before it can be cached.
const stagingMemoryLimit = 4 << 20 // 4 MiB

// errReaderClosed is returned by reads after the cache reader is closed.
var errReaderClosed = errors.New("cache: reader is closed")

// newCacheReader returns a reader that serves the content of src to the caller
// while staging a copy for the cache. The staged copy becomes part of the cache
// only when the whole content has been read, verified against desc, and the
// reader is closed. Until then it lives under a temporary identity that Fetch
// and Exists cannot observe.
//
// If desc cannot be verified against, or the staging area cannot be set up,
// src is returned as is: caching is an optimization and must never fail a
// fetch.
func newCacheReader(ctx context.Context, src io.ReadCloser, desc ocispec.Descriptor, cache content.Storage) io.ReadCloser {
	if err := desc.Digest.Validate(); err != nil || desc.Size < 0 {
		return src
	}
	stage, err := newStaging(desc.Size)
	if err != nil {
		return src
	}
	return &cacheReader{
		ctx:      ctx,
		src:      src,
		desc:     desc,
		cache:    cache,
		stage:    stage,
		verifier: desc.Digest.Verifier(),
	}
}

// cacheReader reads content from a source and stages it for caching.
//
// The read budget is the size of the expected descriptor: the reader never
// delivers more than Size bytes and reads at most one byte beyond it, to detect
// trailing content. The staged bytes are committed only by Close, after the
// size and the digest have both been verified. A read error, a cancellation, a
// truncated source, a digest mismatch, trailing content, or a Close before the
// whole content was read discards the staging and closes the source. Nothing
// is ever committed unless the content is complete and matches desc.
type cacheReader struct {
	ctx      context.Context
	src      io.ReadCloser
	desc     ocispec.Descriptor
	cache    content.Storage
	stage    staging
	verifier digest.Verifier

	// read is the number of bytes read from src and delivered to the caller.
	read int64
	// eof is true once src was observed to end exactly at the read budget.
	eof bool
	// verified is true once the content at the read budget has passed digest
	// verification.
	verified bool
	// err is the first fatal error, sticky for the rest of the reader's life.
	err error

	srcClosed   bool
	stageClosed bool
	closed      bool
	closeErr    error
}

// Read reads content from the source, hashing and staging it on the way.
//
// Once the end of the content is observed, Read verifies the digest before
// reporting io.EOF, so a caller that reads to the end learns about mismatched
// content from Read as well as from Close.
func (r *cacheReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, errReaderClosed
	}
	if r.err != nil {
		return 0, r.err
	}
	if r.eof {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.ctx.Err(); err != nil {
		r.abort(err)
		return 0, err
	}

	remaining := r.desc.Size - r.read
	if remaining == 0 {
		// The budget is exhausted. Read at most one more byte to tell a
		// complete content from one with trailing data, and stop immediately
		// if that byte exists.
		if err := r.probe(); err != nil {
			r.abort(err)
			return 0, err
		}
		return 0, r.complete()
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	n, err := r.src.Read(p)
	if n > 0 {
		_, _ = r.verifier.Write(p[:n]) // the hash never fails
		if _, writeErr := r.stage.Write(p[:n]); writeErr != nil {
			writeErr = fmt.Errorf("failed to stage content %s: %w", r.desc.Digest, writeErr)
			r.abort(writeErr)
			return n, writeErr
		}
		r.read += int64(n)
	}
	switch {
	case err == nil:
		return n, nil
	case err == io.EOF:
		// The source ended. It must have met the read budget exactly.
		if r.read < r.desc.Size {
			return n, r.truncated()
		}
		r.eof = true
		if finErr := r.complete(); finErr != nil {
			return n, finErr
		}
		return n, io.EOF
	default:
		err = fmt.Errorf("failed to read content %s: %w", r.desc.Digest, err)
		r.abort(err)
		return n, err
	}
}

// probe reads at most one byte from the source after the read budget has been
// exhausted. A byte means trailing data; EOF marks the content complete.
func (r *cacheReader) probe() error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	var b [1]byte
	n, err := io.ReadFull(r.src, b[:])
	if n > 0 {
		return fmt.Errorf("%s: %s: %w", r.desc.Digest, r.desc.MediaType, content.ErrTrailingData)
	}
	if err == io.EOF {
		r.eof = true
		return nil
	}
	return fmt.Errorf("failed to read content %s: %w", r.desc.Digest, err)
}

// complete verifies the content once the source is known to have ended at the
// read budget, and returns io.EOF on success or the verification error.
func (r *cacheReader) complete() error {
	if err := r.verify(); err != nil {
		r.abort(err)
		return err
	}
	return io.EOF
}

// verify checks the content against the descriptor's digest. It must only be
// called once probe has established that the content is complete.
func (r *cacheReader) verify() error {
	if r.verified {
		return nil
	}
	if !r.verifier.Verified() {
		return fmt.Errorf("%s: %s: %w", r.desc.Digest, r.desc.MediaType, content.ErrMismatchedDigest)
	}
	r.verified = true
	return nil
}

// truncated records that the source ended before the expected size and
// discards the staging.
func (r *cacheReader) truncated() error {
	err := fmt.Errorf("%s: %s: %w", r.desc.Digest, r.desc.MediaType, io.ErrUnexpectedEOF)
	r.abort(err)
	return err
}

// abort records err and releases the source and the staging. The cache is
// left untouched: the staged content is not visible to any reader.
func (r *cacheReader) abort(err error) {
	if r.err == nil {
		r.err = err
	}
	_ = r.closeSource()
	_ = r.closeStage()
}

// Close verifies the content read so far and, if it is complete and matches
// the descriptor, commits the staged copy to the cache. Close is idempotent:
// repeated calls return the result of the first one.
func (r *cacheReader) Close() error {
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	r.closeErr = r.close()
	return r.closeErr
}

func (r *cacheReader) close() error {
	if r.err != nil {
		// A read already discarded the staging and closed the source.
		return r.err
	}

	// The caller may have consumed the whole content without observing EOF.
	if r.read < r.desc.Size {
		return r.truncated()
	}
	if !r.eof {
		if err := r.probe(); err != nil {
			r.abort(err)
			return err
		}
	}
	if err := r.verify(); err != nil {
		r.abort(err)
		return err
	}

	// The content is complete; release the source before committing.
	srcErr := r.closeSource()
	if err := r.ctx.Err(); err != nil {
		r.err = err
		_ = r.closeStage()
		return err
	}
	if err := r.commit(); err != nil {
		r.err = err
		_ = r.closeStage()
		return err
	}
	if err := r.closeStage(); err != nil && srcErr == nil {
		srcErr = err
	}
	return srcErr
}

// commit pushes the verified content to the cache. An entry of the same digest
// that another reader committed first makes the commit a no-op, so committing
// the same content more than once is idempotent.
func (r *cacheReader) commit() error {
	if err := r.cache.Push(r.ctx, r.desc, r.stage.reader()); err != nil {
		if errors.Is(err, errdef.ErrAlreadyExists) {
			return nil
		}
		return fmt.Errorf("failed to cache content %s: %w", r.desc.Digest, err)
	}
	return nil
}

func (r *cacheReader) closeSource() error {
	if r.srcClosed {
		return nil
	}
	r.srcClosed = true
	return r.src.Close()
}

func (r *cacheReader) closeStage() error {
	if r.stageClosed {
		return nil
	}
	r.stageClosed = true
	return r.stage.close()
}

// staging holds content under a temporary identity until it is verified and
// committed to the cache.
type staging interface {
	io.Writer
	// reader returns a reader over all the bytes written so far.
	reader() io.Reader
	// close releases the staging. A closed staging must not be written to.
	close() error
}

// newStaging creates a staging area for content of the given size. Small
// content is kept in memory; larger content is written to a temporary file so
// that a large blob does not have to fit in memory.
func newStaging(size int64) (staging, error) {
	if size <= stagingMemoryLimit {
		return &memoryStaging{}, nil
	}
	return newFileStaging()
}

// memoryStaging stages content in memory.
type memoryStaging struct {
	buf bytes.Buffer
}

func (s *memoryStaging) Write(p []byte) (int, error) {
	return s.buf.Write(p)
}

func (s *memoryStaging) reader() io.Reader {
	return bytes.NewReader(s.buf.Bytes())
}

func (s *memoryStaging) close() error {
	s.buf = bytes.Buffer{}
	return nil
}

// fileStaging stages content in a temporary file.
type fileStaging struct {
	file *os.File
	name string
	size int64
	// unlinked is true when the name was removed while the file was open, so
	// that closing the file is all the cleanup that remains.
	unlinked bool
}

// newFileStaging creates a temporary file for staged content. The file is
// removed from the directory immediately on systems that allow unlinking an
// open file, so that content staged by a process that exits abruptly is not
// left behind. On systems that refuse, the file is removed by close.
func newFileStaging() (staging, error) {
	f, err := os.CreateTemp("", "oras-cache-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create staging file: %w", err)
	}
	s := &fileStaging{file: f, name: f.Name()}
	s.unlinked = os.Remove(s.name) == nil
	return s, nil
}

func (s *fileStaging) Write(p []byte) (int, error) {
	n, err := s.file.Write(p)
	s.size += int64(n)
	return n, err
}

func (s *fileStaging) reader() io.Reader {
	return io.NewSectionReader(s.file, 0, s.size)
}

func (s *fileStaging) close() error {
	err := s.file.Close()
	if !s.unlinked {
		if removeErr := os.Remove(s.name); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}
	return err
}
