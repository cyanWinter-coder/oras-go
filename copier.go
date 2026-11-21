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
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/internal/syncutil"
	"github.com/oras-project/oras-go/v3/registry/remote/errcode"
)

// ErrStaleGeneration is returned by [Copier.Copy] when the source changed
// generation while a copy was in flight, so the copy result was discarded
// instead of being committed over the newer source state. The error is
// retryable: a later [Copier.Copy] starts from the current generation.
var ErrStaleGeneration = errors.New("stale source generation")

// DefaultCopierOptions provides the default CopierOptions.
// It is the zero value, provided for convenience. Do not modify it: the
// change would apply to every caller in the process that passes it.
var DefaultCopierOptions CopierOptions = CopierOptions{
	CopyGraphOptions: DefaultCopyGraphOptions,
}

// CopierOptions contains parameters for [Copier.Copy].
type CopierOptions struct {
	CopyGraphOptions
}

// copierMaxAttempts bounds how many times Copier.Copy restarts after the
// source changed generation mid-copy. Each restart re-resolves the source
// reference in the current generation, and content already present in the
// destination is not uploaded again, so restarts are cheap.
const copierMaxAttempts = 3

// Copier copies artifacts from a source target to a destination target,
// coordinating concurrent copies so that the same content is probed and
// uploaded only once even when many goroutines copy at the same time.
//
// Concurrent Copy calls that resolve to the same digest share a single
// in-flight copy: one call probes the destination and uploads the content
// while the others wait for its result. A waiting caller is cancelled
// independently of the shared copy: its Copy returns the context error, but
// the shared copy keeps running for the remaining callers.
//
// If RefreshSource is set and a copy fails because the source rejected the
// authentication, the refresh runs once across all concurrent callers and
// the failed copies are retried once. The error of a failed copy keeps its
// recognizable form: it unwraps to the *CopyError classifying the failed
// operation and to the registry's errcode.ErrorResponse, including the
// status code and the RetryAfter wait hint.
//
// Every refresh opens a new source generation. A copy result is committed
// only if it comes from the current generation: before publishing, the
// source reference is resolved again and the copy is published only if the
// digest still matches. The destination is tagged (updating its index, for
// targets that keep one) only after the content is fully copied, so a failed
// copy leaves the previous tags and index in place, and a successful copy is
// not uploaded again by a later one.
//
// The zero value is not usable: Source and Destination are required.
type Copier struct {
	// Source is the target the artifacts are copied from. It is required.
	Source ReadOnlyTarget

	// Destination is the target the artifacts are copied to. It is required.
	Destination Target

	// RefreshSource renews the source authentication, for example by
	// discarding cached credentials so that the next request
	// re-authenticates. It is called at most once for a burst of concurrent
	// authentication failures; concurrent callers share its result.
	// If nil, an authentication failure is returned without a retry.
	RefreshSource func(ctx context.Context) error

	// copies shares one in-flight copy per destination digest.
	copies syncutil.Flight[string, ocispec.Descriptor]
	// refresh shares one in-flight authentication refresh per failure
	// generation, so a failure that a completed refresh did not cover still
	// triggers its own refresh.
	refresh syncutil.Flight[uint64, struct{}]
	// genLock serializes source refreshes against publishes: a refresh takes
	// the write lock and a publish the read lock, so no refresh can slip
	// between the generation check and the tag.
	genLock sync.RWMutex
	// generation is the current source generation, advanced by every
	// successful refresh. Guarded by genLock.
	generation uint64
}

// Copy copies the artifact identified by srcRef from Source to Destination,
// tagging it as dstRef. If dstRef is empty, srcRef is used.
//
// Returns the descriptor of the copied root node on success.
func (c *Copier) Copy(ctx context.Context, srcRef, dstRef string, opts CopierOptions) (ocispec.Descriptor, error) {
	if c.Source == nil {
		return ocispec.Descriptor{}, newCopyError("Copy", CopyErrorOriginSource, ocispec.Descriptor{}, errors.New("nil source target"))
	}
	if c.Destination == nil {
		return ocispec.Descriptor{}, newCopyError("Copy", CopyErrorOriginDestination, ocispec.Descriptor{}, errors.New("nil destination target"))
	}
	if dstRef == "" {
		dstRef = srcRef
	}

	var lastErr error
	for attempt := 0; attempt < copierMaxAttempts; attempt++ {
		desc, err := c.copyAttempt(ctx, srcRef, dstRef, opts)
		if err == nil {
			return desc, nil
		}
		if !errors.Is(err, ErrStaleGeneration) {
			return ocispec.Descriptor{}, err
		}
		// The source moved to a new generation mid-copy: restart in the
		// current generation.
		lastErr = err
	}
	return ocispec.Descriptor{}, lastErr
}

// copyAttempt runs a single attempt of Copy in the current source
// generation.
func (c *Copier) copyAttempt(ctx context.Context, srcRef, dstRef string, opts CopierOptions) (ocispec.Descriptor, error) {
	// Resolve the source reference to learn the digest to copy. This is the
	// only per-call probe: concurrent calls that resolve to the same digest
	// share everything after it.
	root, err := c.resolve(ctx, srcRef)
	if err != nil {
		return ocispec.Descriptor{}, err
	}

	// Share one in-flight copy per destination digest. Subscribers wait with
	// their own contexts and can leave independently.
	generation := c.currentGeneration()
	desc, err := c.copies.Do(ctx, root.Digest.String(), func(ctx context.Context) (ocispec.Descriptor, error) {
		return c.copyContent(ctx, root, generation, opts)
	})
	if err != nil {
		return ocispec.Descriptor{}, err
	}

	// Publish: tag the destination only if the copied digest is still what
	// the source reference resolves to in the current source generation.
	if err := c.publish(ctx, root, srcRef, dstRef, generation); err != nil {
		return ocispec.Descriptor{}, err
	}
	return desc, nil
}

// resolve resolves srcRef, refreshing the source authentication once if the
// resolution is rejected for authentication reasons.
func (c *Copier) resolve(ctx context.Context, srcRef string) (ocispec.Descriptor, error) {
	failedAt := c.currentGeneration()
	desc, err := c.Source.Resolve(ctx, srcRef)
	if err != nil && c.RefreshSource != nil && isSourceAuthError(err) {
		if refreshErr := c.refreshSource(ctx, failedAt); refreshErr != nil {
			return ocispec.Descriptor{}, refreshErr
		}
		desc, err = c.Source.Resolve(ctx, srcRef)
	}
	return desc, newCopyError("Resolve", CopyErrorOriginSource, ocispec.Descriptor{}, err)
}

// copyContent copies the content of root from the source to the destination,
// refreshing the source authentication once if the copy is rejected for
// authentication reasons. The copied content is committed only if the source
// is still in the generation the copy started in.
func (c *Copier) copyContent(ctx context.Context, root ocispec.Descriptor, generation uint64, opts CopierOptions) (ocispec.Descriptor, error) {
	err := CopyGraph(ctx, c.Source, c.Destination, root, opts.CopyGraphOptions)
	if err != nil && c.RefreshSource != nil && isSourceAuthError(err) {
		if refreshErr := c.refreshSource(ctx, generation); refreshErr != nil {
			return ocispec.Descriptor{}, refreshErr
		}
		// The retry belongs to the new generation.
		generation = c.currentGeneration()
		err = CopyGraph(ctx, c.Source, c.Destination, root, opts.CopyGraphOptions)
	}
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	if c.currentGeneration() != generation {
		// The source moved to a new generation while the copy was in
		// flight; discard the result instead of committing it over the
		// newer state.
		return ocispec.Descriptor{}, fmt.Errorf("%w: copy of %s discarded", ErrStaleGeneration, root.Digest)
	}
	return root, nil
}

// publish tags the destination for root after re-validating that the source
// reference still resolves to root in the current source generation.
func (c *Copier) publish(ctx context.Context, root ocispec.Descriptor, srcRef, dstRef string, generation uint64) error {
	// Hold the read lock from the generation check to the tag, so that no
	// source refresh can slip in between and make the copied result stale.
	c.genLock.RLock()
	defer c.genLock.RUnlock()

	if c.generation != generation {
		return fmt.Errorf("%w: copy of %s discarded before tagging", ErrStaleGeneration, root.Digest)
	}
	latest, err := c.Source.Resolve(ctx, srcRef)
	if err != nil {
		return newCopyError("Resolve", CopyErrorOriginSource, root, err)
	}
	if !content.Equal(latest, root) {
		return fmt.Errorf("%w: %s moved from %s to %s", ErrStaleGeneration, srcRef, root.Digest, latest.Digest)
	}
	if err := c.Destination.Tag(ctx, root, dstRef); err != nil {
		return newCopyError("Tag", CopyErrorOriginDestination, root, err)
	}
	return nil
}

// refreshSource refreshes the source authentication and opens a new source
// generation. Concurrent callers share one in-flight refresh, and a caller
// whose failure was already covered by a completed refresh — that is, the
// source generation advanced past failedAt since the failure was observed —
// does not refresh again.
func (c *Copier) refreshSource(ctx context.Context, failedAt uint64) error {
	_, err := c.refresh.Do(ctx, failedAt, func(ctx context.Context) (struct{}, error) {
		c.genLock.Lock()
		defer c.genLock.Unlock()
		if c.generation != failedAt {
			// another caller already refreshed after the failure was
			// observed; the failure belongs to a previous generation
			return struct{}{}, nil
		}
		if err := c.RefreshSource(ctx); err != nil {
			return struct{}{}, err
		}
		// A successful refresh opens a new source generation: results
		// produced before it must not be committed afterwards.
		c.generation++
		return struct{}{}, nil
	})
	if err != nil {
		return newCopyError("RefreshSource", CopyErrorOriginSource, ocispec.Descriptor{}, err)
	}
	return nil
}

// currentGeneration returns the current source generation.
func (c *Copier) currentGeneration() uint64 {
	c.genLock.RLock()
	defer c.genLock.RUnlock()
	return c.generation
}

// isSourceAuthError reports whether err is a source-side authentication
// failure, that is, a 401 UNAUTHORIZED response from the source registry.
// The classification is read off the error chain, so the error keeps its
// recognizable form for the caller: a CopyError classifying the failed
// operation, wrapping the errcode.ErrorResponse of the registry.
func isSourceAuthError(err error) bool {
	var copyErr *CopyError
	if errors.As(err, &copyErr) && copyErr.Origin != CopyErrorOriginSource {
		// a destination-side failure is not a source authentication failure
		return false
	}
	var errResp *errcode.ErrorResponse
	if errors.As(err, &errResp) && errResp.StatusCode == http.StatusUnauthorized {
		return true
	}
	// A single inner error, e.g. a bare UNAUTHORIZED.
	var ec errcode.Error
	if errors.As(err, &ec) && ec.Code == errcode.ErrorCodeUnauthorized {
		return true
	}
	// Multiple inner errors. errcode.Errors only unwraps to a single
	// errcode.Error when it holds exactly one element, so the multi-error
	// case must be inspected explicitly.
	var ecs errcode.Errors
	if errors.As(err, &ecs) {
		for _, e := range ecs {
			if e.Code == errcode.ErrorCodeUnauthorized {
				return true
			}
		}
	}
	return false
}
