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

// Package cache provides a cache layer for content stores.
package cache

import (
	"context"
	"io"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/registry"
)

// target wraps a ReadOnlyTarget with a caching layer.
type target struct {
	oras.ReadOnlyTarget
	cache content.Storage
}

// CacheReadOnlyTarget creates a new cached target.
// The returned target will first check the cache for content before
// fetching from the source. Fetched content is cached while being read.
//
// The cache only ever holds complete, verified content. On a cache miss, the
// content read from the source is staged under a temporary identity that
// ordinary reads cannot observe. The staged copy becomes the cache entry of
// its digest only after the whole content has been read and verified against
// the descriptor. Cancellation, a read error, a digest mismatch, a size
// mismatch, or closing the reader before the content is complete discards the
// staging, closes the source reader, and leaves the cache untouched. A
// completed commit is idempotent: content that is already cached is not
// rewritten, so concurrent readers of the same content can all commit safely.
//
// The returned target keeps the following optional interfaces of the source,
// so that type assertions for them succeed exactly when they succeed on the
// source:
//   - [registry.ReferenceFetcher]: fetched content is served from and stored
//     in the cache.
//   - [registry.ReferenceParser]: passed through to the source.
//   - [registry.ReferrerLister]: passed through to the source. Referrers are
//     always listed from the source and are never cached.
//
// Other interfaces implemented by the source are not available through the
// returned target.
func CacheReadOnlyTarget(source oras.ReadOnlyTarget, cache content.Storage) oras.ReadOnlyTarget {
	t := &target{
		ReadOnlyTarget: source,
		cache:          cache,
	}
	parser, isParser := source.(registry.ReferenceParser)
	lister, isLister := source.(registry.ReferrerLister)
	refFetcher, isRefFetcher := source.(registry.ReferenceFetcher)
	if !isRefFetcher {
		switch {
		case isParser && isLister:
			return &parserListerTarget{target: t, ReferenceParser: parser, ReferrerLister: lister}
		case isParser:
			return &parserTarget{target: t, ReferenceParser: parser}
		case isLister:
			return &listerTarget{target: t, ReferrerLister: lister}
		default:
			return t
		}
	}

	rt := &referenceTarget{
		target:           t,
		ReferenceFetcher: refFetcher,
	}
	switch {
	case isParser && isLister:
		return &referenceParserListerTarget{referenceTarget: rt, ReferenceParser: parser, ReferrerLister: lister}
	case isParser:
		return &referenceParserTarget{referenceTarget: rt, ReferenceParser: parser}
	case isLister:
		return &referenceListerTarget{referenceTarget: rt, ReferrerLister: lister}
	default:
		return rt
	}
}

// Fetch fetches the content identified by the descriptor.
// It first checks the cache, and if not found, fetches from the source
// while caching the content.
//
// The content is committed to the cache only when the whole content has been
// read and verified against desc and the reader is closed. Until then the
// cached copy is not visible: a concurrent Fetch either hits a previously
// committed copy or fetches from the source again. If the content does not
// match desc, or the reader is closed before the content is complete, nothing
// is cached. A read error is returned by the Read that encountered it, and a
// verification failure, or a Close before the content is complete, is returned
// by Close. In both cases the source reader is closed.
func (t *target) Fetch(ctx context.Context, desc ocispec.Descriptor) (io.ReadCloser, error) {
	rc, err := t.cache.Fetch(ctx, desc)
	if err == nil {
		return rc, nil
	}

	rc, err = t.ReadOnlyTarget.Fetch(ctx, desc)
	if err != nil {
		return nil, err
	}

	return newCacheReader(ctx, rc, desc, t.cache), nil
}

// Exists returns true if the described content exists.
// It checks both the cache and the source.
func (t *target) Exists(ctx context.Context, desc ocispec.Descriptor) (bool, error) {
	exists, err := t.cache.Exists(ctx, desc)
	if err == nil && exists {
		return true, nil
	}
	return t.ReadOnlyTarget.Exists(ctx, desc)
}

// referenceTarget extends target with ReferenceFetcher support.
type referenceTarget struct {
	*target
	registry.ReferenceFetcher
}

// FetchReference fetches the content identified by the reference.
// It first resolves the reference to a descriptor via a lightweight HEAD
// request, then serves from the local cache if available. On a cache miss,
// the content is fetched from the source and cached while being read, on the
// same terms as [target.Fetch].
//
// Trade-off: a cache miss requires one extra HEAD request (Resolve) compared
// to a direct FetchReference call. Cache hits avoid downloading any content
// body from the source.
func (t *referenceTarget) FetchReference(ctx context.Context, reference string) (ocispec.Descriptor, io.ReadCloser, error) {
	// Resolve via HEAD to get the descriptor without downloading content.
	desc, err := t.Resolve(ctx, reference)
	if err != nil {
		return ocispec.Descriptor{}, nil, err
	}

	// Cache hit: serve from cache without issuing a GET to the source.
	if rc, err := t.cache.Fetch(ctx, desc); err == nil {
		return desc, rc, nil
	}

	// Cache miss: fetch from source and cache while reading.
	// Use the descriptor returned by FetchReference (not Resolve) to handle
	// the case where the tag was updated between the two calls.
	fetchedDesc, rc, err := t.ReferenceFetcher.FetchReference(ctx, reference)
	if err != nil {
		return ocispec.Descriptor{}, nil, err
	}
	return fetchedDesc, newCacheReader(ctx, rc, fetchedDesc, t.cache), nil
}

// The types below add the optional interfaces that CacheReadOnlyTarget passes
// through unchanged. A method set cannot be built at runtime, so there is one
// type per combination of the source's optional interfaces. This keeps a type
// assertion on the cached target in agreement with the same assertion on the
// source.

// parserTarget is a target whose source implements registry.ReferenceParser.
type parserTarget struct {
	*target
	registry.ReferenceParser
}

// listerTarget is a target whose source implements registry.ReferrerLister.
type listerTarget struct {
	*target
	registry.ReferrerLister
}

// parserListerTarget is a target whose source implements
// registry.ReferenceParser and registry.ReferrerLister.
type parserListerTarget struct {
	*target
	registry.ReferenceParser
	registry.ReferrerLister
}

// referenceParserTarget is a referenceTarget whose source also implements
// registry.ReferenceParser.
type referenceParserTarget struct {
	*referenceTarget
	registry.ReferenceParser
}

// referenceListerTarget is a referenceTarget whose source also implements
// registry.ReferrerLister.
type referenceListerTarget struct {
	*referenceTarget
	registry.ReferrerLister
}

// referenceParserListerTarget is a referenceTarget whose source also
// implements registry.ReferenceParser and registry.ReferrerLister.
type referenceParserListerTarget struct {
	*referenceTarget
	registry.ReferenceParser
	registry.ReferrerLister
}
