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

package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/errdef"
	"github.com/oras-project/oras-go/v3/internal/descriptor"
	"github.com/oras-project/oras-go/v3/registry/remote/errcode"
)

// zeroDigest represents a digest that consists of zeros. zeroDigest is used
// for pinging Referrers API.
const zeroDigest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

// referrerOperation represents an operation on a referrer.
type referrerOperation = int32

const (
	// referrerOperationAdd represents an addition operation on a referrer.
	referrerOperationAdd referrerOperation = iota
	// referrerOperationRemove represents a removal operation on a referrer.
	referrerOperationRemove
)

// referrerChange represents a change on a referrer.
type referrerChange struct {
	referrer  ocispec.Descriptor
	operation referrerOperation
}

// referrersIndexState is a snapshot of a referrers index observed at a point in
// time, used as the baseline of a referrers index transaction.
type referrersIndexState struct {
	// desc is the descriptor of the index the snapshot was read from.
	// It is empty if no index exists yet.
	desc ocispec.Descriptor
	// exists reports whether an index exists.
	exists bool
	// validator is the entity tag of the index, if the registry supplied one.
	// It is the exact validator a conditional push must carry, since an opaque
	// entity tag need not equal the descriptor digest.
	validator string
	// referrers is the list of referrers in the index.
	referrers []ocispec.Descriptor
}

// referrersIndexTransaction applies a batch of referrer changes to a referrers
// index against a previously read baseline and publishes the result.
//
// The tag-schema index is a shared read-modify-write target: the registry does
// not serialize writers, so two clients can read the same list and each publish
// a version containing only its own addition, silently losing the other. Each
// transaction therefore carries the identity of the baseline it was computed
// from into the publish as an HTTP precondition. A registry that enforces ETag
// conditions rejects the publish with 412 when the index changed since the
// baseline read, and the transaction re-reads, merges only this client's
// changes on top, and tries again.
// Reference: https://github.com/opencontainers/distribution-spec/blob/v1.1.1/spec.md#referrers-tag-schema
type referrersIndexTransaction struct {
	// tag is the referrers tag holding the index.
	tag string
	// subject is the descriptor of the artifact the referrers index is for.
	subject ocispec.Descriptor
	// state is the baseline read by referrersIndexRead.
	state referrersIndexState
	// skipGC disables deletion of a superseded index.
	skipGC bool
}

// referrersIndexPrecondition is the HTTP precondition that ties a push to the
// baseline the change was computed from.
type referrersIndexPrecondition struct {
	// ifMatch is sent as If-Match when non-empty.
	ifMatch string
	// ifNoneMatch is sent as If-None-Match when non-empty.
	ifNoneMatch string
}

// precondition returns the precondition that fails if the index is not the
// baseline read by the transaction.
//
// An existing baseline is matched on its entity tag, or on the quoted digest
// when the registry supplied no entity tag. A missing baseline is matched with
// If-None-Match: *, so creating the index fails once another client created it.
func (t *referrersIndexTransaction) precondition() referrersIndexPrecondition {
	if !t.state.exists {
		return referrersIndexPrecondition{ifNoneMatch: "*"}
	}
	if t.state.validator != "" {
		return referrersIndexPrecondition{ifMatch: t.state.validator}
	}
	return referrersIndexPrecondition{ifMatch: `"` + t.state.desc.Digest.String() + `"`}
}

// referrersIndexRead reads the current referrers index for t.subject, recording
// it as the baseline of t. A missing index is a valid and empty baseline.
func (r *Repository) referrersIndexRead(ctx context.Context, t *referrersIndexTransaction) error {
	desc, referrers, validator, err := r.referrersFromIndex(ctx, t.tag, t.subject)
	if err != nil {
		if errors.Is(err, errdef.ErrNotFound) {
			t.state = referrersIndexState{}
			return nil
		}
		return err
	}
	t.state = referrersIndexState{
		desc:      desc,
		exists:    true,
		validator: validator,
		referrers: referrers,
	}
	return nil
}

// apply applies changes to the baseline read by [Repository.referrersIndexRead]
// and returns the referrers the index should contain.
// It returns errNoReferrerUpdate if the index would not change.
func (t *referrersIndexTransaction) apply(changes []referrerChange) ([]ocispec.Descriptor, error) {
	return applyReferrerChanges(t.state.referrers, changes)
}

// commit publishes updatedReferrers as the referrers index for t.subject and
// reports whether the publish is confirmed.
//
// The publish is conditional on the baseline read by referrersIndexRead. If the
// registry rejects it because the index moved on (412), commit returns false and
// no error, and the caller re-reads and retries. A registry that ignores
// preconditions accepts the publish; commit cannot tell that apart from a
// successful conditional publish, so such registries keep the tag schema's
// inherent best-effort semantics.
//
// An empty list is not published unless referrers GC is skipped: the superseded
// index is then deleted instead, leaving the tag without an index for the list
// to be reconstructed from. When referrers GC is skipped the empty index is
// published and the old index is kept.
//
// The updated index blob is pushed under its digest before its tag is written.
// If the tag write fails, neither the index blob nor the referrer manifest that
// was pushed before the index update is deleted, so the whole update is safe to
// retry: the manifest is stored by digest and re-pushing it converges, and
// re-adding a referrer already present is a no-op unless its metadata changed.
func (s *manifestStore) commitReferrersIndex(ctx context.Context, t *referrersIndexTransaction, updatedReferrers []ocispec.Descriptor) (bool, error) {
	if len(updatedReferrers) > 0 || t.skipGC {
		// Publish a new index in either case:
		// 1. the referrers list has been updated with a non-zero size, or
		// 2. the updated referrers list is empty but referrers GC is skipped,
		//    in which case an empty index should still be published as the old
		//    index will not be deleted.
		newIndexDesc, newIndex, err := generateIndex(updatedReferrers, t.subject.Digest.Algorithm())
		if err != nil {
			return false, fmt.Errorf("failed to generate referrers index for referrers tag %s: %w", t.tag, err)
		}

		// Conditional pushes are optional in the tag schema. A registry that
		// rejected one before is not asked again; every later update falls back
		// to the unconditional, best-effort publish documented by the spec.
		var precond referrersIndexPrecondition
		if !s.repo.referrersPreconditionUnsupported.Load() {
			precond = t.precondition()
		}
		err = s.pushConditional(ctx, newIndexDesc, bytes.NewReader(newIndex), t.tag, precond)
		if err != nil && precond != (referrersIndexPrecondition{}) && isPreconditionUnsupported(err) {
			// The registry does not understand the conditional header it was
			// sent. Remember that and publish without it, so the referrers
			// index is still updated on registries without ETag conditions.
			s.repo.referrersPreconditionUnsupported.Store(true)
			err = s.push(ctx, newIndexDesc, bytes.NewReader(newIndex), t.tag)
		}
		if err != nil {
			if isPreconditionFailed(err) {
				// The index changed since the baseline read: another client
				// published between our read and our write. Report a conflict
				// so the caller re-reads and merges on top of their result.
				return false, nil
			}
			return false, fmt.Errorf("failed to push referrers index tagged by %s: %w", t.tag, err)
		}
	}

	// The publish is confirmed, or an empty list supersedes the index. Deleting
	// the superseded index is best effort relative to the update, but reported:
	// it does not affect the index content, only storage.
	if !t.skipGC && t.state.exists {
		if err := s.repo.delete(ctx, t.state.desc, true); err != nil {
			return true, &ReferrersError{
				Op:      opDeleteReferrersIndex,
				Err:     fmt.Errorf("failed to delete dangling referrers index %s for referrers tag %s: %w", t.state.desc.Digest.String(), t.tag, err),
				Subject: t.subject,
			}
		}
	}
	return true, nil
}

// isPreconditionFailed reports whether err is a registry rejection of a
// conditional write, i.e. the index changed since the baseline was read.
func isPreconditionFailed(err error) bool {
	var respErr *errcode.ErrorResponse
	return errors.As(err, &respErr) && respErr.StatusCode == http.StatusPreconditionFailed
}

// isPreconditionUnsupported reports whether err is a registry rejecting the
// conditional header itself rather than the condition. The tag schema makes
// conditional pushes optional, so such a registry is retried unconditionally.
func isPreconditionUnsupported(err error) bool {
	var respErr *errcode.ErrorResponse
	if !errors.As(err, &respErr) {
		return false
	}
	switch respErr.StatusCode {
	case http.StatusBadRequest, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

var (
	// errNoReferrerUpdate is returned by applyReferrerChanges() when there
	// is no any referrer update.
	errNoReferrerUpdate = errors.New("no referrer update")

	// errReferrersIndexConflict reports that a referrers index transaction ran
	// out of retries because other clients kept changing the index.
	errReferrersIndexConflict = errors.New("concurrent referrers index update")
)

const (
	// maxReferrersIndexAttempts bounds how many times a referrers index
	// transaction is attempted before errReferrersIndexConflict is returned.
	maxReferrersIndexAttempts = 8
)

const (
	// opDeleteReferrersIndex represents the operation for deleting a
	// referrers index.
	opDeleteReferrersIndex = "DeleteReferrersIndex"
)

// ReferrersError records an error and the operation and the subject descriptor.
type ReferrersError struct {
	// Op represents the failing operation.
	Op string
	// Subject is the descriptor of referenced artifact.
	Subject ocispec.Descriptor
	// Err is the entity of referrers error.
	Err error
}

// Error returns error msg of IgnorableError.
func (e *ReferrersError) Error() string {
	return e.Err.Error()
}

// Unwrap returns the inner error of IgnorableError.
func (e *ReferrersError) Unwrap() error {
	return errors.Unwrap(e.Err)
}

// IsIndexDelete tells if e is kind of error related to referrers
// index deletion.
func (e *ReferrersError) IsReferrersIndexDelete() bool {
	return e.Op == opDeleteReferrersIndex
}

// buildReferrersTag builds the referrers tag for the given manifest descriptor.
// Format: <algorithm>-<digest>, where <digest> is limited to 64 characters.
// Reference: https://github.com/opencontainers/distribution-spec/blob/v1.1.1/spec.md#unavailable-referrers-api
func buildReferrersTag(desc ocispec.Descriptor) (string, error) {
	if err := desc.Digest.Validate(); err != nil {
		return "", fmt.Errorf("failed to build referrers tag for %s: %w", desc.Digest, err)
	}
	alg := desc.Digest.Algorithm().String()
	encoded := desc.Digest.Encoded()
	if len(encoded) > 64 {
		encoded = encoded[:64]
	}
	return alg + "-" + encoded, nil
}

// isImageIndexContentType checks if the Content-Type header value is an OCI
// image index. The letter case and the media type parameters are ignored.
func isImageIndexContentType(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && mediaType == ocispec.MediaTypeImageIndex
}

// isReferrersFilterApplied checks if requsted is in the applied filter list.
func isReferrersFilterApplied(applied, requested string) bool {
	if applied == "" || requested == "" {
		return false
	}
	filters := strings.SplitSeq(applied, ",")
	for f := range filters {
		if f == requested {
			return true
		}
	}
	return false
}

// filterReferrers filters a slice of referrers by artifactType in place.
// The returned slice contains matching referrers.
func filterReferrers(refs []ocispec.Descriptor, artifactType string) []ocispec.Descriptor {
	if artifactType == "" {
		return refs
	}
	var j int
	for i, ref := range refs {
		if ref.ArtifactType == artifactType {
			if i != j {
				refs[j] = ref
			}
			j++
		}
	}
	return refs[:j]
}

// applyReferrerChanges applies referrerChanges on referrers and returns the
// updated referrers.
// Returns errNoReferrerUpdate if there is no any referrers updates.
func applyReferrerChanges(referrers []ocispec.Descriptor, referrerChanges []referrerChange) ([]ocispec.Descriptor, error) {
	referrersMap := make(map[descriptor.Descriptor]int, len(referrers)+len(referrerChanges))
	updatedReferrers := make([]ocispec.Descriptor, 0, len(referrers)+len(referrerChanges))
	var updateRequired bool
	for _, r := range referrers {
		if content.Equal(r, ocispec.Descriptor{}) {
			// skip bad entry
			updateRequired = true
			continue
		}
		key := descriptor.FromOCI(r)
		if _, ok := referrersMap[key]; ok {
			// skip duplicates
			updateRequired = true
			continue
		}
		updatedReferrers = append(updatedReferrers, r)
		referrersMap[key] = len(updatedReferrers) - 1
	}

	// apply changes
	for _, change := range referrerChanges {
		key := descriptor.FromOCI(change.referrer)
		switch change.operation {
		case referrerOperationAdd:
			if pos, ok := referrersMap[key]; ok {
				// A descriptor is identified by media type, digest and size, so
				// an existing entry with a different artifactType or different
				// annotations is stale. Refresh it from the pushed manifest so
				// the index stays consistent with what the referrer declares.
				if !sameReferrerMetadata(updatedReferrers[pos], change.referrer) {
					updatedReferrers[pos] = change.referrer
					updateRequired = true
				}
				continue
			}
			// add distinct referrers
			updatedReferrers = append(updatedReferrers, change.referrer)
			referrersMap[key] = len(updatedReferrers) - 1
		case referrerOperationRemove:
			if pos, ok := referrersMap[key]; ok {
				// remove referrers that are already in the map
				updatedReferrers[pos] = ocispec.Descriptor{}
				delete(referrersMap, key)
			}
		}
	}

	// skip unnecessary update
	if !updateRequired && len(referrersMap) == len(referrers) {
		// if the result referrer map contains the same content as the
		// original referrers, consider that there is no update on the
		// referrers.
		for _, r := range referrers {
			key := descriptor.FromOCI(r)
			if _, ok := referrersMap[key]; !ok {
				updateRequired = true
			}
		}
		if !updateRequired {
			return nil, errNoReferrerUpdate
		}
	}

	return removeEmptyDescriptors(updatedReferrers, len(referrersMap)), nil
}

// sameReferrerMetadata reports whether a and b carry the same artifactType and
// annotations, the fields that are not part of a descriptor's identity key.
func sameReferrerMetadata(a, b ocispec.Descriptor) bool {
	if a.ArtifactType != b.ArtifactType || len(a.Annotations) != len(b.Annotations) {
		return false
	}
	for k, av := range a.Annotations {
		if bv, ok := b.Annotations[k]; !ok || bv != av {
			return false
		}
	}
	return true
}

// removeEmptyDescriptors in-place removes empty items from descs, given a hint
// of the number of non-empty descriptors.
func removeEmptyDescriptors(descs []ocispec.Descriptor, hint int) []ocispec.Descriptor {
	j := 0
	for i, r := range descs {
		if !content.Equal(r, ocispec.Descriptor{}) {
			if i > j {
				descs[j] = r
			}
			j++
		}
		if j == hint {
			break
		}
	}
	return descs[:j]
}
