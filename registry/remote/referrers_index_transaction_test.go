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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/internal/spec"
	"golang.org/x/sync/errgroup"
)

// referrersIndexRegistry is a minimal in-memory registry implementing the
// manifest endpoints used by the referrers tag schema. It optionally enforces
// If-Match / If-None-Match on manifest PUTs, like a registry that supports ETag
// conditions, and records how often a conditional write was rejected.
type referrersIndexRegistry struct {
	t                        *testing.T
	enforcePreconditions     bool
	rejectConditionalHeaders bool

	server    *httptest.Server
	mu        sync.Mutex
	manifests map[string]referrersIndexManifest
	// afterRead runs after a GET response is written, outside the lock. A test
	// uses it to stage a competing client that publishes between this client's
	// read and its conditional write.
	afterRead func()
	// beforeConditionalCheck runs under the lock immediately before a
	// conditional PUT's precondition is checked. A test uses it to model a
	// competitor that wins the race on every attempt.
	beforeConditionalCheck func()
	// failPuts, when set, makes every manifest PUT fail with a server error,
	// modelling a transient publish failure.
	failPuts          atomic.Bool
	conditionalMisses atomic.Int32
	unconditionalPuts atomic.Int32
}

type referrersIndexManifest struct {
	mediaType string
	body      []byte
}

func newReferrersIndexRegistry(t *testing.T, enforcePreconditions bool) *referrersIndexRegistry {
	return &referrersIndexRegistry{
		t:                    t,
		enforcePreconditions: enforcePreconditions,
		manifests:            make(map[string]referrersIndexManifest),
	}
}

// set stores a manifest under reference without going through HTTP.
func (r *referrersIndexRegistry) set(reference string, mediaType string, body []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.manifests[reference] = referrersIndexManifest{mediaType: mediaType, body: body}
}

// get returns the manifest stored under reference.
func (r *referrersIndexRegistry) get(reference string) (referrersIndexManifest, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.manifests[reference]
	return m, ok
}

// validatorLocked returns the ETag of the manifest at reference and whether it
// exists. The caller must hold r.mu.
func (r *referrersIndexRegistry) validatorLocked(reference string) (etag string, exists bool) {
	m, ok := r.manifests[reference]
	if !ok {
		return "", false
	}
	return `"` + digest.FromBytes(m.body).String() + `"`, true
}

func (r *referrersIndexRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !strings.HasPrefix(req.URL.Path, "/v2/test/manifests/") {
		r.t.Errorf("unexpected access: %s %q", req.Method, req.URL)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	reference := strings.TrimPrefix(req.URL.Path, "/v2/test/manifests/")

	switch req.Method {
	case http.MethodGet:
		r.mu.Lock()
		m, ok := r.manifests[reference]
		afterRead := r.afterRead
		r.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.Header().Set("Content-Type", m.mediaType)
			w.Header().Set("Content-Length", strconv.Itoa(len(m.body)))
			w.Header().Set("Docker-Content-Digest", digest.FromBytes(m.body).String())
			w.Header().Set("ETag", `"`+digest.FromBytes(m.body).String()+`"`)
			w.WriteHeader(http.StatusOK)
			w.Write(m.body)
		}
		if afterRead != nil {
			afterRead()
		}
	case http.MethodPut:
		body, err := io.ReadAll(req.Body)
		if err != nil {
			r.t.Errorf("failed to read body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.failPuts.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		ifMatch := req.Header.Get("If-Match")
		ifNoneMatch := req.Header.Get("If-None-Match")
		conditional := ifMatch != "" || ifNoneMatch != ""

		r.mu.Lock()
		if conditional && r.rejectConditionalHeaders {
			r.mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if conditional && !r.enforcePreconditions {
			// Accept the conditional write without checking it, like a registry
			// that tolerates the header but has no ETag conditions.
			r.unconditionalPuts.Add(1)
			r.manifests[reference] = referrersIndexManifest{mediaType: req.Header.Get("Content-Type"), body: body}
			r.mu.Unlock()
			w.Header().Set("Docker-Content-Digest", digest.FromBytes(body).String())
			w.WriteHeader(http.StatusCreated)
			return
		}
		if conditional {
			if r.beforeConditionalCheck != nil {
				r.beforeConditionalCheck()
			}
			etag, exists := r.validatorLocked(reference)
			matchesNone := ifNoneMatch == "*" && !exists
			matchesETag := ifMatch != "" && exists && ifMatch == etag
			if !matchesNone && !matchesETag {
				r.conditionalMisses.Add(1)
				r.mu.Unlock()
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
		if !conditional {
			r.unconditionalPuts.Add(1)
		}
		r.manifests[reference] = referrersIndexManifest{mediaType: req.Header.Get("Content-Type"), body: body}
		r.mu.Unlock()
		w.Header().Set("Docker-Content-Digest", digest.FromBytes(body).String())
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		r.mu.Lock()
		delete(r.manifests, reference)
		r.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	default:
		r.t.Errorf("unexpected access: %s %q", req.Method, req.URL)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// newRepository returns a repository pointed at the registry.
func (r *referrersIndexRegistry) newRepository() *Repository {
	repo, err := NewRepository(strings.TrimPrefix(r.server.URL, "http://") + "/test")
	if err != nil {
		r.t.Fatalf("NewRepository() error = %v", err)
	}
	repo.Registry.PlainHTTP = true
	repo.SetReferrersCapability(false)
	return repo
}

// server is started lazily by start; tests call start before newRepository.
func (r *referrersIndexRegistry) start() {
	r.server = httptest.NewServer(r)
	r.t.Cleanup(r.server.Close)
}

// testReferrer builds a descriptor for a referrer manifest with a distinct
// artifactType and annotations, matching what indexReferrersForPush records.
func testReferrer(name, artifactType string, annotations map[string]string) ocispec.Descriptor {
	return ocispec.Descriptor{
		MediaType:    spec.MediaTypeArtifactManifest,
		Digest:       digest.FromString(name),
		Size:         int64(len(name)),
		ArtifactType: artifactType,
		Annotations:  annotations,
	}
}

// Test_updateReferrersIndex_conditionalConflict tests that a competing writer
// that publishes between this client's read and write makes the transaction
// retry on a fresh read and merge both descriptors, rather than overwriting the
// competing entry.
func Test_updateReferrersIndex_conditionalConflict(t *testing.T) {
	ctx := context.Background()
	subject := content.NewDescriptorFromBytes(spec.MediaTypeArtifactManifest, []byte(`{"layers":[]}`))
	referrersTag, err := buildReferrersTag(subject)
	if err != nil {
		t.Fatal(err)
	}
	mine := testReferrer("mine", "application/vnd.test.mine", map[string]string{"owner": "me"})
	theirs := testReferrer("theirs", "application/vnd.test.theirs", map[string]string{"owner": "them"})

	registry := newReferrersIndexRegistry(t, true)
	registry.set(referrersTag, ocispec.MediaTypeImageIndex, mustMarshalIndex(t, nil))
	// After this client reads the index, another client publishes its own
	// addition before this client's conditional write lands.
	var competed atomic.Bool
	registry.afterRead = func() {
		if competed.CompareAndSwap(false, true) {
			registry.set(referrersTag, ocispec.MediaTypeImageIndex, mustMarshalIndex(t, []ocispec.Descriptor{theirs}))
		}
	}
	registry.start()

	repo := registry.newRepository()
	store := repo.manifests()
	if err := store.updateReferrersIndex(ctx, subject, referrerChange{mine, referrerOperationAdd}); err != nil {
		t.Fatalf("updateReferrersIndex() error = %v", err)
	}

	if !competed.Load() {
		t.Fatal("test setup: the competing write never happened")
	}
	if got := registry.conditionalMisses.Load(); got == 0 {
		t.Error("no conditional PUT was rejected")
	}
	got := registry.indexReferrers(t, referrersTag)
	if !containsReferrer(got, mine) {
		t.Errorf("this client's referrer was lost: %v", got)
	}
	if !containsReferrer(got, theirs) {
		t.Errorf("the competing client's referrer was lost: %v", got)
	}
}

// Test_updateReferrersIndex_conditionalConflictGivesUp tests that a transaction
// which keeps losing the race stops after a bounded number of attempts and
// reports the conflict instead of retrying forever.
func Test_updateReferrersIndex_conditionalConflictGivesUp(t *testing.T) {
	ctx := context.Background()
	subject := content.NewDescriptorFromBytes(spec.MediaTypeArtifactManifest, []byte(`{"layers":[]}`))
	referrersTag, err := buildReferrersTag(subject)
	if err != nil {
		t.Fatal(err)
	}
	mine := testReferrer("mine", "application/vnd.test.mine", nil)

	registry := newReferrersIndexRegistry(t, true)
	registry.set(referrersTag, ocispec.MediaTypeImageIndex, mustMarshalIndex(t, nil))
	// Before every conditional write is checked, a competitor publishes a new
	// distinct index, so this client's writes are always stale.
	var noise atomic.Int32
	registry.beforeConditionalCheck = func() {
		registry.manifests[referrersTag] = referrersIndexManifest{
			mediaType: ocispec.MediaTypeImageIndex,
			body: mustMarshalIndex(t, []ocispec.Descriptor{
				testReferrer(fmt.Sprintf("noise-%d", noise.Add(1)), "application/vnd.test.noise", nil),
			}),
		}
	}
	registry.start()

	repo := registry.newRepository()
	store := repo.manifests()
	err = store.updateReferrersIndex(ctx, subject, referrerChange{mine, referrerOperationAdd})
	if !errorsIsConflict(err) {
		t.Fatalf("updateReferrersIndex() error = %v, want a conflict error", err)
	}
	if got := registry.conditionalMisses.Load(); got != maxReferrersIndexAttempts {
		t.Errorf("conditional attempts = %d, want %d", got, maxReferrersIndexAttempts)
	}
}

// Test_updateReferrersIndex_baselinePrecondition tests that the conditional
// push carries the baseline read: If-Match with the read index digest on an
// existing index, and If-None-Match: * when no index exists yet.
func Test_updateReferrersIndex_baselinePrecondition(t *testing.T) {
	ctx := context.Background()
	subject := content.NewDescriptorFromBytes(spec.MediaTypeArtifactManifest, []byte(`{"layers":[]}`))
	referrersTag, err := buildReferrersTag(subject)
	if err != nil {
		t.Fatal(err)
	}
	existing := testReferrer("existing", "application/vnd.test.existing", nil)
	mine := testReferrer("mine", "application/vnd.test.mine", nil)

	for _, tt := range []struct {
		name        string
		initial     []ocispec.Descriptor
		wantIfMatch string
		wantIfNone  string
	}{
		{
			name:        "existing index uses If-Match",
			initial:     []ocispec.Descriptor{existing},
			wantIfMatch: `"` + digest.FromBytes(mustMarshalIndex(t, []ocispec.Descriptor{existing})).String() + `"`,
		},
		{
			name:       "missing index uses If-None-Match",
			wantIfNone: "*",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			registry := newReferrersIndexRegistry(t, true)
			if tt.initial != nil {
				registry.set(referrersTag, ocispec.MediaTypeImageIndex, mustMarshalIndex(t, tt.initial))
			}
			var gotIfMatch, gotIfNone string
			registry.start()
			// Wrap the server handler to observe request headers.
			captured := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method == http.MethodPut && strings.HasSuffix(req.URL.Path, referrersTag) {
					gotIfMatch = req.Header.Get("If-Match")
					gotIfNone = req.Header.Get("If-None-Match")
				}
				registry.ServeHTTP(w, req)
			}))
			defer captured.Close()
			repo, err := NewRepository(strings.TrimPrefix(captured.URL, "http://") + "/test")
			if err != nil {
				t.Fatal(err)
			}
			repo.Registry.PlainHTTP = true
			repo.SetReferrersCapability(false)

			if err := repo.manifests().updateReferrersIndex(ctx, subject, referrerChange{mine, referrerOperationAdd}); err != nil {
				t.Fatalf("updateReferrersIndex() error = %v", err)
			}
			if gotIfMatch != tt.wantIfMatch {
				t.Errorf("If-Match = %q, want %q", gotIfMatch, tt.wantIfMatch)
			}
			if gotIfNone != tt.wantIfNone {
				t.Errorf("If-None-Match = %q, want %q", gotIfNone, tt.wantIfNone)
			}
		})
	}
}

// Test_updateReferrersIndex_preconditionlessFallback tests that a registry which
// rejects the conditional header still gets its referrers index updated, and
// that later updates do not retry the header.
func Test_updateReferrersIndex_preconditionlessFallback(t *testing.T) {
	ctx := context.Background()
	subject := content.NewDescriptorFromBytes(spec.MediaTypeArtifactManifest, []byte(`{"layers":[]}`))
	referrersTag, err := buildReferrersTag(subject)
	if err != nil {
		t.Fatal(err)
	}
	registry := newReferrersIndexRegistry(t, false)
	registry.rejectConditionalHeaders = true
	registry.set(referrersTag, ocispec.MediaTypeImageIndex, mustMarshalIndex(t, nil))
	registry.start()

	repo := registry.newRepository()
	store := repo.manifests()
	first := testReferrer("first", "application/vnd.test.first", nil)
	if err := store.updateReferrersIndex(ctx, subject, referrerChange{first, referrerOperationAdd}); err != nil {
		t.Fatalf("updateReferrersIndex() first error = %v", err)
	}
	if !repo.referrersPreconditionUnsupported.Load() {
		t.Error("referrersPreconditionUnsupported was not recorded")
	}
	second := testReferrer("second", "application/vnd.test.second", nil)
	if err := store.updateReferrersIndex(ctx, subject, referrerChange{second, referrerOperationAdd}); err != nil {
		t.Fatalf("updateReferrersIndex() second error = %v", err)
	}
	if got := registry.unconditionalPuts.Load(); got != 2 {
		t.Errorf("unconditional PUTs = %d, want 2", got)
	}
	got := registry.indexReferrers(t, referrersTag)
	if !containsReferrer(got, first) || !containsReferrer(got, second) {
		t.Errorf("referrers = %v, want both first and second", got)
	}
}

// Test_updateReferrersIndex_ignoringRegistryKeepsLastWrite tests the documented
// best-effort behaviour on a registry that accepts conditional headers but does
// not enforce them: concurrent writers can still lose an entry. This pins the
// limit of the fallback rather than asserting that it is safe.
func Test_updateReferrersIndex_ignoringRegistryKeepsLastWrite(t *testing.T) {
	ctx := context.Background()
	subject := content.NewDescriptorFromBytes(spec.MediaTypeArtifactManifest, []byte(`{"layers":[]}`))
	referrersTag, err := buildReferrersTag(subject)
	if err != nil {
		t.Fatal(err)
	}
	registry := newReferrersIndexRegistry(t, false)
	registry.set(referrersTag, ocispec.MediaTypeImageIndex, mustMarshalIndex(t, nil))
	registry.start()

	repo := registry.newRepository()
	store := repo.manifests()
	mine := testReferrer("mine", "application/vnd.test.mine", nil)
	if err := store.updateReferrersIndex(ctx, subject, referrerChange{mine, referrerOperationAdd}); err != nil {
		t.Fatalf("updateReferrersIndex() error = %v", err)
	}
	if got := registry.conditionalMisses.Load(); got != 0 {
		t.Errorf("conditional misses = %d, want 0: an enforcing registry was not used", got)
	}
	got := registry.indexReferrers(t, referrersTag)
	if !containsReferrer(got, mine) {
		t.Errorf("referrers = %v, want %v", got, mine)
	}
}

// Test_updateReferrersIndex_retryPreservesUploadedReferrer tests that when the
// index publish fails, the referrer manifest already stored by the registry
// stays available and the update can be retried to completion.
func Test_updateReferrersIndex_retryPreservesUploadedReferrer(t *testing.T) {
	ctx := context.Background()
	subject := content.NewDescriptorFromBytes(spec.MediaTypeArtifactManifest, []byte(`{"layers":[]}`))
	referrersTag, err := buildReferrersTag(subject)
	if err != nil {
		t.Fatal(err)
	}
	registry := newReferrersIndexRegistry(t, true)
	registry.set(referrersTag, ocispec.MediaTypeImageIndex, mustMarshalIndex(t, nil))
	registry.start()

	repo := registry.newRepository()
	store := repo.manifests()

	// The first publish fails with a server error and leaves the index as it
	// was.
	registry.failPuts.Store(true)
	mine := testReferrer("mine", "application/vnd.test.mine", nil)
	err = store.updateReferrersIndex(ctx, subject, referrerChange{mine, referrerOperationAdd})
	if err == nil {
		t.Fatal("updateReferrersIndex() error = nil, want a push failure")
	}
	// Nothing was deleted on the failure path: the index is still readable.
	if _, ok := registry.get(referrersTag); !ok {
		t.Fatal("the referrers index was deleted after a failed publish")
	}

	// Retry after the failure: the update completes and the index names the
	// referrer exactly once.
	registry.failPuts.Store(false)
	if err := store.updateReferrersIndex(ctx, subject, referrerChange{mine, referrerOperationAdd}); err != nil {
		t.Fatalf("updateReferrersIndex() retry error = %v", err)
	}
	got := registry.indexReferrers(t, referrersTag)
	if countReferrer(got, mine) != 1 {
		t.Errorf("referrers = %v, want exactly one entry for %v", got, mine)
	}
}

// Test_updateReferrersIndex_descriptorMetadataConsistent tests that re-adding an
// existing referrer with different artifactType or annotations refreshes the
// index entry, keeping the index consistent with the pushed manifest.
func Test_updateReferrersIndex_descriptorMetadataConsistent(t *testing.T) {
	ctx := context.Background()
	subject := content.NewDescriptorFromBytes(spec.MediaTypeArtifactManifest, []byte(`{"layers":[]}`))
	referrersTag, err := buildReferrersTag(subject)
	if err != nil {
		t.Fatal(err)
	}
	registry := newReferrersIndexRegistry(t, true)
	registry.start()
	repo := registry.newRepository()
	store := repo.manifests()

	stale := testReferrer("same", "application/vnd.test.old", map[string]string{"k": "old"})
	if err := store.updateReferrersIndex(ctx, subject, referrerChange{stale, referrerOperationAdd}); err != nil {
		t.Fatalf("updateReferrersIndex() error = %v", err)
	}
	fresh := testReferrer("same", "application/vnd.test.new", map[string]string{"k": "new"})
	if err := store.updateReferrersIndex(ctx, subject, referrerChange{fresh, referrerOperationAdd}); err != nil {
		t.Fatalf("updateReferrersIndex() error = %v", err)
	}
	got := registry.indexReferrers(t, referrersTag)
	if len(got) != 1 {
		t.Fatalf("referrers = %v, want one entry", got)
	}
	if got[0].ArtifactType != fresh.ArtifactType || !reflect.DeepEqual(got[0].Annotations, fresh.Annotations) {
		t.Errorf("referrers[0] = %+v, want the refreshed descriptor %+v", got[0], fresh)
	}
}

// Test_updateReferrersIndex_concurrentBatchesLoseNothing tests that many
// concurrent updates in one process all land in the index, with the merge pool
// batching them and the registry enforcing conditional writes.
func Test_updateReferrersIndex_concurrentBatchesLoseNothing(t *testing.T) {
	ctx := context.Background()
	subject := content.NewDescriptorFromBytes(spec.MediaTypeArtifactManifest, []byte(`{"layers":[]}`))
	referrersTag, err := buildReferrersTag(subject)
	if err != nil {
		t.Fatal(err)
	}
	registry := newReferrersIndexRegistry(t, true)
	registry.start()
	repo := registry.newRepository()
	store := repo.manifests()

	const writers = 16
	descs := make([]ocispec.Descriptor, writers)
	for i := range descs {
		descs[i] = testReferrer(fmt.Sprintf("ref-%d", i), "application/vnd.test", map[string]string{"i": strconv.Itoa(i)})
	}
	eg, egCtx := errgroup.WithContext(ctx)
	for i := range descs {
		eg.Go(func() error {
			return store.updateReferrersIndex(egCtx, subject, referrerChange{descs[i], referrerOperationAdd})
		})
	}
	if err := eg.Wait(); err != nil {
		t.Fatalf("concurrent updateReferrersIndex() error = %v", err)
	}
	got := registry.indexReferrers(t, referrersTag)
	for _, desc := range descs {
		if !containsReferrer(got, desc) {
			t.Errorf("referrer %s lost from the index", desc.Digest)
		}
	}
	if len(got) != writers {
		t.Errorf("referrers count = %d, want %d", len(got), writers)
	}
}

// Test_updateReferrersIndex_concurrentWritersLoseNothing simulates two clients
// in the same repository that race against each other through the registry, each
// publishing conditionally from a stale baseline. The enforcing registry rejects
// the losing write; the losing client re-reads and merges, so the final index
// contains both.
func Test_updateReferrersIndex_concurrentWritersLoseNothing(t *testing.T) {
	ctx := context.Background()
	subject := content.NewDescriptorFromBytes(spec.MediaTypeArtifactManifest, []byte(`{"layers":[]}`))
	referrersTag, err := buildReferrersTag(subject)
	if err != nil {
		t.Fatal(err)
	}
	registry := newReferrersIndexRegistry(t, true)
	registry.set(referrersTag, ocispec.MediaTypeImageIndex, mustMarshalIndex(t, nil))
	registry.start()

	// Two repositories model two clients in the same namespace; the merge pool
	// is per repository, so only the registry's conditional writes serialize
	// them.
	repoA, err := NewRepository(strings.TrimPrefix(registry.server.URL, "http://") + "/test")
	if err != nil {
		t.Fatal(err)
	}
	repoA.Registry.PlainHTTP = true
	repoA.SetReferrersCapability(false)
	repoB, err := NewRepository(strings.TrimPrefix(registry.server.URL, "http://") + "/test")
	if err != nil {
		t.Fatal(err)
	}
	repoB.Registry.PlainHTTP = true
	repoB.SetReferrersCapability(false)

	// Synchronization so both clients read an empty baseline before either
	// publishes: the first client to finish reading waits until both have read.
	var reads atomic.Int32
	bothRead := make(chan struct{})
	var once sync.Once
	registry.afterRead = func() {
		if reads.Add(1) >= 2 {
			once.Do(func() { close(bothRead) })
		}
		<-bothRead
	}

	refA := testReferrer("a", "application/vnd.test.a", nil)
	refB := testReferrer("b", "application/vnd.test.b", nil)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return repoA.manifests().updateReferrersIndex(egCtx, subject, referrerChange{refA, referrerOperationAdd})
	})
	eg.Go(func() error {
		return repoB.manifests().updateReferrersIndex(egCtx, subject, referrerChange{refB, referrerOperationAdd})
	})
	if err := eg.Wait(); err != nil {
		t.Fatalf("concurrent updateReferrersIndex() error = %v", err)
	}
	got := registry.indexReferrers(t, referrersTag)
	if !containsReferrer(got, refA) || !containsReferrer(got, refB) {
		t.Errorf("referrers = %v, want both %v and %v", got, refA, refB)
	}
}

// Test_ManifestStore_Push_ReferrersAPIUnavailable_ConditionalPush tests that a
// manifest push with a subject uses a conditional index publish on a registry
// with ETag conditions and still lists the referrer.
func Test_ManifestStore_Push_ReferrersAPIUnavailable_ConditionalPush(t *testing.T) {
	ctx := context.Background()
	subject := content.NewDescriptorFromBytes(spec.MediaTypeArtifactManifest, []byte(`{"layers":[]}`))
	referrersTag, err := buildReferrersTag(subject)
	if err != nil {
		t.Fatal(err)
	}

	artifact := spec.Artifact{
		MediaType:    spec.MediaTypeArtifactManifest,
		ArtifactType: "application/vnd.test.artifact",
		Subject:      &subject,
		Annotations:  map[string]string{"foo": "bar"},
	}
	artifactJSON, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	artifactDesc := content.NewDescriptorFromBytes(artifact.MediaType, artifactJSON)
	artifactDesc.ArtifactType = artifact.ArtifactType
	artifactDesc.Annotations = artifact.Annotations

	registry := newReferrersIndexRegistry(t, true)
	registry.start()
	// Manifest pushes other than the index are stored too, so the subject and
	// artifact endpoints resolve.
	inner := registry.server.Config.Handler
	registry.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, "/v2/test/manifests/") &&
			!strings.HasSuffix(req.URL.Path, referrersTag) &&
			(req.Method == http.MethodPut || req.Method == http.MethodGet || req.Method == http.MethodHead) {
			body, _ := io.ReadAll(req.Body)
			switch req.Method {
			case http.MethodPut:
				key := strings.TrimPrefix(req.URL.Path, "/v2/test/manifests/")
				registry.set(key, req.Header.Get("Content-Type"), body)
				w.Header().Set("Docker-Content-Digest", digest.FromBytes(body).String())
				w.WriteHeader(http.StatusCreated)
			case http.MethodGet:
				key := strings.TrimPrefix(req.URL.Path, "/v2/test/manifests/")
				m, ok := registry.get(key)
				if !ok {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", m.mediaType)
				w.Header().Set("Content-Length", strconv.Itoa(len(m.body)))
				w.Header().Set("Docker-Content-Digest", digest.FromBytes(m.body).String())
				w.WriteHeader(http.StatusOK)
				w.Write(m.body)
			}
			return
		}
		inner.ServeHTTP(w, req)
	})

	repo := registry.newRepository()
	if err := repo.Push(ctx, artifactDesc, bytes.NewReader(artifactJSON)); err != nil {
		t.Fatalf("Push() error = %v", err)
	}
	got := registry.indexReferrers(t, referrersTag)
	if len(got) != 1 {
		t.Fatalf("referrers = %v, want one entry", got)
	}
	if !content.Equal(got[0], artifactDesc) {
		t.Errorf("referrers[0] = %v, want %v", got[0], artifactDesc)
	}
	if got[0].ArtifactType != artifactDesc.ArtifactType {
		t.Errorf("artifactType = %q, want %q", got[0].ArtifactType, artifactDesc.ArtifactType)
	}
	if !reflect.DeepEqual(got[0].Annotations, artifactDesc.Annotations) {
		t.Errorf("annotations = %v, want %v", got[0].Annotations, artifactDesc.Annotations)
	}

	// List the referrers through the public API to confirm the published index
	// is readable back.
	var listed []ocispec.Descriptor
	if err := repo.Referrers(ctx, subject, "", func(page []ocispec.Descriptor) error {
		listed = append(listed, page...)
		return nil
	}); err != nil {
		t.Fatalf("Referrers() error = %v", err)
	}
	if len(listed) != 1 || !content.Equal(listed[0], artifactDesc) {
		t.Errorf("Referrers() = %v, want [%v]", listed, artifactDesc)
	}
}

// Test_updateReferrersIndex_tagschemaConcurrencyLostUpdate is a regression test
// for the read-modify-write race: with two clients that both read the index
// before either writes, one client's addition must not be lost.
func Test_updateReferrersIndex_tagschemaConcurrencyLostUpdate(t *testing.T) {
	ctx := context.Background()
	subject := content.NewDescriptorFromBytes(spec.MediaTypeArtifactManifest, []byte(`{"layers":[]}`))
	referrersTag, err := buildReferrersTag(subject)
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name                 string
		enforcePreconditions bool
		wantBothPresent      bool
	}{
		{
			name:                 "enforcing registry merges both",
			enforcePreconditions: true,
			wantBothPresent:      true,
		},
		{
			name:                 "non-enforcing registry is best effort",
			enforcePreconditions: false,
			wantBothPresent:      false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			registry := newReferrersIndexRegistry(t, tt.enforcePreconditions)
			registry.set(referrersTag, ocispec.MediaTypeImageIndex, mustMarshalIndex(t, nil))
			// Interleave: after this client reads the baseline, the competing
			// client publishes its own addition.
			refA := testReferrer("a", "application/vnd.test.a", nil)
			refB := testReferrer("b", "application/vnd.test.b", nil)
			var injected atomic.Bool
			registry.afterRead = func() {
				if injected.CompareAndSwap(false, true) {
					registry.set(referrersTag, ocispec.MediaTypeImageIndex, mustMarshalIndex(t, []ocispec.Descriptor{refB}))
				}
			}
			registry.start()
			repo := registry.newRepository()
			if err := repo.manifests().updateReferrersIndex(ctx, subject, referrerChange{refA, referrerOperationAdd}); err != nil {
				t.Fatalf("updateReferrersIndex() error = %v", err)
			}
			got := registry.indexReferrers(t, referrersTag)
			bothPresent := containsReferrer(got, refA) && containsReferrer(got, refB)
			if bothPresent != tt.wantBothPresent {
				t.Errorf("both referrers present = %v, want %v (index: %v)", bothPresent, tt.wantBothPresent, got)
			}
		})
	}
}

// indexReferrers decodes the index stored under the referrers tag.
func (r *referrersIndexRegistry) indexReferrers(t *testing.T, referrersTag string) []ocispec.Descriptor {
	t.Helper()
	m, ok := r.get(referrersTag)
	if !ok {
		t.Fatalf("no manifest stored under referrers tag %s", referrersTag)
	}
	var index ocispec.Index
	if err := json.Unmarshal(m.body, &index); err != nil {
		t.Fatalf("failed to decode referrers index: %v", err)
	}
	return index.Manifests
}

func mustMarshalIndex(t *testing.T, manifests []ocispec.Descriptor) []byte {
	t.Helper()
	if manifests == nil {
		manifests = []ocispec.Descriptor{}
	}
	body, err := json.Marshal(ocispec.Index{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageIndex,
		Manifests: manifests,
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func containsReferrer(descs []ocispec.Descriptor, want ocispec.Descriptor) bool {
	return countReferrer(descs, want) > 0
}

func countReferrer(descs []ocispec.Descriptor, want ocispec.Descriptor) int {
	var n int
	for _, d := range descs {
		if content.Equal(d, want) {
			n++
		}
	}
	return n
}

func errorsIsConflict(err error) bool {
	return err != nil && strings.Contains(err.Error(), errReferrersIndexConflict.Error())
}
