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
	"encoding/json"
	"errors"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/content/memory"
	"github.com/oras-project/oras-go/v3/errdef"
)

// faultTransactionStore wraps an in-memory store with push failures, fetch
// corruption and cancellation hooks.
type faultTransactionStore struct {
	*memory.Store

	mu         sync.Mutex
	pushCount  map[digest.Digest]int
	failPush   map[digest.Digest]error
	failExists map[digest.Digest]error
	corrupt    map[digest.Digest][]byte
	beforePush func(context.Context, ocispec.Descriptor) error
	pushOrder  []digest.Digest
}

func newFaultTransactionStore() *faultTransactionStore {
	return &faultTransactionStore{
		Store:      memory.New(),
		pushCount:  make(map[digest.Digest]int),
		failPush:   make(map[digest.Digest]error),
		failExists: make(map[digest.Digest]error),
		corrupt:    make(map[digest.Digest][]byte),
	}
}

func (s *faultTransactionStore) Push(ctx context.Context, desc ocispec.Descriptor, r io.Reader) error {
	s.mu.Lock()
	s.pushCount[desc.Digest]++
	s.pushOrder = append(s.pushOrder, desc.Digest)
	hook := s.beforePush
	pushErr := s.failPush[desc.Digest]
	s.mu.Unlock()

	if hook != nil {
		if err := hook(ctx, desc); err != nil {
			return err
		}
	}
	if pushErr != nil {
		return pushErr
	}

	// Repair a test-only corrupted Fetch view when the node is repushed.
	s.mu.Lock()
	delete(s.corrupt, desc.Digest)
	s.mu.Unlock()

	return s.Store.Push(ctx, desc, r)
}

func (s *faultTransactionStore) Fetch(ctx context.Context, desc ocispec.Descriptor) (io.ReadCloser, error) {
	s.mu.Lock()
	bad, ok := s.corrupt[desc.Digest]
	s.mu.Unlock()
	if ok {
		return io.NopCloser(bytes.NewReader(bad)), nil
	}
	return s.Store.Fetch(ctx, desc)
}

func (s *faultTransactionStore) Exists(ctx context.Context, desc ocispec.Descriptor) (bool, error) {
	s.mu.Lock()
	err := s.failExists[desc.Digest]
	s.mu.Unlock()
	if err != nil {
		return false, err
	}
	return s.Store.Exists(ctx, desc)
}

func (s *faultTransactionStore) pushes(d digest.Digest) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pushCount[d]
}

func (s *faultTransactionStore) totalPushes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int
	for _, n := range s.pushCount {
		total += n
	}
	return total
}

func (s *faultTransactionStore) resetPushes() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushCount = make(map[digest.Digest]int)
	s.pushOrder = nil
}

func pushTransactionBytes(t *testing.T, ctx context.Context, store content.Storage, mediaType string, data []byte) ocispec.Descriptor {
	t.Helper()
	desc := content.NewDescriptorFromBytes(mediaType, data)
	if err := store.Push(ctx, desc, bytes.NewReader(data)); err != nil && !errors.Is(err, errdef.ErrAlreadyExists) {
		t.Fatalf("Push(%s): %v", mediaType, err)
	}
	return desc
}

func pushTransactionJSON(t *testing.T, ctx context.Context, store content.Storage, mediaType string, value any) ocispec.Descriptor {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return pushTransactionBytes(t, ctx, store, mediaType, data)
}

func txBlob(t *testing.T, ctx context.Context, store content.Storage, text string) ocispec.Descriptor {
	t.Helper()
	return pushTransactionBytes(t, ctx, store, ocispec.MediaTypeImageLayer, []byte(text))
}

func txConfig(t *testing.T, ctx context.Context, store content.Storage, text string) ocispec.Descriptor {
	t.Helper()
	return pushTransactionBytes(t, ctx, store, ocispec.MediaTypeImageConfig, []byte(text))
}

func txManifest(t *testing.T, ctx context.Context, store content.Storage, config ocispec.Descriptor, layers ...ocispec.Descriptor) ocispec.Descriptor {
	t.Helper()
	return pushTransactionJSON(t, ctx, store, ocispec.MediaTypeImageManifest, ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    config,
		Layers:    layers,
	})
}

func txIndex(t *testing.T, ctx context.Context, store content.Storage, children ...ocispec.Descriptor) ocispec.Descriptor {
	t.Helper()
	return pushTransactionJSON(t, ctx, store, ocispec.MediaTypeImageIndex, ocispec.Index{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageIndex,
		Manifests: children,
	})
}

func txMultiPlatformGraph(t *testing.T, ctx context.Context, store content.Storage) (root ocispec.Descriptor, platformA, platformB txPlatformNodes) {
	t.Helper()
	platformA.layer = txBlob(t, ctx, store, "layer-a")
	platformA.config = txConfig(t, ctx, store, `{"os":"linux","architecture":"amd64"}`)
	platformA.manifest = txManifest(t, ctx, store, platformA.config, platformA.layer)

	platformB.layer = txBlob(t, ctx, store, "layer-b")
	platformB.config = txConfig(t, ctx, store, `{"os":"linux","architecture":"arm64"}`)
	platformB.manifest = txManifest(t, ctx, store, platformB.config, platformB.layer)

	root = txIndex(t, ctx, store, platformA.manifest, platformB.manifest)
	return root, platformA, platformB
}

type txPlatformNodes struct {
	layer    ocispec.Descriptor
	config   ocispec.Descriptor
	manifest ocispec.Descriptor
}

func requireDigestSet(t *testing.T, got []ocispec.Descriptor, want ...ocispec.Descriptor) {
	t.Helper()
	gotSet := make(map[digest.Digest]struct{}, len(got))
	for _, desc := range got {
		if _, dup := gotSet[desc.Digest]; dup {
			t.Fatalf("duplicate descriptor %s in result", desc.Digest)
		}
		gotSet[desc.Digest] = struct{}{}
	}
	if len(gotSet) != len(want) {
		t.Fatalf("got %d descriptors (%v), want %d (%v)", len(gotSet), got, len(want), want)
	}
	for _, desc := range want {
		if _, ok := gotSet[desc.Digest]; !ok {
			t.Fatalf("missing descriptor %s; got %v", desc.Digest, got)
		}
	}
}

func requireFailedDigests(t *testing.T, report *oras.GraphTransactionReport, want ...ocispec.Descriptor) {
	t.Helper()
	if report == nil {
		t.Fatal("nil report")
	}
	if len(report.FailedNodes) != len(want) {
		got := make([]digest.Digest, 0, len(report.FailedNodes))
		for _, node := range report.FailedNodes {
			got = append(got, node.Descriptor.Digest)
		}
		t.Fatalf("failed nodes = %v, want %v", got, want)
	}
	gotSet := make(map[digest.Digest]error, len(report.FailedNodes))
	for _, node := range report.FailedNodes {
		gotSet[node.Descriptor.Digest] = node.Err
	}
	for _, desc := range want {
		err, ok := gotSet[desc.Digest]
		if !ok {
			t.Fatalf("failed node %s not reported: %+v", desc.Digest, report.FailedNodes)
		}
		if err == nil {
			t.Fatalf("failed node %s has nil error", desc.Digest)
		}
	}
}

func requireSortedReport(t *testing.T, report *oras.GraphTransactionReport) {
	t.Helper()
	if !sort.SliceIsSorted(report.Nodes, func(i, j int) bool {
		return report.Nodes[i].Descriptor.Digest.String() < report.Nodes[j].Descriptor.Digest.String()
	}) {
		t.Fatal("Nodes are not digest-sorted")
	}
	if !sort.SliceIsSorted(report.CompletedNodes, func(i, j int) bool {
		return report.CompletedNodes[i].Digest.String() < report.CompletedNodes[j].Digest.String()
	}) {
		t.Fatal("CompletedNodes are not digest-sorted")
	}
	if !sort.SliceIsSorted(report.FailedNodes, func(i, j int) bool {
		return report.FailedNodes[i].Descriptor.Digest.String() < report.FailedNodes[j].Descriptor.Digest.String()
	}) {
		t.Fatal("FailedNodes are not digest-sorted")
	}
}

func TestCopyGraphTransaction_PublishesParentsAfterChildren(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := newFaultTransactionStore()

	layer := txBlob(t, ctx, src, "ordered-layer")
	config := txConfig(t, ctx, src, "{}")
	manifest := txManifest(t, ctx, src, config, layer)
	root := txIndex(t, ctx, src, manifest)

	report, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{
		Concurrency: 4,
	})
	if err != nil {
		t.Fatalf("CopyGraphTransaction: %v", err)
	}
	if report == nil || !report.OK() {
		t.Fatalf("report not OK: %+v", report)
	}
	requireDigestSet(t, report.Completed, layer, config, manifest, root)
	requireSortedReport(t, report)

	indexOf := func(d digest.Digest) int {
		for i, pushed := range dst.pushOrder {
			if pushed == d {
				return i
			}
		}
		return -1
	}
	for parent, children := range map[digest.Digest][]digest.Digest{
		manifest.Digest: {config.Digest, layer.Digest},
		root.Digest:     {manifest.Digest},
	} {
		parentPos := indexOf(parent)
		if parentPos < 0 {
			t.Fatalf("parent %s was not pushed", parent)
		}
		for _, child := range children {
			childPos := indexOf(child)
			if childPos < 0 || childPos > parentPos {
				t.Fatalf("child %s at %d was not pushed before parent %s at %d; order=%v",
					child, childPos, parent, parentPos, dst.pushOrder)
			}
		}
	}
}

func TestCopyGraphTransaction_PartialFailureStableReportAndResume(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := newFaultTransactionStore()
	root, platformA, platformB := txMultiPlatformGraph(t, ctx, src)

	boom := errors.New("simulated destination outage")
	dst.mu.Lock()
	dst.failPush[platformB.layer.Digest] = boom
	dst.mu.Unlock()

	report, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{
		Concurrency: 4,
	})
	if err == nil {
		t.Fatal("expected partial failure error")
	}
	var txErr *oras.GraphTransactionError
	if !errors.As(err, &txErr) {
		t.Fatalf("error %T is not *GraphTransactionError", err)
	}
	if txErr.Report != report {
		t.Fatal("GraphTransactionError does not wrap the returned report")
	}
	if !errors.Is(err, oras.ErrGraphTransactionFailed) {
		t.Fatal("error does not wrap ErrGraphTransactionFailed")
	}
	if !errors.Is(err, boom) {
		t.Fatal("error does not expose the root push failure")
	}
	if report.OK() {
		t.Fatal("report should not be OK")
	}

	// The unaffected platform and the independent config from the failing
	// platform are complete. The bad blob, its manifest and the root are failed.
	requireDigestSet(t, report.Completed,
		platformA.layer, platformA.config, platformA.manifest,
		platformB.config,
	)
	requireFailedDigests(t, report,
		platformB.layer, platformB.manifest, root,
	)
	if len(report.Blocked) != 2 {
		t.Fatalf("Blocked len = %d, want manifest and root: %+v", len(report.Blocked), report.Blocked)
	}
	requireSortedReport(t, report)

	for _, desc := range []ocispec.Descriptor{platformA.manifest, platformB.config} {
		exists, err := dst.Exists(ctx, desc)
		if err != nil || !exists {
			t.Fatalf("completed node %s exists=%v err=%v", desc.Digest, exists, err)
		}
	}
	for _, desc := range []ocispec.Descriptor{platformB.manifest, root} {
		exists, err := dst.Exists(ctx, desc)
		if err != nil || exists {
			t.Fatalf("unpublished parent %s exists=%v err=%v", desc.Digest, exists, err)
		}
	}

	// Remove the fault and retry. Durable, digest-verified nodes are skipped;
	// only the failed blob and two blocked parents should be pushed again.
	dst.mu.Lock()
	delete(dst.failPush, platformB.layer.Digest)
	dst.mu.Unlock()
	dst.resetPushes()

	retryReport, retryErr := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{
		Concurrency: 4,
	})
	if retryErr != nil {
		t.Fatalf("retry CopyGraphTransaction: %v\nreport: %+v", retryErr, retryReport)
	}
	if !retryReport.OK() {
		t.Fatalf("retry report not OK: %+v", retryReport)
	}
	requireDigestSet(t, retryReport.Completed,
		platformA.layer, platformA.config, platformA.manifest,
		platformB.layer, platformB.config, platformB.manifest, root,
	)
	if len(retryReport.Skipped) != 4 {
		t.Fatalf("Skipped len = %d, want four durable nodes: %v", len(retryReport.Skipped), retryReport.Skipped)
	}
	if got := dst.totalPushes(); got != 3 {
		t.Fatalf("retry pushes = %d, want blob, child manifest and root", got)
	}
	for _, desc := range []ocispec.Descriptor{platformB.layer, platformB.manifest, root} {
		if dst.pushes(desc.Digest) != 1 {
			t.Fatalf("node %s pushed %d times on retry, want 1", desc.Digest, dst.pushes(desc.Digest))
		}
		exists, err := dst.Exists(ctx, desc)
		if err != nil || !exists {
			t.Fatalf("retried node %s exists=%v err=%v", desc.Digest, exists, err)
		}
	}
}

func TestCopyGraphTransaction_RetryReverifiesCorruptExistingNode(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := newFaultTransactionStore()
	layer := txBlob(t, ctx, src, "corrupt-me")
	config := txConfig(t, ctx, src, "{}")
	root := txManifest(t, ctx, src, config, layer)

	if _, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{}); err != nil {
		t.Fatalf("initial copy: %v", err)
	}

	dst.mu.Lock()
	dst.corrupt[layer.Digest] = []byte("this is not the layer bytes")
	dst.mu.Unlock()
	dst.resetPushes()

	report, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{})
	if err != nil {
		t.Fatalf("repair retry: %v", err)
	}
	if !report.OK() {
		t.Fatalf("repair report not OK: %+v", report)
	}
	if dst.pushes(layer.Digest) != 1 {
		t.Fatalf("corrupt layer pushes = %d, want 1", dst.pushes(layer.Digest))
	}

	// The repaired target must now pass a fresh digest verification and be
	// skipped without another push.
	dst.resetPushes()
	report, err = oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{})
	if err != nil {
		t.Fatalf("verification retry: %v", err)
	}
	if !report.OK() {
		t.Fatalf("verification report not OK: %+v", report)
	}
	if got := dst.totalPushes(); got != 0 {
		t.Fatalf("clean retry made %d pushes, want 0", got)
	}
}

func TestCopyGraphTransaction_DeepGraph(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := memory.New()

	const depth = 500
	descs := make([]ocispec.Descriptor, depth)
	var child ocispec.Descriptor
	for i := depth - 1; i >= 0; i-- {
		if i == depth-1 {
			child = txIndex(t, ctx, src)
		} else {
			child = txIndex(t, ctx, src, child)
		}
		descs[i] = child
	}
	root := descs[0]

	start := time.Now()
	report, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{
		Concurrency: 16,
		MaxNodes:    depth + 10,
	})
	if err != nil {
		t.Fatalf("deep CopyGraphTransaction after %v: %v", time.Since(start), err)
	}
	if !report.OK() {
		t.Fatalf("deep report not OK: %+v", report)
	}
	if len(report.CompletedNodes) != depth {
		t.Fatalf("completed nodes = %d, want %d", len(report.CompletedNodes), depth)
	}
	requireSortedReport(t, report)
	for i, desc := range descs {
		exists, err := dst.Exists(ctx, desc)
		if err != nil {
			t.Fatalf("Exists(node %d): %v", i, err)
		}
		if !exists {
			t.Fatalf("deep node %d (%s) missing from destination", i, desc.Digest)
		}
	}
}

func TestCopyGraphTransaction_DepthAndNodeLimits(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := memory.New()

	const depth = 6
	var root, child ocispec.Descriptor
	for i := depth - 1; i >= 0; i-- {
		if i == depth-1 {
			child = txIndex(t, ctx, src)
		} else {
			child = txIndex(t, ctx, src, child)
		}
		if i == 0 {
			root = child
		}
	}

	t.Run("depth", func(t *testing.T) {
		_, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{
			MaxDepth: 2,
		})
		var limitErr *oras.GraphLimitError
		if !errors.As(err, &limitErr) {
			t.Fatalf("error %T is not *GraphLimitError: %v", err, err)
		}
		if limitErr.Kind != oras.GraphLimitDepth {
			t.Fatalf("kind = %q, want depth", limitErr.Kind)
		}
		if !errors.Is(err, oras.ErrGraphDepthLimitExceeded) {
			t.Fatal("depth error does not wrap sentinel")
		}
	})

	t.Run("depth alias", func(t *testing.T) {
		_, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{
			Depth: 2,
		})
		if !errors.Is(err, oras.ErrGraphDepthLimitExceeded) {
			t.Fatalf("Depth alias error %v does not wrap depth limit", err)
		}
	})

	t.Run("nodes", func(t *testing.T) {
		_, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{
			MaxNodes: 3,
		})
		var limitErr *oras.GraphLimitError
		if !errors.As(err, &limitErr) {
			t.Fatalf("error %T is not *GraphLimitError: %v", err, err)
		}
		if limitErr.Kind != oras.GraphLimitNodes {
			t.Fatalf("kind = %q, want nodes", limitErr.Kind)
		}
		if !errors.Is(err, oras.ErrGraphNodeLimitExceeded) {
			t.Fatal("node error does not wrap sentinel")
		}
	})
}

func TestCopyGraphTransaction_CycleVisitedOnce(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := newFaultTransactionStore()

	a := pushTransactionJSON(t, ctx, src, ocispec.MediaTypeImageIndex, ocispec.Index{
		Annotations: map[string]string{"cycle": "a"},
	})
	b := pushTransactionJSON(t, ctx, src, ocispec.MediaTypeImageIndex, ocispec.Index{
		Annotations: map[string]string{"cycle": "b"},
	})
	var mu sync.Mutex
	calls := make(map[digest.Digest]int)
	find := func(_ context.Context, _ content.Fetcher, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		mu.Lock()
		calls[desc.Digest]++
		mu.Unlock()
		switch desc.Digest {
		case a.Digest:
			return []ocispec.Descriptor{b}, nil
		case b.Digest:
			return []ocispec.Descriptor{a}, nil
		default:
			return nil, nil
		}
	}

	report, err := oras.CopyGraphTransaction(ctx, src, dst, a, oras.GraphTransactionOptions{
		FindSuccessors: find,
	})
	if err == nil {
		t.Fatal("expected cycle transaction error")
	}
	if !errors.Is(err, oras.ErrCyclicGraph) {
		t.Fatalf("error %v does not wrap ErrCyclicGraph", err)
	}
	requireFailedDigests(t, report, a, b)
	for _, d := range []ocispec.Descriptor{a, b} {
		if calls[d.Digest] != 1 {
			t.Fatalf("FindSuccessors(%s) calls = %d, want 1", d.Digest, calls[d.Digest])
		}
		if dst.pushes(d.Digest) != 0 {
			t.Fatalf("cyclic node %s was pushed %d times", d.Digest, dst.pushes(d.Digest))
		}
	}
}

func TestCopyGraphTransaction_CancellationStopsScheduling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	src := memory.New()
	dst := newFaultTransactionStore()

	blobs := []ocispec.Descriptor{
		txBlob(t, context.Background(), src, "cancel-1"),
		txBlob(t, context.Background(), src, "cancel-2"),
		txBlob(t, context.Background(), src, "cancel-3"),
		txBlob(t, context.Background(), src, "cancel-4"),
	}
	root := txIndex(t, context.Background(), src, blobs...)

	started := make(chan struct{}, 1)
	dst.beforePush = func(ctx context.Context, desc ocispec.Descriptor) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}

	done := make(chan error, 1)
	go func() {
		_, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{
			Concurrency: 1,
		})
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first push")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("transaction did not release workers after cancellation")
	}

	// Only the single worker permitted by Concurrency=1 could have entered
	// Push. Canceled waiters must fail before issuing another push.
	if got := dst.totalPushes(); got != 1 {
		t.Fatalf("pushes after cancel = %d, want only the in-flight push", got)
	}
}

func TestGraphTransaction_RetryUsesSameReportSemantics(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := newFaultTransactionStore()
	root, _, platformB := txMultiPlatformGraph(t, ctx, src)
	dst.mu.Lock()
	dst.failPush[platformB.layer.Digest] = errors.New("once")
	dst.mu.Unlock()

	tx := oras.NewGraphTransaction(src, dst, root, oras.GraphTransactionOptions{Concurrency: 2})
	report, err := tx.Run(ctx)
	if err == nil {
		t.Fatal("expected first run failure")
	}
	if tx.Report() != report {
		t.Fatal("Report did not return the latest run report")
	}

	dst.mu.Lock()
	delete(dst.failPush, platformB.layer.Digest)
	dst.mu.Unlock()
	report, err = tx.Retry(ctx)
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if !report.OK() {
		t.Fatalf("retry report not OK: %+v", report)
	}
	if tx.Report() != report {
		t.Fatal("Report did not update after Retry")
	}
}

func TestCopyGraphTransaction_CopyHooks(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := memory.New()
	layer := txBlob(t, ctx, src, "hooks")
	root := txIndex(t, ctx, src, layer)

	var mu sync.Mutex
	pre, post, skipped := make(map[digest.Digest]int), make(map[digest.Digest]int), make(map[digest.Digest]int)
	opts := oras.GraphTransactionOptions{Concurrency: 2}
	opts.PreCopy = func(_ context.Context, desc ocispec.Descriptor) error {
		mu.Lock()
		defer mu.Unlock()
		pre[desc.Digest]++
		return nil
	}
	opts.PostCopy = func(_ context.Context, desc ocispec.Descriptor) error {
		mu.Lock()
		defer mu.Unlock()
		post[desc.Digest]++
		return nil
	}
	opts.OnCopySkipped = func(_ context.Context, desc ocispec.Descriptor) error {
		mu.Lock()
		defer mu.Unlock()
		skipped[desc.Digest]++
		return nil
	}

	report, err := oras.CopyGraphTransaction(ctx, src, dst, root, opts)
	if err != nil {
		t.Fatalf("first copy: %v", err)
	}
	if !report.OK() {
		t.Fatalf("first report: %+v", report)
	}
	for _, d := range []ocispec.Descriptor{layer, root} {
		if pre[d.Digest] != 1 || post[d.Digest] != 1 {
			t.Fatalf("first hooks for %s: pre=%d post=%d", d.Digest, pre[d.Digest], post[d.Digest])
		}
	}

	mu.Lock()
	pre = make(map[digest.Digest]int)
	post = make(map[digest.Digest]int)
	mu.Unlock()
	report, err = oras.CopyGraphTransaction(ctx, src, dst, root, opts)
	if err != nil {
		t.Fatalf("second copy: %v", err)
	}
	if !report.OK() {
		t.Fatalf("second report: %+v", report)
	}
	for _, d := range []ocispec.Descriptor{layer, root} {
		mu.Lock()
		if pre[d.Digest] != 0 || post[d.Digest] != 0 || skipped[d.Digest] != 1 {
			mu.Unlock()
			t.Fatalf("retry hooks for %s: pre=%d post=%d skipped=%d", d.Digest, pre[d.Digest], post[d.Digest], skipped[d.Digest])
		}
		mu.Unlock()
	}
}

func TestCopyGraphTransaction_DuplicateDigestProcessedOnce(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := newFaultTransactionStore()
	child := txBlob(t, ctx, src, "duplicate-digest")
	root := txIndex(t, ctx, src)

	find := func(_ context.Context, _ content.Fetcher, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		if desc.Digest == root.Digest {
			duplicateMediaType := child
			duplicateMediaType.MediaType = "application/x-duplicate-descriptor"
			return []ocispec.Descriptor{child, child, duplicateMediaType}, nil
		}
		return nil, nil
	}

	report, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{
		MaxNodes:       2,
		FindSuccessors: find,
	})
	if err != nil {
		t.Fatalf("CopyGraphTransaction: %v", err)
	}
	if !report.OK() {
		t.Fatalf("report not OK: %+v", report)
	}
	if len(report.Nodes) != 2 {
		t.Fatalf("Nodes len = %d, want root plus one digest", len(report.Nodes))
	}
	if dst.pushes(child.Digest) != 1 {
		t.Fatalf("child pushes = %d, want 1", dst.pushes(child.Digest))
	}
}

func TestCopyGraphTransaction_SourceSuccessorFailureIsStable(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := newFaultTransactionStore()
	child := txBlob(t, ctx, src, "missing-successors")
	root := txIndex(t, ctx, src, child)

	find := func(ctx context.Context, fetcher content.Fetcher, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		if desc.Digest == child.Digest {
			return nil, errors.New("source read failed")
		}
		return content.Successors(ctx, fetcher, desc)
	}

	report, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{
		FindSuccessors: find,
	})
	if err == nil {
		t.Fatal("expected transaction error")
	}
	requireFailedDigests(t, report, child, root)
	if len(report.CompletedNodes) != 0 {
		t.Fatalf("completed nodes = %+v, want none", report.CompletedNodes)
	}
}

func TestCopyGraphTransaction_ExistsFailureAllowsIndependentChildren(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := newFaultTransactionStore()
	child := txBlob(t, ctx, src, "independent-child")
	root := txIndex(t, ctx, src, child)

	existsBoom := errors.New("destination exists failed")
	dst.mu.Lock()
	dst.failExists[root.Digest] = existsBoom
	dst.mu.Unlock()

	report, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{})
	if err == nil {
		t.Fatal("expected transaction error")
	}
	if !errors.Is(err, existsBoom) {
		t.Fatalf("error %v does not wrap Exists failure", err)
	}
	requireFailedDigests(t, report, root)
	requireDigestSet(t, report.Completed, child)
}

func TestCopyGraphTransaction_InvalidSuccessor(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := memory.New()
	root := txIndex(t, ctx, src)
	bad := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.Digest("not-a-digest"),
		Size:      1,
	}
	find := func(context.Context, content.Fetcher, ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		return []ocispec.Descriptor{bad}, nil
	}

	report, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{
		FindSuccessors: find,
	})
	if err == nil {
		t.Fatal("expected invalid successor error")
	}
	requireFailedDigests(t, report, root)
	if !errors.Is(err, errdef.ErrInvalidDigest) {
		t.Fatalf("error %v does not wrap ErrInvalidDigest", err)
	}
}

func TestCopyGraphTransaction_SkipDigestVerification(t *testing.T) {
	ctx := context.Background()
	src := memory.New()
	dst := newFaultTransactionStore()
	layer := txBlob(t, ctx, src, "skip-verification")
	root := txIndex(t, ctx, src, layer)

	if _, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{}); err != nil {
		t.Fatalf("initial copy: %v", err)
	}
	dst.mu.Lock()
	dst.corrupt[layer.Digest] = []byte("different bytes of the same length!!")
	dst.mu.Unlock()
	dst.resetPushes()

	report, err := oras.CopyGraphTransaction(ctx, src, dst, root, oras.GraphTransactionOptions{
		SkipDigestVerification: true,
	})
	if err != nil {
		t.Fatalf("skip verification retry: %v", err)
	}
	if !report.OK() {
		t.Fatalf("report not OK: %+v", report)
	}
	if got := dst.totalPushes(); got != 0 {
		t.Fatalf("pushes = %d, want zero when verification is skipped", got)
	}
}

func TestGraphTransactionReportAndLimitErrorStrings(t *testing.T) {
	var nilReport *oras.GraphTransactionReport
	if nilReport.OK() {
		t.Fatal("nil report should not be OK")
	}
	txErr := &oras.GraphTransactionError{}
	if txErr.Error() == "" {
		t.Fatal("empty transaction error string")
	}
	for _, err := range []error{
		(&oras.GraphLimitError{Kind: oras.GraphLimitDepth, Limit: 1, Value: 2}).Unwrap(),
		(&oras.GraphLimitError{Kind: oras.GraphLimitNodes, Limit: 1, Value: 2}).Unwrap(),
		(&oras.GraphLimitError{Kind: "other"}).Unwrap(),
	} {
		if err == nil {
			t.Fatal("limit error unwrap returned nil")
		}
	}
	cycle := &oras.GraphCycleError{Nodes: []ocispec.Descriptor{{Digest: "sha256:abc"}}}
	if !errors.Is(cycle, oras.ErrCyclicGraph) || cycle.Error() == "" {
		t.Fatal("cycle error should wrap sentinel and render")
	}
}

func TestCopyGraphTransaction_NilTargets(t *testing.T) {
	ctx := context.Background()
	desc := ocispec.Descriptor{Digest: digest.FromString("root"), Size: 1}
	if _, err := oras.CopyGraphTransaction(ctx, nil, memory.New(), desc, oras.GraphTransactionOptions{}); err == nil {
		t.Fatal("expected nil source error")
	}
	if _, err := oras.CopyGraphTransaction(ctx, memory.New(), nil, desc, oras.GraphTransactionOptions{}); err == nil {
		t.Fatal("expected nil destination error")
	}
}
