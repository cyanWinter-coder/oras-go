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
	"sort"
	"strings"
	"sync"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/internal/cas"
	"github.com/oras-project/oras-go/v3/internal/syncutil"
	"golang.org/x/sync/semaphore"
)

// Graph transaction limit kinds.
const (
	// GraphLimitDepth identifies a MaxDepth traversal limit error.
	GraphLimitDepth = "depth"
	// GraphLimitNodes identifies a MaxNodes traversal limit error.
	GraphLimitNodes = "nodes"
)

// GraphNodeState is the terminal state of a node in a graph transaction.
type GraphNodeState string

const (
	// GraphNodeStateCompleted means the node was copied by this call.
	GraphNodeStateCompleted GraphNodeState = "completed"
	// GraphNodeStateSkipped means the node already existed in the target and
	// passed digest verification, so it was not copied again.
	GraphNodeStateSkipped GraphNodeState = "skipped"
	// GraphNodeStateFailed means an operation on the node, or one of its
	// blocking dependencies, failed.
	GraphNodeStateFailed GraphNodeState = "failed"
	// GraphNodeStateCanceled means the node did not finish because the
	// context was canceled or timed out.
	GraphNodeStateCanceled GraphNodeState = "canceled"
)

// Common graph transaction errors.
var (
	// ErrGraphTransactionFailed is returned when one or more graph nodes fail.
	ErrGraphTransactionFailed = errors.New("graph transaction failed")
	// ErrGraphLimitExceeded is wrapped by depth and node-count limit errors.
	ErrGraphLimitExceeded = errors.New("graph traversal limit exceeded")
	// ErrGraphDepthLimitExceeded is returned when a graph is deeper than
	// GraphTransactionOptions.MaxDepth permits.
	ErrGraphDepthLimitExceeded = fmt.Errorf("%w: depth limit", ErrGraphLimitExceeded)
	// ErrGraphNodeLimitExceeded is returned when a graph contains more unique
	// digests than GraphTransactionOptions.MaxNodes permits.
	ErrGraphNodeLimitExceeded = fmt.Errorf("%w: node limit", ErrGraphLimitExceeded)
	// ErrCyclicGraph is returned when the nodes that still need to be copied
	// contain a dependency cycle.
	ErrCyclicGraph = errors.New("cyclic graph reference")
	// ErrNodeBlocked is wrapped by errors for nodes that were not copied because
	// a blocking dependency did not complete.
	ErrNodeBlocked = errors.New("node blocked by failed dependency")
)

// GraphTransactionOptions contains parameters for
// [CopyGraphTransaction].
//
// The zero value uses eight concurrent workers and does not limit graph depth
// or the number of unique nodes.
type GraphTransactionOptions struct {
	// Concurrency limits the maximum number of concurrent source lookups,
	// digest verifications and copy operations. If less than or equal to zero,
	// defaultConcurrency is used.
	Concurrency int

	// MaxMetadataBytes limits the maximum size of manifest metadata cached in
	// memory while finding successors. If less than or equal to zero,
	// defaultCopyMaxMetadataBytes is used.
	MaxMetadataBytes int64

	// MaxDepth limits the maximum node depth. The root is depth zero. A
	// positive value rejects a graph that contains a node below that depth.
	// Zero or a negative value allows unlimited depth.
	MaxDepth int

	// Depth is an alias for MaxDepth, matching the depth naming used by
	// ExtendedCopyGraphOptions. MaxDepth takes precedence when both are set.
	Depth int

	// MaxNodes limits the number of unique node digests visited by one
	// transaction. Repeated occurrences of the same digest count once. A
	// positive value rejects a graph with more than the allowed number of
	// unique nodes. Zero or a negative value allows an unlimited number of
	// nodes.
	MaxNodes int

	// SkipDigestVerification disables fetching and digest verification for
	// nodes that report as present in the target. By default, an existing node
	// is reused only when its bytes are fetched from the target and match its
	// descriptor.
	SkipDigestVerification bool

	// FindSuccessors finds the successors of the current node. If nil,
	// content.Successors is used.
	FindSuccessors func(ctx context.Context, fetcher content.Fetcher, desc ocispec.Descriptor) ([]ocispec.Descriptor, error)

	// CopyGraphOptions provides the standard copy callbacks and mount settings.
	// The promoted Concurrency, MaxMetadataBytes and FindSuccessors fields on
	// GraphTransactionOptions take precedence when set directly.
	CopyGraphOptions
}

// GraphNodeResult is the stable terminal detail for one graph node.
type GraphNodeResult struct {
	// Descriptor is the node descriptor.
	ocispec.Descriptor
	// State is the terminal node state.
	State GraphNodeState
	// Reused reports whether the node already existed in the target and passed
	// digest verification.
	Reused bool
}

// GraphNodeError describes a node and the operation that failed.
type GraphNodeError struct {
	// Descriptor is the node that was being processed.
	ocispec.Descriptor
	// Op is the transaction phase or underlying operation that failed.
	Op string
	// Err is the failure.
	Err error
}

// Error implements error.
func (e GraphNodeError) Error() string {
	if e.Op == "" {
		return fmt.Sprintf("%s: %v", e.Descriptor.Digest, e.Err)
	}
	return fmt.Sprintf("%s: %s: %v", e.Descriptor.Digest, e.Op, e.Err)
}

// Unwrap returns the underlying error.
func (e GraphNodeError) Unwrap() error {
	return e.Err
}

// GraphTransactionReport is the stable result of a graph transaction.
//
// The slice fields are sorted by digest and therefore remain stable across
// runs and goroutine scheduling.
type GraphTransactionReport struct {
	// Root is the transaction root descriptor.
	Root ocispec.Descriptor

	// Nodes contains every planned node in digest order.
	Nodes []GraphNodeResult

	// CompletedNodes contains descriptors for nodes that were copied or reused.
	CompletedNodes []ocispec.Descriptor
	// FailedNodes contains every node that reached a terminal failure state,
	// including nodes blocked by a failed dependency.
	FailedNodes []GraphNodeError
	// CanceledNodes contains nodes that did not finish because the context was
	// canceled or timed out.
	CanceledNodes []ocispec.Descriptor

	// Completed contains descriptors for all copied or reused nodes.
	Completed []ocispec.Descriptor
	// Skipped contains descriptors for reused target nodes.
	Skipped []ocispec.Descriptor
	// Failed contains direct and dependency failures in the same shape used by
	// VerifyGraphReport.
	Failed []FailedNode
	// Blocked contains nodes that were not pushed because a dependency failed.
	Blocked []GraphNodeError
}

// OK reports whether the transaction completed every node without failures or
// cancellation.
func (r *GraphTransactionReport) OK() bool {
	return r != nil && len(r.FailedNodes) == 0 && len(r.CanceledNodes) == 0
}

// GraphTransactionError wraps a partial graph transaction report.
type GraphTransactionError struct {
	// Report is the completed partial result.
	Report *GraphTransactionReport
}

// Error implements error.
func (e *GraphTransactionError) Error() string {
	if e == nil || e.Report == nil {
		return ErrGraphTransactionFailed.Error()
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d failed node(s)", ErrGraphTransactionFailed, len(e.Report.FailedNodes))
	for _, node := range e.Report.FailedNodes {
		b.WriteString("\n  - ")
		b.WriteString(node.Error())
	}
	return b.String()
}

// Unwrap returns the sentinel and all node errors so errors.Is and errors.As
// can inspect both the transaction and individual causes.
func (e *GraphTransactionError) Unwrap() []error {
	errs := []error{ErrGraphTransactionFailed}
	if e == nil || e.Report == nil {
		return errs
	}
	for _, node := range e.Report.FailedNodes {
		if node.Err != nil {
			errs = append(errs, node.Err)
		}
	}
	return errs
}

// GraphLimitError is returned when a traversal safety limit is exceeded.
type GraphLimitError struct {
	// Kind is either GraphLimitDepth or GraphLimitNodes.
	Kind string
	// Limit is the configured limit.
	Limit int
	// Value is the observed depth or node count.
	Value int
	// Descriptor is the node that caused the limit to be exceeded.
	Descriptor ocispec.Descriptor
}

// Error implements error.
func (e *GraphLimitError) Error() string {
	switch e.Kind {
	case GraphLimitDepth:
		return fmt.Sprintf("depth %d exceeds limit %d at %s", e.Value, e.Limit, e.Descriptor.Digest)
	case GraphLimitNodes:
		return fmt.Sprintf("node count at least %d exceeds limit %d at %s", e.Value, e.Limit, e.Descriptor.Digest)
	default:
		return fmt.Sprintf("graph limit %s exceeded at %s", e.Kind, e.Descriptor.Digest)
	}
}

// Unwrap returns the kind-specific limit sentinel.
func (e *GraphLimitError) Unwrap() error {
	if e.Kind == GraphLimitDepth {
		return ErrGraphDepthLimitExceeded
	}
	if e.Kind == GraphLimitNodes {
		return ErrGraphNodeLimitExceeded
	}
	return ErrGraphLimitExceeded
}

// GraphCycleError describes nodes that could not be completed because they
// participate in, or depend on, a dependency cycle among nodes that still need
// to be copied.
type GraphCycleError struct {
	// Nodes lists the node described by this error.
	Nodes []ocispec.Descriptor
}

// Error implements error.
func (e *GraphCycleError) Error() string {
	digests := make([]string, 0, len(e.Nodes))
	for _, node := range e.Nodes {
		digests = append(digests, node.Digest.String())
	}
	return fmt.Sprintf("%s: %s", ErrCyclicGraph, strings.Join(digests, " -> "))
}

// Unwrap returns ErrCyclicGraph.
func (e *GraphCycleError) Unwrap() error {
	return ErrCyclicGraph
}

// graphTransactionNode is a planned graph node.
type graphTransactionNode struct {
	desc       ocispec.Descriptor
	depth      int
	successors []digest.Digest
	parents    []digest.Digest
	reused     bool
	verifyErr  error
	preErr     error
	preErrOp   string
}

// graphTransactionExec is the mutable execution state for a node. Each field
// is written only by the node's own goroutine before done is closed. Other
// goroutines may read err/state after receiving from done.
type graphTransactionExec struct {
	node   *graphTransactionNode
	done   chan struct{}
	err    error
	op     string
	state  GraphNodeState
	reused bool
}

// GraphTransaction is a resumable graph copy transaction. A zero-value
// GraphTransaction is not valid; use NewGraphTransaction.
//
// The target itself is the durable resume record. Run or Retry verifies nodes
// already present in the target and skips those whose digest matches.
type GraphTransaction struct {
	src  content.ReadOnlyStorage
	dst  content.Storage
	root ocispec.Descriptor
	opts GraphTransactionOptions

	mu     sync.Mutex
	report *GraphTransactionReport
}

// NewGraphTransaction creates a resumable graph transaction.
func NewGraphTransaction(src content.ReadOnlyStorage, dst content.Storage, root ocispec.Descriptor, opts GraphTransactionOptions) *GraphTransaction {
	return &GraphTransaction{
		src:  src,
		dst:  dst,
		root: root,
		opts: opts,
	}
}

// Run executes the transaction.
func (tx *GraphTransaction) Run(ctx context.Context) (*GraphTransactionReport, error) {
	report, err := CopyGraphTransaction(ctx, tx.src, tx.dst, tx.root, tx.opts)
	tx.mu.Lock()
	tx.report = report
	tx.mu.Unlock()
	return report, err
}

// Retry executes the transaction again. Nodes that are already present in the
// target and pass digest verification are skipped.
func (tx *GraphTransaction) Retry(ctx context.Context) (*GraphTransactionReport, error) {
	return tx.Run(ctx)
}

// Report returns the latest report produced by Run or Retry, if any.
func (tx *GraphTransaction) Report() *GraphTransactionReport {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.report
}

// CopyGraphTransaction copies a rooted OCI graph to dst with all-or-nothing
// publication ordering for missing manifests.
//
// Traversal deduplicates digests and enforces opts.MaxDepth and
// opts.MaxNodes. A missing parent manifest is pushed only after every direct
// blocking successor has completed successfully. Independent branches continue
// when another branch fails. The returned report contains stable, digest-sorted
// details for completed and failed nodes. Calling the function again, or
// GraphTransaction.Retry, skips target nodes that exist and pass digest
// verification.
func CopyGraphTransaction(ctx context.Context, src content.ReadOnlyStorage, dst content.Storage, root ocispec.Descriptor, opts GraphTransactionOptions) (*GraphTransactionReport, error) {
	report := &GraphTransactionReport{Root: root}
	if src == nil {
		err := newCopyError("CopyGraphTransaction", CopyErrorOriginSource, ocispec.Descriptor{}, errors.New("nil source target"))
		return report, err
	}
	if dst == nil {
		err := newCopyError("CopyGraphTransaction", CopyErrorOriginDestination, ocispec.Descriptor{}, errors.New("nil destination target"))
		return report, err
	}
	if err := validateSuccessor(root); err != nil {
		return report, newCopyError("CopyGraphTransaction", CopyErrorOriginSource, root, err)
	}

	// Reconcile the direct traversal fields with the embedded standard copy
	// options. Direct fields win; otherwise values set through CopyGraphOptions
	// are preserved.
	if opts.Concurrency <= 0 {
		opts.Concurrency = opts.CopyGraphOptions.Concurrency
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = defaultConcurrency
	}
	opts.CopyGraphOptions.Concurrency = opts.Concurrency

	if opts.MaxMetadataBytes <= 0 {
		opts.MaxMetadataBytes = opts.CopyGraphOptions.MaxMetadataBytes
	}
	if opts.MaxMetadataBytes <= 0 {
		opts.MaxMetadataBytes = defaultCopyMaxMetadataBytes
	}
	opts.CopyGraphOptions.MaxMetadataBytes = opts.MaxMetadataBytes

	if opts.FindSuccessors == nil {
		opts.FindSuccessors = opts.CopyGraphOptions.FindSuccessors
	}
	if opts.FindSuccessors == nil {
		opts.FindSuccessors = content.Successors
	}
	opts.CopyGraphOptions.FindSuccessors = opts.FindSuccessors

	if opts.MaxDepth <= 0 && opts.Depth > 0 {
		opts.MaxDepth = opts.Depth
	}
	if opts.MaxDepth < 0 {
		opts.MaxDepth = 0
	}
	opts.Depth = opts.MaxDepth
	if opts.MaxNodes < 0 {
		opts.MaxNodes = 0
	}

	limiter := semaphore.NewWeighted(int64(opts.Concurrency))
	proxy := cas.NewProxyWithLimit(src, cas.NewMemory(), opts.MaxMetadataBytes)

	graph, err := discoverTransactionGraph(ctx, src, dst, root, opts, proxy, limiter)
	if err != nil {
		return report, err
	}

	r := &transactionRunner{
		ctx:     ctx,
		src:     src,
		dst:     dst,
		opts:    opts,
		proxy:   proxy,
		limiter: limiter,
		graph:   graph,
		exec:    make(map[digest.Digest]*graphTransactionExec, len(graph.nodes)),
	}
	report = r.run()
	if ctx.Err() != nil {
		return report, ctx.Err()
	}
	if !report.OK() {
		return report, &GraphTransactionError{Report: report}
	}
	return report, nil
}

// transactionRunner executes a planned graph.
type transactionRunner struct {
	ctx     context.Context
	src     content.ReadOnlyStorage
	dst     content.Storage
	opts    GraphTransactionOptions
	proxy   *cas.Proxy
	limiter *semaphore.Weighted
	graph   *transactionGraph
	exec    map[digest.Digest]*graphTransactionExec
	wg      sync.WaitGroup
}

type transactionGraph struct {
	root  ocispec.Descriptor
	nodes map[digest.Digest]*graphTransactionNode
}

type transactionDiscoveryResult struct {
	reused     bool
	verifyErr  error
	err        error
	successors []ocispec.Descriptor
}

// discoverTransactionGraph walks the graph breadth-first so depth is the
// shortest distance from the root. One level's successor lookups run
// concurrently.
func discoverTransactionGraph(
	ctx context.Context,
	src content.ReadOnlyStorage,
	dst content.Storage,
	root ocispec.Descriptor,
	opts GraphTransactionOptions,
	proxy *cas.Proxy,
	limiter *semaphore.Weighted,
) (*transactionGraph, error) {
	rootKey := root.Digest
	g := &transactionGraph{
		root: root,
		nodes: map[digest.Digest]*graphTransactionNode{
			rootKey: {desc: root, depth: 0},
		},
	}
	visited := map[digest.Digest]struct{}{rootKey: {}}
	frontier := []struct {
		desc  ocispec.Descriptor
		depth int
	}{{root, 0}}

	for len(frontier) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		indices := make([]int, len(frontier))
		for i := range indices {
			indices[i] = i
		}
		results := make([]transactionDiscoveryResult, len(frontier))
		err := syncutil.Go(ctx, limiter, func(ctx context.Context, _ *syncutil.LimitedRegion, i int) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			results[i] = discoverTransactionNode(ctx, src, dst, frontier[i].desc, opts, proxy)
			// Context cancellation is an abort, not a per-node partial result.
			if err := ctx.Err(); err != nil {
				return err
			}
			return nil
		}, indices...)
		if err != nil {
			return nil, context.Cause(ctx)
		}

		var next []struct {
			desc  ocispec.Descriptor
			depth int
		}
		for i, item := range frontier {
			result := results[i]
			node := g.nodes[item.desc.Digest]
			node.reused = result.reused
			node.verifyErr = result.verifyErr
			if result.err != nil {
				node.preErr = result.err
				node.preErrOp = "Traverse"
			}

			edgeSeen := make(map[digest.Digest]struct{}, len(result.successors))
			for _, successor := range result.successors {
				key := successor.Digest
				if _, ok := edgeSeen[key]; ok {
					continue
				}
				edgeSeen[key] = struct{}{}

				if _, seen := visited[key]; !seen {
					childDepth := item.depth + 1
					if opts.MaxDepth > 0 && childDepth > opts.MaxDepth {
						return nil, &GraphLimitError{
							Kind:       GraphLimitDepth,
							Limit:      opts.MaxDepth,
							Value:      childDepth,
							Descriptor: successor,
						}
					}
					if opts.MaxNodes > 0 && len(g.nodes) >= opts.MaxNodes {
						return nil, &GraphLimitError{
							Kind:       GraphLimitNodes,
							Limit:      opts.MaxNodes,
							Value:      len(g.nodes) + 1,
							Descriptor: successor,
						}
					}
					if err := validateSuccessor(successor); err != nil {
						wrapped := newCopyError("FindSuccessors", CopyErrorOriginSource, item.desc,
							fmt.Errorf("invalid successor descriptor: successor media type: %s; successor size: %d; successor digest: %s: %w",
								successor.MediaType, successor.Size, successor.Digest, err))
						node.preErr = errors.Join(node.preErr, wrapped)
						node.preErrOp = "FindSuccessors"
						continue
					}

					visited[key] = struct{}{}
					child := &graphTransactionNode{desc: successor, depth: childDepth}
					g.nodes[key] = child
					next = append(next, struct {
						desc  ocispec.Descriptor
						depth int
					}{successor, childDepth})
				}

				child := g.nodes[key]
				node.successors = append(node.successors, key)
				if child != nil {
					child.parents = append(child.parents, item.desc.Digest)
				}
			}
		}
		frontier = next
	}

	markTransactionCycles(g)
	return g, nil
}

// discoverTransactionNode checks target durability and finds successors for
// one node.
func discoverTransactionNode(
	ctx context.Context,
	src content.ReadOnlyStorage,
	dst content.Storage,
	desc ocispec.Descriptor,
	opts GraphTransactionOptions,
	proxy *cas.Proxy,
) transactionDiscoveryResult {
	result := transactionDiscoveryResult{}

	reused, verifyErr, err := existingTransactionNode(ctx, dst, desc, opts.SkipDigestVerification)
	result.reused = reused
	result.verifyErr = verifyErr
	if err != nil {
		result.err = errors.Join(result.err, err)
	}

	successors, err := opts.FindSuccessors(ctx, proxy, desc)
	if err != nil {
		wrapped := newCopyError("FindSuccessors", CopyErrorOriginSource, desc, err)
		result.err = errors.Join(result.err, wrapped)
		return result
	}
	successors = removeForeignLayers(successors)
	for _, successor := range successors {
		if err := validateSuccessor(successor); err != nil {
			wrapped := newCopyError("FindSuccessors", CopyErrorOriginSource, desc,
				fmt.Errorf("invalid successor descriptor: successor media type: %s; successor size: %d; successor digest: %s: %w",
					successor.MediaType, successor.Size, successor.Digest, err))
			result.err = errors.Join(result.err, wrapped)
			return result
		}
	}
	result.successors = successors
	return result
}

// existingTransactionNode reports whether desc may be skipped. A digest
// mismatch is returned as verifyErr so the caller can recopy; hard Exists or
// context errors are returned as err.
func existingTransactionNode(ctx context.Context, dst content.Storage, desc ocispec.Descriptor, skipVerification bool) (reused bool, verifyErr error, err error) {
	exists, err := dst.Exists(ctx, desc)
	if err != nil {
		return false, nil, newCopyError("Exists", CopyErrorOriginDestination, desc, err)
	}
	if !exists {
		return false, nil, nil
	}
	if skipVerification {
		return true, nil, nil
	}
	if err := content.Verify(ctx, dst, desc); err != nil {
		if ctx.Err() != nil {
			return false, nil, err
		}
		return false, fmt.Errorf("digest verification failed: %w", err), nil
	}
	return true, nil, nil
}

// markTransactionCycles marks nodes left without an acyclic dependency path.
// Terminal reused/pre-failed nodes do not participate in the wait graph.
func markTransactionCycles(g *transactionGraph) {
	active := make(map[digest.Digest]bool, len(g.nodes))
	remaining := make(map[digest.Digest]int, len(g.nodes))
	for key, node := range g.nodes {
		isActive := node.preErr == nil && !node.reused
		active[key] = isActive
		if !isActive {
			continue
		}
		for _, childKey := range node.successors {
			if child, ok := g.nodes[childKey]; ok && child.preErr == nil && !child.reused {
				remaining[key]++
			}
		}
	}

	var queue []digest.Digest
	for key := range g.nodes {
		if active[key] && remaining[key] == 0 {
			queue = append(queue, key)
		}
	}
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		for _, parentKey := range g.nodes[key].parents {
			if !active[parentKey] || remaining[parentKey] == 0 {
				continue
			}
			remaining[parentKey]--
			if remaining[parentKey] == 0 {
				queue = append(queue, parentKey)
			}
		}
	}

	cycleKeys := make([]digest.Digest, 0)
	for key := range g.nodes {
		if active[key] && remaining[key] > 0 {
			cycleKeys = append(cycleKeys, key)
		}
	}
	if len(cycleKeys) == 0 {
		return
	}
	sort.Slice(cycleKeys, func(i, j int) bool { return cycleKeys[i].String() < cycleKeys[j].String() })
	for _, key := range cycleKeys {
		node := g.nodes[key]
		node.preErr = &GraphCycleError{Nodes: []ocispec.Descriptor{node.desc}}
		node.preErrOp = "Traverse"
	}
}

// run starts one goroutine per planned node. Goroutines block on child
// completion channels without holding the concurrency semaphore.
func (r *transactionRunner) run() *GraphTransactionReport {
	for key, node := range r.graph.nodes {
		r.exec[key] = &graphTransactionExec{
			node: node,
			done: make(chan struct{}),
		}
	}

	r.wg.Add(len(r.exec))
	for key := range r.exec {
		go r.runNode(key)
	}
	r.wg.Wait()

	return r.snapshot()
}

// runNode is the per-node goroutine entry point.
func (r *transactionRunner) runNode(key digest.Digest) {
	ex := r.exec[key]
	defer r.wg.Done()
	defer close(ex.done)

	node := ex.node
	if node.preErr != nil {
		ex.state = GraphNodeStateFailed
		ex.err = node.preErr
		ex.op = node.preErrOp
		if ex.op == "" {
			ex.op = "Traverse"
		}
		return
	}
	if node.reused {
		if hook := r.opts.OnCopySkipped; hook != nil {
			if err := r.limiter.Acquire(r.ctx, 1); err != nil {
				ex.state = GraphNodeStateCanceled
				ex.err = err
				ex.op = "Concurrency"
				return
			}
			if err := hook(r.ctx, node.desc); err != nil {
				r.limiter.Release(1)
				ex.state = GraphNodeStateFailed
				ex.err = err
				ex.op = "OnCopySkipped"
				return
			}
			r.limiter.Release(1)
		}
		ex.state = GraphNodeStateSkipped
		ex.reused = true
		return
	}

	// Wait for every direct successor. The semaphore is not held here, so a
	// deep/wide parent does not consume a worker while blocked.
	for _, childKey := range node.successors {
		child, ok := r.exec[childKey]
		if !ok {
			err := fmt.Errorf("%w: missing planned successor %s", ErrNodeBlocked, childKey)
			ex.state = GraphNodeStateFailed
			ex.err = err
			ex.op = "Schedule"
			return
		}
		select {
		case <-child.done:
		case <-r.ctx.Done():
			ex.state = GraphNodeStateCanceled
			ex.err = r.ctx.Err()
			ex.op = "WaitDependencies"
			return
		}
	}
	if r.ctx.Err() != nil {
		ex.state = GraphNodeStateCanceled
		ex.err = r.ctx.Err()
		ex.op = "WaitDependencies"
		return
	}

	// Collect dependency failures deterministically.
	var dependencyFailures []GraphNodeError
	for _, childKey := range node.successors {
		child := r.exec[childKey]
		if child.err != nil {
			dependencyFailures = append(dependencyFailures, GraphNodeError{
				Descriptor: child.node.desc,
				Op:         child.op,
				Err:        child.err,
			})
		}
	}
	if len(dependencyFailures) > 0 {
		sort.Slice(dependencyFailures, func(i, j int) bool {
			return dependencyFailures[i].Descriptor.Digest.String() < dependencyFailures[j].Descriptor.Digest.String()
		})
		errs := make([]error, 0, len(dependencyFailures)+1)
		errs = append(errs, ErrNodeBlocked)
		for _, failure := range dependencyFailures {
			errs = append(errs, failure)
		}
		ex.state = GraphNodeStateFailed
		ex.err = errors.Join(errs...)
		ex.op = "WaitDependencies"
		return
	}

	if err := r.limiter.Acquire(r.ctx, 1); err != nil {
		ex.state = GraphNodeStateCanceled
		ex.err = err
		ex.op = "Concurrency"
		return
	}
	defer r.limiter.Release(1)

	if r.ctx.Err() != nil {
		ex.state = GraphNodeStateCanceled
		ex.err = r.ctx.Err()
		ex.op = "CopyNode"
		return
	}

	// Discovery verified target state, but check once more immediately before
	// pushing in case another transaction completed this node concurrently.
	exists, err := r.dst.Exists(r.ctx, node.desc)
	if err != nil {
		if r.ctx.Err() != nil {
			ex.state = GraphNodeStateCanceled
			ex.err = r.ctx.Err()
			ex.op = "Exists"
		} else {
			ex.state = GraphNodeStateFailed
			ex.err = newCopyError("Exists", CopyErrorOriginDestination, node.desc, err)
			ex.op = "Exists"
		}
		return
	}
	if exists && !r.opts.SkipDigestVerification {
		if err := content.Verify(r.ctx, r.dst, node.desc); err == nil {
			ex.state = GraphNodeStateSkipped
			ex.reused = true
			return
		} else if r.ctx.Err() != nil {
			ex.state = GraphNodeStateCanceled
			ex.err = r.ctx.Err()
			ex.op = "Verify"
			return
		}
	} else if exists && r.opts.SkipDigestVerification {
		ex.state = GraphNodeStateSkipped
		ex.reused = true
		return
	}

	if err := r.pushNode(node.desc); err != nil {
		if r.ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			ex.state = GraphNodeStateCanceled
		} else {
			ex.state = GraphNodeStateFailed
		}
		if node.verifyErr != nil {
			err = errors.Join(node.verifyErr, err)
		}
		ex.err = err
		ex.op = "CopyNode"
		return
	}

	ex.state = GraphNodeStateCompleted
}

// pushNode pushes from the metadata cache when available, otherwise from the
// source. The standard copy helpers invoke PreCopy/PostCopy/MountFrom hooks and
// tolerate ErrAlreadyExists for concurrent/retry races.
func (r *transactionRunner) pushNode(desc ocispec.Descriptor) error {
	opts := r.opts.CopyGraphOptions
	if exists, err := r.proxy.Cache.Exists(r.ctx, desc); err == nil && exists {
		return copyNode(r.ctx, r.proxy.Cache, r.dst, desc, opts)
	} else if err != nil && r.ctx.Err() != nil {
		return err
	}
	return mountOrCopyNode(r.ctx, r.src, r.dst, desc, opts)
}

// snapshot builds the stable public report.
func (r *transactionRunner) snapshot() *GraphTransactionReport {
	keys := make([]digest.Digest, 0, len(r.exec))
	for key := range r.exec {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

	report := &GraphTransactionReport{Root: r.graph.root}
	for _, key := range keys {
		ex := r.exec[key]
		result := GraphNodeResult{
			Descriptor: ex.node.desc,
			State:      ex.state,
			Reused:     ex.reused || ex.state == GraphNodeStateSkipped,
		}
		report.Nodes = append(report.Nodes, result)

		switch ex.state {
		case GraphNodeStateCompleted, GraphNodeStateSkipped:
			report.CompletedNodes = append(report.CompletedNodes, ex.node.desc)
			report.Completed = append(report.Completed, ex.node.desc)
			if ex.state == GraphNodeStateSkipped || result.Reused {
				report.Skipped = append(report.Skipped, ex.node.desc)
			}
		case GraphNodeStateFailed:
			nodeErr := GraphNodeError{Descriptor: ex.node.desc, Op: ex.op, Err: ex.err}
			report.FailedNodes = append(report.FailedNodes, nodeErr)
			report.Failed = append(report.Failed, FailedNode{Descriptor: ex.node.desc, Err: ex.err})
			if errors.Is(ex.err, ErrNodeBlocked) || errors.Is(ex.err, ErrCyclicGraph) {
				report.Blocked = append(report.Blocked, nodeErr)
			}
		case GraphNodeStateCanceled:
			report.CanceledNodes = append(report.CanceledNodes, ex.node.desc)
		}
	}
	return report
}
