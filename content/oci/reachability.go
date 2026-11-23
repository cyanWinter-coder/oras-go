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

package oci

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"regexp"
	"strings"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/errdef"
	"github.com/oras-project/oras-go/v3/internal/container/set"
	"github.com/oras-project/oras-go/v3/internal/descriptor"
	"github.com/oras-project/oras-go/v3/internal/manifestutil"
	"github.com/oras-project/oras-go/v3/internal/resolver"
)

var (
	digestAlgorithmPattern = regexp.MustCompile(
		`^[a-z0-9]+([+._-][a-z0-9]+)*$`,
	)
	digestEncodedPattern = regexp.MustCompile(`^[a-f0-9]+$`)
)

var (
	// errMismatchedSize classifies a blob whose size differs from the size
	// that the descriptor referencing it declares.
	errMismatchedSize = errors.New("mismatched size")
	// errManifestCycle classifies manifests that reference one another in a
	// cycle, which the OCI image specification forbids: the graph of an
	// artifact is a DAG. Content addressing cannot produce a cycle on its
	// own, so one appears only in a layout whose blobs do not hash to their
	// paths.
	errManifestCycle = errors.New("manifest reference cycle")
)

// referenceEdge is a descriptor and the path of what references it.
type referenceEdge struct {
	desc ocispec.Descriptor
	from string
}

// verifyReachability records in acc the blobs that index.json references but
// that are absent (Missing), and, if every reference could be followed, the
// verified blobs that are not reachable (Unreferenced). A reference that cannot
// be followed is recorded as Unverifiable, with the reason.
//
// The two come from different traversals on purpose. The graph that
// gcReachable builds is neither "everything referenced" (it silently
// skips manifests that are not found) nor "everything on disk" (it never
// fetches blobs that are not manifests), so Missing is computed here as the
// referenced set minus present, which is what the sweep found on disk.
// Unreferenced uses gcReachable, the rule [Store.GC] uses.
//
// present holds the path of every entry the sweep found in a digest directory,
// and unlisted the algorithm directories that could not be listed, whose
// content is unknown rather than absent.
func verifyReachability(ctx context.Context, fsys fs.FS, index *ocispec.Index, present map[string]struct{}, unlisted map[string]error, acc *verifyAccumulator) error {
	storage := NewStorageFromFS(fsys)

	incomplete, err := walkIndexReferences(ctx, storage, index, fsys, present, unlisted, acc)
	if err != nil {
		return err
	}
	if incomplete {
		return nil
	}

	refMap, ok := resolveIndexReferences(ctx, index, present, unlisted, acc)
	if !ok {
		return nil
	}

	return reportUnreferenced(ctx, storage, refMap, acc)
}

// walkIndexReferences verifies the references reachable from index.json.
// It reports whether any reference could not be followed completely.
//
// The walk visits every descriptor once: nodes referenced more than once are
// verified on first encounter and skipped afterwards, which also terminates
// the walk on a reference cycle. Cycles do not go unnoticed, though: after
// the walk, reportManifestCycles records the manifests that form one.
func walkIndexReferences(ctx context.Context, storage content.Fetcher, index *ocispec.Index, fsys fs.FS, present map[string]struct{}, unlisted map[string]error, acc *verifyAccumulator) (bool, error) {
	incomplete := reportUnlistedDirectories(unlisted, acc)

	queue := make([]referenceEdge, 0, len(index.Manifests))
	for _, desc := range index.Manifests {
		queue = append(queue, referenceEdge{
			desc: desc,
			from: ocispec.ImageIndexFile,
		})
	}

	seen := set.New[digest.Digest]()
	for len(queue) > 0 {
		if err := isContextDone(ctx); err != nil {
			return incomplete, err
		}

		edge := queue[0]
		queue = queue[1:]

		desc := edge.desc
		if seen.Contains(desc.Digest) {
			continue
		}
		seen.Add(desc.Digest)

		p, alg, ok := blobPathOf(desc.Digest)
		if !ok {
			acc.addUnverifiable(
				edge.from,
				fmt.Errorf("invalid digest %q: %w", desc.Digest, errdef.ErrInvalidDigest),
			)
			incomplete = true
			continue
		}

		if _, unknown := unlisted[alg]; unknown {
			continue // already reported by the sweep
		}

		if _, exists := present[p]; !exists {
			acc.addMissing(p, fmt.Errorf("referenced by %s: %w", edge.from, errdef.ErrNotFound))
			continue
		}

		// A blob that already failed hash verification is not size-checked:
		// its content does not match its path digest, so its size proves
		// nothing further, and the path is already reported.
		if !acc.hasFailed(p) {
			if err := verifyReferenceSize(fsys, p, desc); err != nil {
				acc.addFailed(p, err)
			}
		}

		successors, err := verifiySuccessors(ctx, storage, desc, present)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return incomplete, err
			}

			acc.addUnverifiable(p, err)
			incomplete = true
			continue
		}

		for _, successor := range successors {
			queue = append(queue, referenceEdge{
				desc: successor,
				from: p,
			})
		}
	}

	if err := reportManifestCycles(ctx, fsys, index, present, acc); err != nil {
		return incomplete, err
	}
	return incomplete, nil
}

// verifyReferenceSize checks that the blob at p is as large as the descriptor
// referencing it declares. A matching path digest alone does not catch a
// descriptor whose size does not match the content: the blob can be intact
// while every read of it through the descriptor fails on the size check.
func verifyReferenceSize(fsys fs.FS, p string, desc ocispec.Descriptor) error {
	fi, err := fs.Stat(fsys, p)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if !fi.Mode().IsRegular() {
		// not a blob file; the sweep has already reported it
		return nil
	}
	if fi.Size() != desc.Size {
		return fmt.Errorf("size is %d, descriptor declares %d: %w", fi.Size(), desc.Size, errMismatchedSize)
	}
	return nil
}

// reportManifestCycles records the manifests of index whose references form a
// cycle, which the OCI image specification forbids: the graph of an artifact
// is a DAG. Content addressing cannot produce a cycle on its own, so one
// appears only in a layout whose blobs do not hash to their paths. The
// references of such a manifest are invisible to the verified walk, so the
// raw content of every present manifest reachable from index.json is read
// here, whether or not it hashes to its path digest, and the cycle is looked
// for in the graph those raw references form. The raw graph is used for
// nothing else: a cycle does not make reachability incomplete, since the
// verified walk has already visited every reachable node.
func reportManifestCycles(ctx context.Context, fsys fs.FS, index *ocispec.Index, present map[string]struct{}, acc *verifyAccumulator) error {
	raw := &rawReferenceGraph{
		fsys:       fsys,
		present:    present,
		successors: make(map[digest.Digest][]ocispec.Descriptor),
	}
	for _, desc := range index.Manifests {
		if err := raw.expand(ctx, desc); err != nil {
			return err
		}
	}

	const (
		unvisited = iota
		inStack
		done
	)
	state := make(map[digest.Digest]int, len(raw.successors))
	reported := set.New[digest.Digest]()

	// frame is one node of the depth-first search and the successor of it to
	// visit next.
	type frame struct {
		dgst digest.Digest
		next int
	}

	for _, desc := range index.Manifests {
		root := desc.Digest
		if _, ok := raw.successors[root]; !ok || state[root] != unvisited {
			continue
		}
		state[root] = inStack
		stack := []frame{{dgst: root}}
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			successors := raw.successors[top.dgst]
			if top.next >= len(successors) {
				state[top.dgst] = done
				stack = stack[:len(stack)-1]
				continue
			}
			next := successors[top.next].Digest
			top.next++
			if _, ok := raw.successors[next]; !ok {
				continue
			}
			switch state[next] {
			case unvisited:
				state[next] = inStack
				stack = append(stack, frame{dgst: next})
			case inStack:
				// a reference to a node on the stack closes a cycle
				if reported.Contains(next) {
					continue
				}
				reported.Add(next)
				var cycle []digest.Digest
				for i := len(stack) - 1; ; i-- {
					cycle = append([]digest.Digest{stack[i].dgst}, cycle...)
					if stack[i].dgst == next {
						break
					}
				}
				p, _, _ := blobPathOf(next)
				acc.addUnverifiable(p, cycleError(cycle))
			}
		}
	}
	return nil
}

// rawReferenceGraph discovers the references of present manifests by reading
// their content as found, whether or not it hashes to its path digest.
type rawReferenceGraph struct {
	fsys    fs.FS
	present map[string]struct{}
	// successors maps the digest a present manifest is referenced by to the
	// present manifests its content references. Every key is a node that has
	// been expanded; a node without present manifest successors maps to nil.
	successors map[digest.Digest][]ocispec.Descriptor
}

// expand reads the manifest desc refers to and records the manifests it
// references, recursively. Only descriptors that declare a manifest media
// type and are present are read; anything else is a leaf.
func (g *rawReferenceGraph) expand(ctx context.Context, desc ocispec.Descriptor) error {
	if err := isContextDone(ctx); err != nil {
		return err
	}
	if !descriptor.IsManifest(desc) {
		return nil
	}
	dgst := desc.Digest
	if _, ok := g.successors[dgst]; ok {
		return nil
	}
	p, _, ok := blobPathOf(dgst)
	if !ok {
		return nil
	}
	if _, ok := g.present[p]; !ok {
		return nil
	}
	g.successors[dgst] = nil // expanded, even if its content cannot be read

	data, err := fs.ReadFile(g.fsys, p)
	if err != nil {
		return nil
	}
	// Parse the content as what its descriptor declares, under the digest the
	// content actually has: a manifest that does not hash to its path still
	// gives up its references.
	actual := ocispec.Descriptor{
		MediaType: desc.MediaType,
		Digest:    digest.FromBytes(data),
		Size:      int64(len(data)),
	}
	fetcher := content.FetcherFunc(func(context.Context, ocispec.Descriptor) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	})
	successors, err := content.Successors(ctx, fetcher, actual)
	if err != nil {
		return nil
	}
	for _, successor := range successors {
		if !descriptor.IsManifest(successor) {
			continue
		}
		sp, _, ok := blobPathOf(successor.Digest)
		if !ok {
			continue
		}
		if _, ok := g.present[sp]; !ok {
			continue
		}
		g.successors[dgst] = append(g.successors[dgst], successor)
	}
	for _, successor := range g.successors[dgst] {
		if err := g.expand(ctx, successor); err != nil {
			return err
		}
	}
	return nil
}

// cycleError describes a reference cycle, naming the manifests on it in
// order, first and last the same.
func cycleError(cycle []digest.Digest) error {
	names := make([]string, 0, len(cycle)+1)
	for _, dgst := range cycle {
		names = append(names, dgst.String())
	}
	names = append(names, cycle[0].String())
	return fmt.Errorf("cycle %s: %w", strings.Join(names, " -> "), errManifestCycle)
}

// reportUnlistedDirectories records blob directories that could not be listed.
func reportUnlistedDirectories(unlisted map[string]error, acc *verifyAccumulator) bool {
	for alg, err := range unlisted {
		acc.addUnverifiable(
			path.Join(ocispec.ImageBlobsDir, alg),
			fmt.Errorf("reachability unknown: %w", err),
		)
	}
	return len(unlisted) > 0
}

// verifiySuccessors returns a manifest's outgoing references, excluding its
// subject when that subject is absent from the layout.
func verifiySuccessors(ctx context.Context, storage content.Fetcher, desc ocispec.Descriptor, present map[string]struct{}) ([]ocispec.Descriptor, error) {
	successors, err := content.Successors(ctx, storage, desc)
	if err != nil {
		return nil, err
	}

	subject, err := manifestutil.Subject(ctx, storage, desc)
	if err != nil {
		return nil, err
	}

	if subject != nil &&
		len(successors) > 0 &&
		successors[0].Digest == subject.Digest {
		p, _, _ := blobPathOf(subject.Digest)
		if _, exists := present[p]; !exists {
			successors = successors[1:]
		}
	}

	return successors, nil
}

// resolveIndexReferences resolves index references using the same rules as GC.
// Missing untagged entries are excluded because GC cannot retain them.
func resolveIndexReferences(ctx context.Context, index *ocispec.Index, present map[string]struct{}, unlisted map[string]error, acc *verifyAccumulator) (map[string]ocispec.Descriptor, bool) {
	tagResolver := resolver.NewMemory()
	if _, err := tagIndex(ctx, index, tagResolver); err != nil {
		acc.addUnverifiable(ocispec.ImageIndexFile, err)
		return nil, false
	}

	refMap := tagResolver.Map()
	for ref, desc := range refMap {
		if ref != desc.Digest.String() {
			continue
		}

		p, alg, ok := blobPathOf(desc.Digest)
		if !ok {
			continue
		}
		if _, unknown := unlisted[alg]; unknown {
			continue
		}
		if _, exists := present[p]; !exists {
			delete(refMap, ref)
		}
	}
	return refMap, true
}

// reportUnreferenced reports verified blobs that are not reachable from the
// references retained by GC.
func reportUnreferenced(ctx context.Context, storage content.Fetcher, refMap map[string]ocispec.Descriptor, acc *verifyAccumulator) error {
	_, graph, err := gcReachable(ctx, storage, refMap)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		acc.addUnverifiable(
			ocispec.ImageIndexFile,
			fmt.Errorf("compute reachability: %w", err),
		)
		return nil
	}

	reachable := graph.DigestSet()
	for _, entry := range acc.report().Verified {
		dgst := digest.NewDigestFromEncoded(
			digest.Algorithm(path.Base(path.Dir(entry.Path))),
			path.Base(entry.Path),
		)
		if !reachable.Contains(dgst) {
			acc.addUnreferenced(entry.Path)
		}
	}

	return nil
}

// blobPathOf returns the path in a layout of the blob that dgst names, and its
// algorithm directory, without requiring that the algorithm is supported.
// It reports false for a malformed digest.
func blobPathOf(dgst digest.Digest) (p, alg string, ok bool) {
	alg, encoded, found := strings.Cut(string(dgst), ":")
	if !found ||
		!digestAlgorithmPattern.MatchString(alg) ||
		!digestEncodedPattern.MatchString(encoded) {
		return "", "", false
	}

	algorithm := digest.Algorithm(alg)
	if algorithm.Available() && algorithm.Validate(encoded) != nil {
		return "", "", false
	}

	return path.Join(ocispec.ImageBlobsDir, alg, encoded), alg, true
}
