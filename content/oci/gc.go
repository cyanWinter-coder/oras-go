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
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/errdef"
	"github.com/oras-project/oras-go/v3/internal/container/set"
	"github.com/oras-project/oras-go/v3/internal/descriptor"
	"github.com/oras-project/oras-go/v3/internal/graph"
	"github.com/oras-project/oras-go/v3/internal/resolver"
)

const (
	// gcStagingDirName is the name of the directory, in the root of the
	// layout, that holds content removed from blobs/ by a garbage collection
	// that has not been committed yet.
	gcStagingDirName = "gc-staging"
	// gcCommitMarkerName is the name of the file, inside a staging directory,
	// that records that the removal of the staged content is committed. A
	// staging directory without it belongs to a collection that failed or was
	// interrupted before its commit, and its content must become visible
	// again.
	gcCommitMarkerName = "committed"
)

// osRemove and osRename are the file operations of a collection, overridable in
// tests so that failures can be injected without depending on permissions.
var (
	osRemove = os.Remove
	osRename = os.Rename
)

// gcSnapshot is a read-only view of the references and content of a Store at
// one generation.
//
// Every reachability decision of one garbage collection, including the recheck
// before the commit, is made against a single snapshot: a tag, the manifests it
// names, and their configs and layers all come from the same generation
// instead of from the store at different moments. Content is immutable and
// addressed by digest, so only the references have to be frozen, not the
// bytes.
type gcSnapshot struct {
	// refs is the frozen reference map of the store, as returned by
	// [resolver.Memory.Map], and is the reference state the rebuilt metadata
	// is derived from.
	refs map[string]ocispec.Descriptor
	// fetcher reads the content named by refs, and by the references index.json
	// records, so both are read from the same generation.
	fetcher content.Fetcher
	// nodes is the graph of the same generation as refs. It is only read, to
	// recover the descriptor of an unreachable blob that is being spared.
	nodes *graph.Memory
	// protected holds the content reachable from the references recorded in
	// index.json when the snapshot was taken. Another Store on the same
	// directory may have recorded a tag there without this Store seeing it in
	// refs, so that content is kept as well.
	protected set.Set[digest.Digest]
}

// takeGCSnapshot freezes the references, the graph, and the references recorded
// in index.json. The caller must hold the exclusive lock of the store.
func (s *Store) takeGCSnapshot(ctx context.Context) *gcSnapshot {
	return &gcSnapshot{
		refs:      s.tagResolver.Map(),
		fetcher:   s.storage,
		nodes:     s.graph,
		protected: s.reachableFromIndexFile(ctx),
	}
}

// reachableFromIndexFile returns the content that index.json references
// through tags, directly or through a manifest, index, or subject chain. A
// plain digest entry is not a reference: index.json lists every manifest by
// digest, including the ones that are otherwise unreferenced, and treating
// those as references would keep all garbage.
//
// This is an additional protection on top of the Store's own references, so a
// missing or unreadable index yields whatever could be determined rather than
// failing the collection; the pre-commit recheck treats an unreadable index
// conservatively and restores the staged content.
func (s *Store) reachableFromIndexFile(ctx context.Context) set.Set[digest.Digest] {
	reachable := set.New[digest.Digest]()
	index, err := readIndexFile(os.DirFS(s.root))
	if err != nil {
		return reachable
	}
	tagResolver := resolver.NewMemory()
	if _, err := tagIndex(ctx, index, tagResolver); err != nil {
		return reachable
	}
	if _, g, err := gcReachable(ctx, s.storage, tagResolver.Map()); err == nil {
		for dgst := range g.DigestSet() {
			reachable.Add(dgst)
		}
	}
	return reachable
}

// gcPlan is the outcome of inspecting a layout against one snapshot: the
// content that may be removed, and the unreachable manifests that are too
// recent to remove and must stay indexed.
type gcPlan struct {
	remove []digest.Digest
	spared []ocispec.Descriptor
}

// planGC inspects blobs/ against snap and records what may be removed. It does
// not modify the store's files. newGraph is the metadata rebuilt for snap; the
// descriptors of spared manifests are indexed into it so that Predecessors
// still sees manifests pushed before GC.
func (s *Store) planGC(ctx context.Context, snap *gcSnapshot, newGraph *graph.Memory) (*gcPlan, error) {
	// The reachable set is computed once, from the snapshot, and is not
	// recomputed from the live store while the plan is being built.
	reachable := newGraph.DigestSet()
	stale := olderThan(leftoverExpiry)

	rootpath := filepath.Join(s.root, ocispec.ImageBlobsDir)
	algDirs, err := os.ReadDir(rootpath)
	if err != nil {
		return nil, err
	}

	plan := &gcPlan{}
	removePaths := make(map[digest.Digest]string)
	var sparedManifests []ocispec.Descriptor
	for _, algDir := range algDirs {
		if !algDir.IsDir() {
			continue
		}
		alg := algDir.Name()
		// skip unsupported directories
		if !descriptor.IsSupportedAlgorithm(digest.Algorithm(alg)) {
			continue
		}
		algPath := filepath.Join(rootpath, alg)
		digestEntries, err := os.ReadDir(algPath)
		if err != nil {
			return nil, err
		}
		for _, digestEntry := range digestEntries {
			if err := isContextDone(ctx); err != nil {
				return nil, err
			}
			if digestEntry.IsDir() {
				continue
			}
			encoded := digestEntry.Name()
			blobDigest := digest.NewDigestFromEncoded(digest.Algorithm(alg), encoded)
			if err := blobDigest.Validate(); err != nil {
				// skip irrelevant content
				continue
			}
			if reachable.Contains(blobDigest) || snap.protected.Contains(blobDigest) {
				continue
			}
			if stale(digestEntry) {
				// the blob is unreachable in the snapshot, so it can be
				// removed once the spared manifests are known
				removePaths[blobDigest] = filepath.Join(algPath, encoded)
				continue
			}
			// A recent unreachable blob may belong to a write or a multi-step
			// copy that has not recorded its references yet. Keep it, and keep
			// its descriptor so a later Resolve does not fall back to an
			// untyped blob descriptor.
			desc, ok := snap.nodes.Node(blobDigest)
			if !ok {
				continue
			}
			if err := newGraph.Index(ctx, snap.fetcher, desc); err != nil {
				return nil, err
			}
			if descriptor.IsManifest(desc) {
				sparedManifests = append(sparedManifests, desc)
			}
		}
	}

	// A spared manifest is kept on its own modification time, but a blob it
	// references is judged on that blob's: a copy that finds a layer already on
	// disk skips pushing it, so the layer can be stale while the manifest is
	// not. Keep it too, or tagging the manifest later leaves content missing.
	if len(removePaths) > 0 && len(sparedManifests) > 0 {
		referenced := graph.NewMemory()
		if err := referenced.IndexAll(ctx, snap.fetcher, sparedManifests...); err != nil {
			return nil, err
		}
		for dgst := range referenced.DigestSet() {
			delete(removePaths, dgst)
		}
	}

	// Order the removal deterministically, so that reproducing a failure and
	// comparing two plans do not depend on map iteration order.
	remove := make([]digest.Digest, 0, len(removePaths))
	for dgst := range removePaths {
		remove = append(remove, dgst)
	}
	slices.Sort(remove)
	plan.remove = remove
	plan.spared = sparedManifests
	return plan, nil
}

// stagedBlob is one blob that has been moved out of blobs/ and whose removal
// has not been committed yet.
type stagedBlob struct {
	digest digest.Digest
	src    string // path in blobs/, where the blob becomes visible again
	dst    string // path in the staging directory
}

// gcStaging moves blobs out of blobs/ into a generation directory before they
// are removed. A staged blob is invisible to Fetch, Exists, and Resolve, but a
// failed collection can restore it by moving it back, and an interrupted one
// is restored, or its removal finished, when the store is opened again.
//
// A gcStaging is not safe for concurrent use; the store lock serializes it.
type gcStaging struct {
	root  string // the root of the OCI layout
	dir   string // the generation directory, inside gcStagingDirName
	blobs []stagedBlob
}

// stageGC moves the content named by digests into a new staging directory. It
// returns nil if there is nothing to stage. On error, every blob moved so far
// has already been restored.
func (s *Store) stageGC(ctx context.Context, digests []digest.Digest) (*gcStaging, error) {
	if len(digests) == 0 {
		return nil, nil
	}

	stagingRoot := filepath.Join(s.root, gcStagingDirName)
	if err := ensureDir(stagingRoot); err != nil {
		return nil, fmt.Errorf("unable to create garbage collection staging directory: %w", err)
	}
	dir, err := os.MkdirTemp(stagingRoot, "gen-")
	if err != nil {
		return nil, fmt.Errorf("unable to create garbage collection generation: %w", err)
	}
	staged := &gcStaging{root: s.root, dir: dir}
	for _, dgst := range digests {
		if err := isContextDone(ctx); err != nil {
			return nil, errors.Join(err, staged.restoreAll())
		}
		if _, err := staged.stage(dgst); err != nil {
			return nil, errors.Join(err, staged.restoreAll())
		}
	}
	return staged, nil
}

// stage moves one blob out of blobs/. It reports whether the blob was still
// present and has been moved.
func (g *gcStaging) stage(dgst digest.Digest) (bool, error) {
	p, err := blobPath(dgst)
	if err != nil {
		return false, fmt.Errorf("%s: %w", dgst, errdef.ErrInvalidDigest)
	}
	src := filepath.Join(g.root, p)
	dst := filepath.Join(g.dir, p)
	if err := ensureDir(filepath.Dir(dst)); err != nil {
		return false, fmt.Errorf("unable to stage %s: %w", src, err)
	}
	if fi, err := os.Stat(src); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// another collection, or a concurrent writer, already handled it
			return false, nil
		}
		return false, fmt.Errorf("unable to stage %s: %w", src, err)
	} else if !fi.Mode().IsRegular() {
		// a digest-named directory is not a blob; planGC skips it, and a
		// concurrent writer must not make this collection move it
		return false, nil
	}
	if err := osRename(src, dst); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("unable to remove %s: %w", src, err)
	}
	g.blobs = append(g.blobs, stagedBlob{digest: dgst, src: src, dst: dst})
	return true, nil
}

// restoreAll puts every staged blob back where it was. It is idempotent, and
// a blob that another writer has already recreated is left to the writer.
func (g *gcStaging) restoreAll() error {
	if g == nil {
		return nil
	}
	var errs []error
	for i := range g.blobs {
		if err := g.restore(i); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		if err := os.RemoveAll(g.dir); err != nil {
			errs = append(errs, fmt.Errorf("unable to remove garbage collection staging: %w", err))
		}
		g.removeStagingRoot()
	}
	return errors.Join(errs...)
}

// restoreDigests restores the staged blobs named in keep, so content that
// became referenced while the collection was in progress stays visible.
func (g *gcStaging) restoreDigests(keep set.Set[digest.Digest]) error {
	if g == nil || len(keep) == 0 {
		return nil
	}
	var errs []error
	for i := range g.blobs {
		if !keep.Contains(g.blobs[i].digest) {
			continue
		}
		if err := g.restore(i); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// restore moves one staged blob back to its path in blobs/. Content is
// addressable by digest, so an existing file with the same digest is
// equivalent and the staged copy is dropped.
func (g *gcStaging) restore(i int) error {
	b := &g.blobs[i]
	if b.src == "" {
		// already restored
		return nil
	}
	if _, err := os.Stat(b.src); err == nil {
		// a concurrent writer recreated the blob; content is addressed by
		// digest, so the staged copy is redundant
		if err := osRemove(b.dst); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("unable to drop staged copy of %s: %w", b.src, err)
		}
		b.src = ""
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("unable to restore %s: %w", b.src, err)
	}
	if err := ensureDir(filepath.Dir(b.src)); err != nil {
		return fmt.Errorf("unable to restore %s: %w", b.src, err)
	}
	if err := osRename(b.dst, b.src); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// the staged file is gone, so the blob has already been restored
			b.src = ""
			return nil
		}
		return fmt.Errorf("unable to restore %s: %w", b.src, err)
	}
	b.src = ""
	return nil
}

// commit marks the removal as committed, making it safe for an interrupted
// collection to finish the removal instead of restoring the content.
func (g *gcStaging) commit() error {
	if g == nil {
		return nil
	}
	marker := filepath.Join(g.dir, gcCommitMarkerName)
	if err := writeFileAtomic(marker, nil, false); err != nil {
		return fmt.Errorf("unable to commit garbage collection: %w", err)
	}
	return nil
}

// remove deletes the staged content. Once commit succeeded, a removal that
// cannot complete is finished by the recovery of a later collection.
func (g *gcStaging) remove() error {
	if g == nil {
		return nil
	}
	var errs []error
	for _, b := range g.blobs {
		if err := osRemove(b.dst); err != nil && !errors.Is(err, fs.ErrNotExist) {
			// Name the blob by its path in blobs/, which is where a caller that
			// put it there knows it from.
			name := b.src
			if name == "" {
				name = b.dst
			}
			errs = append(errs, fmt.Errorf("unable to remove %s: %w", name, err))
		}
	}
	if len(errs) > 0 {
		// the commit marker stays, so the next collection finishes the removal
		return errors.Join(errs...)
	}
	if err := os.RemoveAll(g.dir); err != nil {
		// the commit marker stays, so the next collection finishes the removal
		return fmt.Errorf("unable to remove garbage collection staging: %w", err)
	}
	g.removeStagingRoot()
	return nil
}

// removeStagingRoot removes the staging root once its last generation is gone.
// A failure only leaves an empty directory behind, which the next recovery
// removes.
func (g *gcStaging) removeStagingRoot() {
	_ = os.Remove(filepath.Join(g.root, gcStagingDirName))
}

// fetch reads a staged blob, so that reachability can be computed over content
// that the collection has moved out of blobs/.
func (g *gcStaging) fetch(desc ocispec.Descriptor) (io.ReadCloser, error) {
	p, err := blobPath(desc.Digest)
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", desc.Digest, desc.MediaType, errdef.ErrInvalidDigest)
	}
	fp, err := os.Open(filepath.Join(g.dir, p))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%s: %s: %w", desc.Digest, desc.MediaType, errdef.ErrNotFound)
		}
		return nil, err
	}
	return fp, nil
}

// stagedFetcher reads from the live layout first and from the staging
// directory second.
type stagedFetcher struct {
	live   content.Fetcher
	staged *gcStaging
}

// Fetch fetches the content identified by the descriptor.
func (f *stagedFetcher) Fetch(ctx context.Context, desc ocispec.Descriptor) (io.ReadCloser, error) {
	rc, err := f.live.Fetch(ctx, desc)
	if err == nil {
		return rc, nil
	}
	if !errors.Is(err, errdef.ErrNotFound) || f.staged == nil {
		return nil, err
	}
	return f.staged.fetch(desc)
}

// gcRecheckReferenced reads the references that index.json records now,
// including the ones another Store on the same directory may have written
// since the snapshot was taken, and returns the staged content that is
// reachable from them together with the descriptors index.json records for it.
//
// The snapshot's fetcher and the staging directory together are a coherent
// generation: every staged blob can still be read, so a reference that points
// at content this collection removed is recognized rather than reported as
// missing.
func (s *Store) gcRecheckReferenced(ctx context.Context, snap *gcSnapshot, staged *gcStaging) (set.Set[digest.Digest], map[digest.Digest]ocispec.Descriptor, error) {
	spare := set.New[digest.Digest]()
	descs := make(map[digest.Digest]ocispec.Descriptor)
	if staged == nil || len(staged.blobs) == 0 {
		return spare, descs, nil
	}

	index, err := readIndexFile(os.DirFS(s.root))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return spare, descs, nil
		}
		// Do not remove content when the current references cannot be read:
		// the staged blobs stay restorable.
		return nil, nil, fmt.Errorf("unable to re-read index before removal: %w", err)
	}

	tagResolver := resolver.NewMemory()
	plains, err := tagIndex(ctx, index, tagResolver)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to re-read index before removal: %w", err)
	}

	fetcher := &stagedFetcher{live: snap.fetcher, staged: staged}
	_, currentGraph, err := gcReachable(ctx, fetcher, tagResolver.Map())
	if err != nil {
		return nil, nil, fmt.Errorf("unable to recheck references before removal: %w", err)
	}
	reachable := currentGraph.DigestSet()
	for _, b := range staged.blobs {
		if reachable.Contains(b.digest) {
			spare.Add(b.digest)
		}
	}
	// A manifest that index.json records, but whose content cannot be
	// traversed (for example because it is corrupt), still protects itself, so
	// the collection never removes digest entries it cannot account for. This
	// applies only to a manifest that the snapshot already spared as recent:
	// garbage entries are spared as digests and stay removable. Plain digest
	// entries are included, since index.json lists every manifest by digest;
	// keeping their blobs is limited by what the snapshot already spared.
	for _, desc := range plains {
		if !spare.Contains(desc.Digest) {
			continue
		}
		descs[desc.Digest] = desc
	}
	return spare, descs, nil
}

// recoverGCStaging restores the content of collections that failed or were
// interrupted before committing, and finishes the removal of the ones that
// had committed. It is idempotent, so a recovery that is itself interrupted is
// completed by the next call.
func recoverGCStaging(root string) error {
	stagingRoot := filepath.Join(root, gcStagingDirName)
	entries, err := os.ReadDir(stagingRoot)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("unable to read garbage collection staging directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(stagingRoot, entry.Name())
		if _, err := os.Stat(filepath.Join(dir, gcCommitMarkerName)); err == nil {
			// the removal was committed; finish it
			if err := os.RemoveAll(dir); err != nil {
				return fmt.Errorf("unable to finish committed garbage collection: %w", err)
			}
			continue
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("unable to inspect garbage collection staging: %w", err)
		}
		if err := restoreGCStagingDir(root, dir); err != nil {
			return err
		}
	}
	// remove the staging root if it became empty; a failure only leaves an
	// empty directory behind
	_ = os.Remove(stagingRoot)
	return nil
}

// restoreGCStagingDir restores the blobs of one uncommitted generation
// directory and removes the directory.
func restoreGCStagingDir(root, dir string) error {
	blobsDir := filepath.Join(dir, ocispec.ImageBlobsDir)
	err := filepath.WalkDir(blobsDir, func(p string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		src := filepath.Join(root, rel)
		if _, err := os.Stat(src); err == nil {
			// a concurrent writer recreated the blob; content is addressed by
			// digest, so the staged copy is redundant
			return os.Remove(p)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("unable to restore %s: %w", src, err)
		}
		if err := ensureDir(filepath.Dir(src)); err != nil {
			return fmt.Errorf("unable to restore %s: %w", src, err)
		}
		if err := osRename(p, src); err != nil {
			return fmt.Errorf("unable to restore %s: %w", src, err)
		}
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("unable to restore interrupted garbage collection: %w", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("unable to remove garbage collection staging: %w", err)
	}
	return nil
}
