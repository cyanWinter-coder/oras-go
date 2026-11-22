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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/errdef"
)

// pushUnreferencedImage pushes an image no tag references and backdates its
// blobs, so that the next collection removes them.
func pushUnreferencedImage(t *testing.T, s *Store, name string) ocispec.Descriptor {
	t.Helper()
	descs, blobs := newImage(t, name, 2)
	for i, desc := range descs {
		if err := s.Push(context.Background(), desc, bytes.NewReader(blobs[i])); err != nil {
			t.Fatalf("Push(%s) error = %v", name, err)
		}
	}
	return descs[len(descs)-1]
}

// blobFile returns the path of a blob in blobs/ of root.
func blobFile(root string, dgst digest.Digest) string {
	return filepath.Join(root, ocispec.ImageBlobsDir, dgst.Algorithm().String(), dgst.Encoded())
}

// failCommitWrites makes the write of the commit marker fail while the
// returned function is called with true. It restores fileWrite when the test
// ends.
func failCommitWrites(t *testing.T) (setFailing func(bool)) {
	t.Helper()

	var failing bool
	original := fileWrite
	fileWrite = func(f *os.File, data []byte) (int, error) {
		if failing && isTempFileOf(filepath.Base(f.Name()), gcCommitMarkerName) {
			return 0, errors.New("no space left on device")
		}
		return original(f, data)
	}
	t.Cleanup(func() { fileWrite = original })
	return func(v bool) { failing = v }
}

// TestStore_GC_CommitFailureRestoresVisibility checks that a collection that
// cannot commit its removal puts the staged content back, so a failed GC leaves
// the store as it was and can be retried.
func TestStore_GC_CommitFailureRestoresVisibility(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal("New() error =", err)
	}
	storeWithTaggedManifest(t, root)

	garbage := pushUnreferencedImage(t, s, "garbage")
	backdateBlobs(t, root)

	setFailing := failCommitWrites(t)
	setFailing(true)
	err = s.GC(ctx)
	setFailing(false)
	if err == nil {
		t.Fatal("GC() succeeded despite the failing commit")
	}

	// The content the collection had staged must be visible again.
	if exists, err := s.Exists(ctx, garbage); err != nil || !exists {
		t.Fatalf("Exists(staged-by-failed-GC) = %v, %v, want true, nil", exists, err)
	}
	rc, err := s.Fetch(ctx, garbage)
	if err != nil {
		t.Fatal("Fetch() error =", err)
	}
	rc.Close()

	// No staging directory is left behind.
	assertNoGCStaging(t, root)

	// A retry succeeds and removes the garbage.
	backdateBlobs(t, root)
	if err := s.GC(ctx); err != nil {
		t.Fatal("retried GC() error =", err)
	}
	if exists, err := s.Exists(ctx, garbage); err != nil || exists {
		t.Errorf("Exists(garbage) after retried GC = %v, %v, want false, nil", exists, err)
	}
	assertNoGCStaging(t, root)
}

// TestStore_GC_SnapshotKeepsContentInIndexFile checks that content that
// index.json still references through a tag is kept even when this Store's
// in-memory view no longer knows about that tag, as happens when another Store
// on the same directory writes the tag.
func TestStore_GC_SnapshotKeepsContentInIndexFile(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal("New() error =", err)
	}

	descs, blobs := newImage(t, "concurrent", 2)
	for i, desc := range descs {
		if err := s.Push(ctx, desc, bytes.NewReader(blobs[i])); err != nil {
			t.Fatal("Push() error =", err)
		}
	}
	manifest := descs[len(descs)-1]
	if err := s.Tag(ctx, manifest, "keep"); err != nil {
		t.Fatal("Tag() error =", err)
	}
	if err := s.SaveIndex(); err != nil {
		t.Fatal("SaveIndex() error =", err)
	}

	// Simulate a stale in-memory view: index.json still names "keep", but this
	// Store no longer sees the tag. A collection must not remove the content.
	s.tagResolver.Untag("keep")
	backdateBlobs(t, root)

	if err := s.GC(ctx); err != nil {
		t.Fatal("GC() error =", err)
	}
	for _, desc := range descs {
		if exists, err := s.Exists(ctx, desc); err != nil || !exists {
			t.Errorf("Exists(content %s referenced by index.json) = %v, %v, want true, nil", desc.Digest, exists, err)
		}
	}
}

// TestStore_GC_RechecksReferencesBeforeCommit checks that content that becomes
// referenced by index.json during a collection -- as another Store on the same
// directory would re-reference it -- is restored instead of removed.
func TestStore_GC_RechecksReferencesBeforeCommit(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal("New() error =", err)
	}

	descs, blobs := newImage(t, "re-referenced", 2)
	for i, desc := range descs {
		if err := s.Push(ctx, desc, bytes.NewReader(blobs[i])); err != nil {
			t.Fatal("Push() error =", err)
		}
	}
	manifest := descs[len(descs)-1]

	// Stage the whole image as a collection would.
	digests := make([]digest.Digest, len(descs))
	for i, desc := range descs {
		digests[i] = desc.Digest
	}
	staged, err := s.stageGC(ctx, digests)
	if err != nil {
		t.Fatal("stageGC() error =", err)
	}
	if staged == nil {
		t.Fatal("stageGC() staged nothing")
	}
	if exists, err := s.Exists(ctx, manifest); err != nil || exists {
		t.Fatalf("Exists(staged manifest) = %v, %v, want false, nil", exists, err)
	}

	// Another Store writes a tag for the staged manifest into index.json.
	index, err := readIndexFile(os.DirFS(root))
	if err != nil {
		t.Fatal(err)
	}
	ref := manifest
	ref.Annotations = map[string]string{ocispec.AnnotationRefName: "race"}
	index.Manifests = append(index.Manifests, ref)
	data, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.indexPath, data, 0666); err != nil {
		t.Fatal(err)
	}

	keep, _, err := s.gcRecheckReferenced(ctx, s.takeGCSnapshot(ctx), staged)
	if err != nil {
		t.Fatal("gcRecheckReferenced() error =", err)
	}
	// The whole image, including the config and layer that are only reachable
	// through the re-referenced manifest, must be recognized.
	for _, desc := range descs {
		if !keep.Contains(desc.Digest) {
			t.Errorf("re-referenced content %s was not recognized as reachable", desc.Digest)
		}
	}
	if err := staged.restoreDigests(keep); err != nil {
		t.Fatal("restoreDigests() error =", err)
	}
	for _, desc := range descs {
		if exists, err := s.Exists(ctx, desc); err != nil || !exists {
			t.Errorf("Exists(restored %s) = %v, %v, want true, nil", desc.Digest, exists, err)
		}
	}
	// The staged generation is empty now; removing it leaves no trace.
	if err := staged.remove(); err != nil {
		t.Fatal("remove() error =", err)
	}
	assertNoGCStaging(t, root)
}

// TestStore_GC_RemovalFailureFinishesOnRecovery checks that a committed removal
// that cannot complete fails the collection, keeps the removed content out of
// reach, and is finished by recovery.
func TestStore_GC_RemovalFailureFinishesOnRecovery(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal("New() error =", err)
	}
	storeWithTaggedManifest(t, root)
	garbage := pushUnreferencedImage(t, s, "unremovable")
	backdateBlobs(t, root)

	// Fail every file removal of a staged blob, so the committed removal
	// cannot complete and its marker stays for recovery. The hook keeps the
	// blob's own path in its message, so the error the collection reports can
	// name the blob.
	original := osRemove
	osRemove = func(p string) error {
		if strings.Contains(p, gcStagingDirName) {
			return fmt.Errorf("device busy: %s", p)
		}
		return original(p)
	}
	t.Cleanup(func() { osRemove = original })

	err = s.GC(ctx)
	if err == nil {
		t.Fatal("GC() error = nil, want error")
	}
	if !strings.Contains(err.Error(), garbage.Digest.Encoded()) {
		t.Errorf("GC() error = %v, want it to mention %s", err, garbage.Digest.Encoded())
	}
	// The removal was committed, so the content stays out of reach.
	if exists, err := s.Exists(ctx, garbage); err != nil || exists {
		t.Errorf("Exists(garbage) = %v, %v, want false, nil", exists, err)
	}

	// The next recovery finishes the committed removal.
	osRemove = original
	if err := recoverGCStaging(root); err != nil {
		t.Fatal("recoverGCStaging() error =", err)
	}
	assertNoGCStaging(t, root)
}

// TestRecoverGCStaging_UncommittedRestores checks that a collection interrupted
// before committing is rolled back when the store is opened again.
func TestRecoverGCStaging_UncommittedRestores(t *testing.T) {
	root := t.TempDir()
	s := storeWithTaggedManifest(t, root)
	ctx := context.Background()

	victim := content.NewDescriptorFromBytes(ocispec.MediaTypeImageConfig, []byte("config"))
	if _, err := os.Stat(blobFile(root, victim.Digest)); err != nil {
		t.Fatal("victim is not on disk:", err)
	}
	stageFileForTest(t, root, "gen-interrupted", victim, false)

	if _, err := New(root); err != nil {
		t.Fatal("New() error =", err)
	}
	if exists, err := s.Exists(ctx, victim); err != nil || !exists {
		t.Errorf("Exists(uncommitted staged blob) = %v, %v, want true, nil", exists, err)
	}
	assertNoGCStaging(t, root)
}

// TestRecoverGCStaging_CommittedFinishesRemoval checks that a collection
// interrupted after committing has its removal finished when the store is
// opened again.
func TestRecoverGCStaging_CommittedFinishesRemoval(t *testing.T) {
	root := t.TempDir()
	s := storeWithTaggedManifest(t, root)

	victim := content.NewDescriptorFromBytes(ocispec.MediaTypeImageConfig, []byte("config"))
	stageFileForTest(t, root, "gen-committed", victim, true)

	if _, err := New(root); err != nil {
		t.Fatal("New() error =", err)
	}
	if exists, err := s.Exists(context.Background(), victim); err != nil || exists {
		t.Errorf("Exists(committed staged blob) = %v, %v, want false, nil", exists, err)
	}
	assertNoGCStaging(t, root)
}

// TestRecoverGCStaging_Idempotent checks that recovery can be repeated, which is
// what a collection interrupted during its own recovery leaves to the next.
func TestRecoverGCStaging_Idempotent(t *testing.T) {
	root := t.TempDir()
	s := storeWithTaggedManifest(t, root)
	ctx := context.Background()

	victim := content.NewDescriptorFromBytes(ocispec.MediaTypeImageConfig, []byte("config"))
	stageFileForTest(t, root, "gen-interrupted", victim, false)

	for i := range 3 {
		if err := recoverGCStaging(root); err != nil {
			t.Fatalf("recoverGCStaging() #%d error = %v", i, err)
		}
		if exists, err := s.Exists(ctx, victim); err != nil || !exists {
			t.Fatalf("Exists() after recovery #%d = %v, %v, want true, nil", i, exists, err)
		}
		assertNoGCStaging(t, root)
	}
}

// TestStore_GC_IsIdempotent checks that running a collection repeatedly leaves
// no staging state and keeps reporting the same content.
func TestStore_GC_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal("New() error =", err)
	}
	storeWithTaggedManifest(t, root)
	garbage := pushUnreferencedImage(t, s, "garbage")
	backdateBlobs(t, root)

	for i := range 3 {
		if err := s.GC(ctx); err != nil {
			t.Fatalf("GC() #%d error = %v", i, err)
		}
		if exists, err := s.Exists(ctx, garbage); err != nil || exists {
			t.Fatalf("Exists(garbage) after GC #%d = %v, %v, want false, nil", i, exists, err)
		}
		assertNoGCStaging(t, root)
	}
}

// TestStore_GC_RecoversBeforePlanning checks that a collection first completes
// the recovery of an interrupted one, so a stale staging directory cannot hide
// content the current index references.
func TestStore_GC_RecoversBeforePlanning(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s := storeWithTaggedManifest(t, root)

	victim := content.NewDescriptorFromBytes(ocispec.MediaTypeImageConfig, []byte("config"))
	stageFileForTest(t, root, "gen-interrupted", victim, false)

	if err := s.GC(ctx); err != nil {
		t.Fatal("GC() error =", err)
	}
	if exists, err := s.Exists(ctx, victim); err != nil || !exists {
		t.Errorf("Exists(tagged content recovered by GC) = %v, %v, want true, nil", exists, err)
	}
	assertNoGCStaging(t, root)
}

// TestStore_GC_StagedReads verifies that the staged fetcher can read content
// the live layout no longer holds, which lets the recheck follow a reference to
// staged content.
func TestStore_GC_StagedReads(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal("New() error =", err)
	}
	descs, blobs := newImage(t, "staged", 2)
	for i, desc := range descs {
		if err := s.Push(ctx, desc, bytes.NewReader(blobs[i])); err != nil {
			t.Fatal("Push() error =", err)
		}
	}
	manifest := descs[len(descs)-1]
	staged, err := s.stageGC(ctx, []digest.Digest{manifest.Digest})
	if err != nil {
		t.Fatal("stageGC() error =", err)
	}
	fetcher := &stagedFetcher{live: s.storage, staged: staged}
	rc, err := fetcher.Fetch(ctx, manifest)
	if err != nil {
		t.Fatal("Fetch(staged manifest) error =", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, blobs[len(blobs)-1]) {
		t.Errorf("Fetch(staged manifest) = %q, want %q", got, blobs[len(blobs)-1])
	}

	// A descriptor that is neither live nor staged is not found.
	missing := content.NewDescriptorFromBytes(ocispec.MediaTypeImageLayer, []byte("missing"))
	if _, err := fetcher.Fetch(ctx, missing); !errors.Is(err, errdef.ErrNotFound) {
		t.Errorf("Fetch(missing) error = %v, want %v", err, errdef.ErrNotFound)
	}

	// Restoring is idempotent.
	if err := staged.restoreAll(); err != nil {
		t.Fatal("restoreAll() error =", err)
	}
	if err := staged.restoreAll(); err != nil {
		t.Fatal("second restoreAll() error =", err)
	}
	if exists, err := s.Exists(ctx, manifest); err != nil || !exists {
		t.Errorf("Exists(restored) = %v, %v, want true, nil", exists, err)
	}
}

// TestStore_GC_RestoresContentReferencedDuringCollection exercises the whole
// collection: content that another Store tags in index.json while the
// collection is staging it must be moved back and stay resolvable.
func TestStore_GC_RestoresContentReferencedDuringCollection(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal("New() error =", err)
	}
	storeWithTaggedManifest(t, root)

	descs, blobs := newImage(t, "raced", 2)
	for i, desc := range descs {
		if err := s.Push(ctx, desc, bytes.NewReader(blobs[i])); err != nil {
			t.Fatal("Push() error =", err)
		}
	}
	manifest := descs[len(descs)-1]
	backdateBlobs(t, root)

	// A concurrent writer tags the last staged blob in index.json, right after
	// it has been moved out of blobs/.
	original := osRename
	osRename = func(src, dst string) error {
		if err := original(src, dst); err != nil {
			return err
		}
		if filepath.Base(dst) != manifest.Digest.Encoded() {
			return nil
		}
		index, err := readIndexFile(os.DirFS(root))
		if err != nil {
			return err
		}
		ref := manifest
		ref.Annotations = map[string]string{ocispec.AnnotationRefName: "raced"}
		index.Manifests = append(index.Manifests, ref)
		data, err := json.Marshal(index)
		if err != nil {
			return err
		}
		return os.WriteFile(s.indexPath, data, 0666)
	}
	t.Cleanup(func() { osRename = original })

	if err := s.GC(ctx); err != nil {
		t.Fatal("GC() error =", err)
	}

	// Every blob of the re-referenced image is visible again.
	for _, desc := range descs {
		if exists, err := s.Exists(ctx, desc); err != nil || !exists {
			t.Errorf("Exists(re-referenced %s) = %v, %v, want true, nil", desc.Digest, exists, err)
		}
	}
	// The restored manifest keeps its digest entry, and a store reopened on the
	// saved layout can resolve it. The tag written by the other Store is
	// dropped, as GC documents: it overwrites index.json with this Store's
	// references.
	if got, err := s.Resolve(ctx, manifest.Digest.String()); err != nil || got.Digest != manifest.Digest {
		t.Errorf("Resolve(restored digest) = %v, %v, want %s", got.Digest, err, manifest.Digest)
	}
	reopened, err := New(root)
	if err != nil {
		t.Fatal("New() error =", err)
	}
	if got, err := reopened.Resolve(ctx, manifest.Digest.String()); err != nil || got.Digest != manifest.Digest {
		t.Errorf("Resolve(restored digest) after reopen = %v, %v, want %s", got.Digest, err, manifest.Digest)
	}
	assertNoGCStaging(t, root)
}

// TestStore_GC_UnreadableIndexBeforeCommitKeepsContent checks that a
// collection does not remove anything when index.json cannot be re-read, since
// it cannot tell whether the staged content is referenced again.
func TestStore_GC_UnreadableIndexBeforeCommitKeepsContent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal("New() error =", err)
	}

	descs, blobs := newImage(t, "unreadable", 2)
	for i, desc := range descs {
		if err := s.Push(ctx, desc, bytes.NewReader(blobs[i])); err != nil {
			t.Fatal("Push() error =", err)
		}
	}
	manifest := descs[len(descs)-1]
	digests := make([]digest.Digest, len(descs))
	for i, desc := range descs {
		digests[i] = desc.Digest
	}
	staged, err := s.stageGC(ctx, digests)
	if err != nil {
		t.Fatal("stageGC() error =", err)
	}
	if staged == nil {
		t.Fatal("stageGC() staged nothing")
	}

	if err := os.WriteFile(s.indexPath, []byte("{"), 0666); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.gcRecheckReferenced(ctx, s.takeGCSnapshot(ctx), staged); err == nil {
		t.Fatal("gcRecheckReferenced() error = nil, want a read error")
	}
	// The staged content stays restorable.
	if err := staged.restoreAll(); err != nil {
		t.Fatal("restoreAll() error =", err)
	}
	if exists, err := s.Exists(ctx, manifest); err != nil || !exists {
		t.Errorf("Exists(restored %s) = %v, %v, want true, nil", manifest.Digest, exists, err)
	}
}

// TestGCStaging_RestoreAlreadyRecreated checks that restoring a blob another
// writer has already recreated drops the staged copy rather than replacing the
// writer's file.
func TestGCStaging_RestoreAlreadyRecreated(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal("New() error =", err)
	}
	descs, blobs := newImage(t, "recreated", 2)
	for i, desc := range descs {
		if err := s.Push(ctx, desc, bytes.NewReader(blobs[i])); err != nil {
			t.Fatal("Push() error =", err)
		}
	}
	layer := descs[len(descs)-2]
	staged, err := s.stageGC(ctx, []digest.Digest{layer.Digest})
	if err != nil {
		t.Fatal("stageGC() error =", err)
	}

	// Another writer recreates the blob at its live path.
	if err := os.WriteFile(blobFile(root, layer.Digest), blobs[0], 0666); err != nil {
		t.Fatal(err)
	}
	if err := staged.restoreAll(); err != nil {
		t.Fatal("restoreAll() error =", err)
	}
	assertNoGCStaging(t, root)
	if exists, err := s.Exists(ctx, layer); err != nil || !exists {
		t.Errorf("Exists(recreated) = %v, %v, want true, nil", exists, err)
	}
}

// TestStore_GC_StageFailureRestoresEverything checks that a collection whose
// staging fails puts back the blobs it had already staged and names the blob it
// could not stage.
func TestStore_GC_StageFailureRestoresEverything(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal("New() error =", err)
	}
	storeWithTaggedManifest(t, root)
	garbage := pushUnreferencedImage(t, s, "garbage")
	backdateBlobs(t, root)

	original := osRename
	osRename = func(src, dst string) error {
		if filepath.Base(src) == garbage.Digest.Encoded() {
			return errors.New("device busy")
		}
		return original(src, dst)
	}
	t.Cleanup(func() { osRename = original })

	err = s.GC(ctx)
	if err == nil {
		t.Fatal("GC() error = nil, want error")
	}
	if !strings.Contains(err.Error(), garbage.Digest.Encoded()) {
		t.Errorf("GC() error = %v, want it to mention %s", err, garbage.Digest.Encoded())
	}
	// Everything that was staged before the failure is visible again.
	for _, desc := range mustImageDescs(t, s, garbage) {
		if exists, err := s.Exists(ctx, desc); err != nil || !exists {
			t.Errorf("Exists(restored %s) = %v, %v, want true, nil", desc.Digest, exists, err)
		}
	}
	assertNoGCStaging(t, root)
}

// TestRecoverGCStaging_RecreatedBlobIsNotOverwritten checks that recovery keeps
// a blob another writer has already recreated and drops the staged copy.
func TestRecoverGCStaging_RecreatedBlobIsNotOverwritten(t *testing.T) {
	root := t.TempDir()
	storeWithTaggedManifest(t, root)

	victim := content.NewDescriptorFromBytes(ocispec.MediaTypeImageConfig, []byte("config"))
	stageFileForTest(t, root, "gen-interrupted", victim, false)
	// Another writer recreates the blob at its live path.
	recreated := []byte("recreated by another writer")
	if err := os.WriteFile(blobFile(root, victim.Digest), recreated, 0666); err != nil {
		t.Fatal(err)
	}

	if err := recoverGCStaging(root); err != nil {
		t.Fatal("recoverGCStaging() error =", err)
	}
	data, err := os.ReadFile(blobFile(root, victim.Digest))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, recreated) {
		t.Errorf("blob content = %q, want the recreated %q", data, recreated)
	}
	assertNoGCStaging(t, root)
}

// mustImageDescs returns the descriptors of the config, layer, and manifest of
// an image whose manifest is desc.
func mustImageDescs(t *testing.T, s *Store, manifest ocispec.Descriptor) []ocispec.Descriptor {
	t.Helper()
	rc, err := s.Fetch(context.Background(), manifest)
	if err != nil {
		t.Fatal("Fetch(manifest) error =", err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	var m ocispec.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return append([]ocispec.Descriptor{m.Config}, append(m.Layers, manifest)...)
}

// TestRecoverGCStaging_EmptyGeneration checks that a generation directory left
// without content, as an interrupted staging can leave, is removed rather than
// failing recovery.
func TestRecoverGCStaging_EmptyGeneration(t *testing.T) {
	root := t.TempDir()
	if _, err := New(root); err != nil {
		t.Fatal("New() error =", err)
	}
	dir := filepath.Join(root, gcStagingDirName, "gen-empty")
	if err := os.MkdirAll(dir, 0777); err != nil {
		t.Fatal(err)
	}

	if err := recoverGCStaging(root); err != nil {
		t.Fatal("recoverGCStaging() error =", err)
	}
	assertNoGCStaging(t, root)
}

// TestStore_GC_EmptyRemovalLeavesNoStaging checks that a collection with
// nothing to remove neither stages nor leaves a directory behind.
func TestStore_GC_EmptyRemovalLeavesNoStaging(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s := storeWithTaggedManifest(t, root)

	if err := s.GC(ctx); err != nil {
		t.Fatal("GC() error =", err)
	}
	assertNoGCStaging(t, root)
	if exists, err := s.Exists(ctx, mustBlobDesc(t, s)); err != nil || !exists {
		t.Errorf("Exists(tagged content) = %v, %v, want true, nil", exists, err)
	}
}

// mustBlobDesc returns the descriptor of the manifest tagged "latest".
func mustBlobDesc(t *testing.T, s *Store) ocispec.Descriptor {
	t.Helper()
	desc, err := s.Resolve(context.Background(), "latest")
	if err != nil {
		t.Fatal("Resolve(latest) error =", err)
	}
	return desc
}

// stageFileForTest moves a blob into a staging generation directory as an
// interrupted collection leaves it, optionally committing the removal.
func stageFileForTest(t *testing.T, root, generation string, desc ocispec.Descriptor, committed bool) {
	t.Helper()
	rel := filepath.Join(ocispec.ImageBlobsDir, desc.Digest.Algorithm().String(), desc.Digest.Encoded())
	src := filepath.Join(root, rel)
	dst := filepath.Join(root, gcStagingDirName, generation, rel)
	if err := os.MkdirAll(filepath.Dir(dst), 0777); err != nil {
		t.Fatal(err)
	}
	if err := osRename(src, dst); err != nil {
		t.Fatal(err)
	}
	if committed {
		marker := filepath.Join(root, gcStagingDirName, generation, gcCommitMarkerName)
		if err := os.WriteFile(marker, nil, 0666); err != nil {
			t.Fatal(err)
		}
	}
}

// assertNoGCStaging fails if a staging directory is left behind.
func assertNoGCStaging(t *testing.T, root string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(root, gcStagingDirName)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("garbage collection staging directory remains: %v", err)
	}
}
