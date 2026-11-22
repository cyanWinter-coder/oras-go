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

package file

import (
	"archive/tar"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// IgnoreFunc decides whether an entry is excluded from a packing snapshot.
// It is invoked with the slash-separated archive path of the entry, relative
// to the packed root ("." denotes the root itself), and the entry's file info.
// Returning true excludes the entry; when the entry is a directory, its whole
// subtree is excluded as well.
//
// The function is called while the snapshot is taken, before any layer content
// is written, so ignore decisions always apply to a single, frozen view of the
// tree. It must not modify the file system.
type IgnoreFunc func(path string, info fs.FileInfo) bool

// snapshotEntry is a single frozen entry of a packing snapshot. Everything
// needed to emit the tar header and to validate the entry again at write time
// is captured here, so the layer and its descriptor are produced from one
// consistent read instead of from independent directory walk and file reads.
type snapshotEntry struct {
	// path is the absolute path of the entry on disk.
	path string
	// name is the slash-separated archive path, prefixed with the layer name.
	name string
	// info is the file info captured by Lstat at snapshot time.
	info os.FileInfo
	// linkTarget is the symbolic link target, empty for non-symlinks.
	linkTarget string
	// header is the tar header derived from info and linkTarget. It is emitted
	// unchanged at write time, guaranteeing the header matches the snapshot.
	header *tar.Header
}

// snapshotBuilder accumulates frozen entries while rejecting archive paths
// that have already been recorded.
type snapshotBuilder struct {
	entries []snapshotEntry
	seen    map[string]struct{}
}

func newSnapshotBuilder() *snapshotBuilder {
	return &snapshotBuilder{seen: make(map[string]struct{})}
}

// add records entry. It returns ErrDuplicatePath when another entry already
// uses the same archive path, which can only be caused by the tree changing
// while it is being walked.
func (b *snapshotBuilder) add(entry snapshotEntry) error {
	if _, ok := b.seen[entry.name]; ok {
		return fmt.Errorf("%s: %w", entry.name, ErrDuplicatePath)
	}
	b.seen[entry.name] = struct{}{}
	b.entries = append(b.entries, entry)
	return nil
}

// takeSnapshot walks root and freezes the list of entries to pack. It applies
// the ignore rules, rejects symbolic links that escape root and duplicate
// archive paths, and captures the tar metadata of every retained entry. The
// returned snapshot is the single source of truth for the subsequent layer
// write: no directory entry is discovered or stat'd again outside the explicit
// re-validation performed against these frozen entries.
func takeSnapshot(ctx context.Context, root, prefix string, ignore IgnoreFunc) ([]snapshotEntry, error) {
	builder := newSnapshotBuilder()

	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			// A missing or unreadable entry means the tree changed while it was
			// being walked, or is not readable. Either way a consistent layer
			// cannot be produced.
			return walkErr
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(filepath.Join(prefix, rel))

		if ignore != nil && ignore(name, info) {
			if info.IsDir() && rel != "." {
				return filepath.SkipDir
			}
			return nil
		}

		var linkTarget string
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err = os.Readlink(path)
			if err != nil {
				return err
			}
			// Validate containment before writing anything, so a link that
			// points outside the packed root can never make it into the layer.
			if err := validateLinkWithinRoot(root, path, linkTarget); err != nil {
				return err
			}
		}

		header, err := tarHeaderFromInfo(info, linkTarget, name)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}

		return builder.add(snapshotEntry{
			path:       path,
			name:       name,
			info:       info,
			linkTarget: linkTarget,
			header:     header,
		})
	})
	if err != nil {
		return nil, err
	}
	return builder.entries, nil
}

// validateLinkWithinRoot verifies that the symbolic link at linkPath with
// target linkTarget cannot resolve outside root. An absolute target is allowed
// only when it stays inside root, matching the round-trip semantics tar already
// provides; a relative target is joined to the link's directory and rejected if
// the lexical result escapes root. Dangling links are allowed as long as their
// target stays inside root, matching the behavior of tar.
func validateLinkWithinRoot(root, linkPath, linkTarget string) error {
	resolved := linkTarget
	if !filepath.IsAbs(linkTarget) {
		resolved = filepath.Join(filepath.Dir(linkPath), linkTarget)
	}

	// Work on lexical, cleaned paths so dangling links can be validated without
	// requiring their target to exist.
	realRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	realTarget, err := filepath.Abs(resolved)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(realRoot, filepath.Clean(realTarget))
	if err != nil {
		return fmt.Errorf("symbolic link %s -> %s: %w", linkPath, linkTarget, ErrSymlinkEscapes)
	}
	rel = filepath.ToSlash(filepath.Clean(rel))
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return fmt.Errorf("symbolic link %s -> %s: %w", linkPath, linkTarget, ErrSymlinkEscapes)
	}
	return nil
}

// entryUnchanged reports whether the on-disk entry still matches the frozen
// snapshot. The mode, size and modification time are compared; inode identity is
// additionally compared on platforms that expose it. A mismatch means the entry
// changed after the snapshot was taken and packing must fail rather than emit a
// layer whose descriptor does not match its content.
func entryUnchanged(entry snapshotEntry, current fs.FileInfo) bool {
	if current.Mode() != entry.info.Mode() {
		return false
	}
	if current.Size() != entry.info.Size() {
		return false
	}
	if !current.ModTime().Equal(entry.info.ModTime()) {
		return false
	}
	return sameFile(entry.info, current)
}

// tarHeaderFromInfo builds the tar header for an entry from its frozen file
// info and, for symbolic links, its link target. The identity fields are reset
// so the archive does not depend on the host user or group.
func tarHeaderFromInfo(info os.FileInfo, linkTarget, name string) (*tar.Header, error) {
	header, err := tar.FileInfoHeader(info, linkTarget)
	if err != nil {
		return nil, err
	}
	header.Name = name
	header.Uid = 0
	header.Gid = 0
	header.Uname = ""
	header.Gname = ""
	return header, nil
}
