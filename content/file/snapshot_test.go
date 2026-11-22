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
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// snapshotTree builds a small tree under a fresh temporary directory and
// returns the tree root.
func snapshotTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "sub", "deep"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("alpha"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "b.txt"), []byte("bravo"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "deep", "c.txt"), []byte("charlie"), 0644); err != nil {
		t.Fatal(err)
	}
	return root
}

func Test_takeSnapshot_freezesEntries(t *testing.T) {
	root := snapshotTree(t)
	const prefix = "layer"

	entries, err := takeSnapshot(context.Background(), root, prefix, nil)
	if err != nil {
		t.Fatalf("takeSnapshot() error = %v", err)
	}

	want := []string{
		"layer",
		"layer/a.txt",
		"layer/sub",
		"layer/sub/b.txt",
		"layer/sub/deep",
		"layer/sub/deep/c.txt",
	}
	if len(entries) != len(want) {
		t.Fatalf("got %d entries, want %d: %+v", len(entries), len(want), entries)
	}
	for i, wantName := range want {
		if entries[i].name != wantName {
			t.Errorf("entry %d name = %q, want %q", i, entries[i].name, wantName)
		}
		if entries[i].header == nil || entries[i].header.Name != wantName {
			t.Errorf("entry %d header not derived from the snapshot", i)
		}
		if entries[i].path == "" {
			t.Errorf("entry %d has empty disk path", i)
		}
	}
}

func Test_takeSnapshot_ignoreRules(t *testing.T) {
	root := snapshotTree(t)

	t.Run("file rule skips a single entry", func(t *testing.T) {
		ignore := func(path string, _ os.FileInfo) bool {
			return path == "layer/a.txt"
		}
		entries, err := takeSnapshot(context.Background(), root, "layer", ignore)
		if err != nil {
			t.Fatalf("takeSnapshot() error = %v", err)
		}
		for _, entry := range entries {
			if entry.name == "layer/a.txt" {
				t.Fatal("ignored file was retained")
			}
		}
	})

	t.Run("directory rule skips the whole subtree", func(t *testing.T) {
		ignore := func(path string, info os.FileInfo) bool {
			return info.IsDir() && path == "layer/sub"
		}
		entries, err := takeSnapshot(context.Background(), root, "layer", ignore)
		if err != nil {
			t.Fatalf("takeSnapshot() error = %v", err)
		}
		for _, entry := range entries {
			if filepath.Clean(entry.name) == filepath.Clean("layer/sub") ||
				strings.HasPrefix(entry.name, "layer/sub/") {
				t.Fatalf("ignored subtree entry was retained: %s", entry.name)
			}
		}
		if len(entries) != 2 { // root and a.txt
			t.Fatalf("got %d entries, want 2", len(entries))
		}
	})

	t.Run("nil rule keeps every entry", func(t *testing.T) {
		entries, err := takeSnapshot(context.Background(), root, "layer", nil)
		if err != nil {
			t.Fatalf("takeSnapshot() error = %v", err)
		}
		if len(entries) != 6 {
			t.Fatalf("got %d entries, want 6", len(entries))
		}
	})
}

func Test_takeSnapshot_contextCancelled(t *testing.T) {
	root := snapshotTree(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := takeSnapshot(ctx, root, "layer", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("takeSnapshot() error = %v, want %v", err, context.Canceled)
	}
}

func Test_snapshotBuilder_add_duplicatePath(t *testing.T) {
	builder := newSnapshotBuilder()
	entry := snapshotEntry{name: "layer/a.txt"}
	if err := builder.add(entry); err != nil {
		t.Fatalf("add() first error = %v", err)
	}
	err := builder.add(entry)
	if !errors.Is(err, ErrDuplicatePath) {
		t.Fatalf("add() second error = %v, want %v", err, ErrDuplicatePath)
	}
}

func Test_validateLinkWithinRoot(t *testing.T) {
	root := filepath.Clean(t.TempDir())
	link := filepath.Join(root, "dir", "link")

	tests := []struct {
		name    string
		target  string
		wantErr bool
	}{
		{name: "relative link inside root", target: "../a.txt"},
		{name: "relative link to current dir", target: "."},
		{name: "relative link traverses out", target: "../../etc/passwd", wantErr: true},
		{name: "absolute link inside root", target: filepath.Join(root, "a.txt")},
		{name: "absolute link outside root", target: filepath.Join(filepath.Dir(root), "outside"), wantErr: true},
		{name: "dangling link inside root", target: "missing.txt"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLinkWithinRoot(root, link, tt.target)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateLinkWithinRoot() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, ErrSymlinkEscapes) {
				t.Fatalf("error = %v, want ErrSymlinkEscapes", err)
			}
		})
	}
}

func Test_entryUnchanged(t *testing.T) {
	root := snapshotTree(t)
	entries, err := takeSnapshot(context.Background(), root, "layer", nil)
	if err != nil {
		t.Fatal(err)
	}
	var fileEntry snapshotEntry
	for _, entry := range entries {
		if entry.name == "layer/a.txt" {
			fileEntry = entry
		}
	}
	if fileEntry.name == "" {
		t.Fatal("a.txt not found in snapshot")
	}

	current, err := os.Lstat(fileEntry.path)
	if err != nil {
		t.Fatal(err)
	}
	if !entryUnchanged(fileEntry, current) {
		t.Error("entryUnchanged() = false for an untouched file")
	}

	if err := os.WriteFile(fileEntry.path, []byte("different content"), 0644); err != nil {
		t.Fatal(err)
	}
	changed, err := os.Lstat(fileEntry.path)
	if err != nil {
		t.Fatal(err)
	}
	if entryUnchanged(fileEntry, changed) {
		t.Error("entryUnchanged() = true for a modified file")
	}
}

func Test_validateSnapshotEntry_missingFile(t *testing.T) {
	root := snapshotTree(t)
	entries, err := takeSnapshot(context.Background(), root, "layer", nil)
	if err != nil {
		t.Fatal(err)
	}
	var fileEntry snapshotEntry
	for _, entry := range entries {
		if entry.name == "layer/a.txt" {
			fileEntry = entry
		}
	}
	if err := os.Remove(fileEntry.path); err != nil {
		t.Fatal(err)
	}
	if err := validateSnapshotEntry(fileEntry); !errors.Is(err, ErrFileChanged) {
		t.Fatalf("validateSnapshotEntry() error = %v, want %v", err, ErrFileChanged)
	}
}

func Test_writeTarSnapshot_emitsFrozenContent(t *testing.T) {
	root := snapshotTree(t)
	entries, err := takeSnapshot(context.Background(), root, "layer", nil)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := writeTarSnapshot(context.Background(), entries, &buf, nil, nil); err != nil {
		t.Fatalf("writeTarSnapshot() error = %v", err)
	}

	tr := tar.NewReader(&buf)
	got := make(map[string]string)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		content, _ := io.ReadAll(tr)
		got[header.Name] = string(content)
	}
	if got["layer/a.txt"] != "alpha" {
		t.Errorf("a.txt content = %q, want %q", got["layer/a.txt"], "alpha")
	}
	if got["layer/sub/b.txt"] != "bravo" {
		t.Errorf("b.txt content = %q, want %q", got["layer/sub/b.txt"], "bravo")
	}
	if _, ok := got["layer"]; !ok {
		t.Error("root directory entry missing")
	}
}

func Test_writeTarSnapshot_fileChanged(t *testing.T) {
	root := snapshotTree(t)
	entries, err := takeSnapshot(context.Background(), root, "layer", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Change every regular file after the snapshot but before the write. The
	// write must fail rather than emit bytes that do not match the frozen size
	// and metadata.
	for _, entry := range entries {
		if entry.info.Mode().IsRegular() {
			if err := os.WriteFile(entry.path, []byte("changed after snapshot"), 0644); err != nil {
				t.Fatal(err)
			}
		}
	}

	var buf bytes.Buffer
	err = writeTarSnapshot(context.Background(), entries, &buf, nil, nil)
	if !errors.Is(err, ErrFileChanged) {
		t.Fatalf("writeTarSnapshot() error = %v, want %v", err, ErrFileChanged)
	}
	// The store-level test verifies that a failed pack never publishes a layer;
	// the in-memory writer here only needs to have stopped at the change.
}

func Test_writeTarSnapshot_contextCancelled(t *testing.T) {
	root := snapshotTree(t)
	entries, err := takeSnapshot(context.Background(), root, "layer", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf bytes.Buffer
	if err := writeTarSnapshot(ctx, entries, &buf, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("writeTarSnapshot() error = %v, want %v", err, context.Canceled)
	}
}

func Test_writeTarSnapshot_appliesModTime(t *testing.T) {
	root := snapshotTree(t)
	entries, err := takeSnapshot(context.Background(), root, "layer", nil)
	if err != nil {
		t.Fatal(err)
	}
	clamp := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	var buf bytes.Buffer
	if err := writeTarSnapshot(context.Background(), entries, &buf, &clamp, nil); err != nil {
		t.Fatalf("writeTarSnapshot() error = %v", err)
	}

	tr := tar.NewReader(&buf)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.ModTime.After(clamp) {
			t.Errorf("%s ModTime = %v, not clamped to %v", header.Name, header.ModTime, clamp)
		}
	}
}

// TestStore_Dir_Add_ignoreRules exercises the full Add path: ignored entries
// must never reach the packed layer, and the descriptor must match the layer
// actually produced.
func TestStore_Dir_Add_ignoreRules(t *testing.T) {
	tempDir := t.TempDir()
	dirPath := filepath.Join(tempDir, "tree")
	if err := os.MkdirAll(filepath.Join(dirPath, "skip"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirPath, "keep.txt"), []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirPath, "skip", "gone.txt"), []byte("gone"), 0644); err != nil {
		t.Fatal(err)
	}

	s, err := New(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.IgnoreRules = func(path string, _ os.FileInfo) bool {
		return path == "tree/skip" || strings.HasPrefix(path, "tree/skip/")
	}

	desc, err := s.Add(context.Background(), "tree", "", dirPath)
	if err != nil {
		t.Fatalf("Store.Add() error = %v", err)
	}

	rc, err := s.Fetch(context.Background(), desc)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	gzr, err := gzip.NewReader(rc)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(header.Name, "tree/skip") {
			t.Errorf("ignored entry %q was packed into the layer", header.Name)
		}
	}
}
