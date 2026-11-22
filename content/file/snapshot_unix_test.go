//go:build unix

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
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func Test_takeSnapshot_symlinkInsideRoot(t *testing.T) {
	root := snapshotTree(t)
	if err := os.Symlink("a.txt", filepath.Join(root, "link.txt")); err != nil {
		t.Skip("symlinks not available:", err)
	}
	// absolute target, still inside the root
	if err := os.Symlink(filepath.Join(root, "a.txt"), filepath.Join(root, "abs.txt")); err != nil {
		t.Fatal(err)
	}

	entries, err := takeSnapshot(context.Background(), root, "layer", nil)
	if err != nil {
		t.Fatalf("takeSnapshot() error = %v", err)
	}
	links := 0
	for _, entry := range entries {
		if entry.header.Typeflag == tar.TypeSymlink {
			links++
			if entry.linkTarget == "" {
				t.Errorf("symlink %s has no recorded target", entry.name)
			}
		}
	}
	if links != 2 {
		t.Errorf("got %d symlink entries, want 2", links)
	}
}

func Test_takeSnapshot_symlinkEscapesRoot(t *testing.T) {
	root := snapshotTree(t)
	outside := filepath.Join(filepath.Dir(root), "outside-secret")
	if err := os.WriteFile(outside, []byte("secret"), 0644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(outside) })

	tests := []struct {
		name   string
		target string
	}{
		{name: "relative escape", target: filepath.Join("..", filepath.Base(root)+"-outside", "x")},
		{name: "absolute escape", target: outside},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linkPath := filepath.Join(root, "escape-"+tt.name)
			if err := os.Symlink(tt.target, linkPath); err != nil {
				t.Skip("symlinks not available:", err)
			}
			_, err := takeSnapshot(context.Background(), root, "layer", nil)
			if !errors.Is(err, ErrSymlinkEscapes) {
				t.Fatalf("takeSnapshot() error = %v, want %v", err, ErrSymlinkEscapes)
			}
		})
	}
}

func TestStore_Dir_Add_symlinkEscapesRoot(t *testing.T) {
	tempDir := t.TempDir()
	dirPath := filepath.Join(tempDir, "tree")
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirPath, "a.txt"), []byte("a"), 0644); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(tempDir, "secret.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dirPath, "leak")); err != nil {
		t.Skip("symlinks not available:", err)
	}

	s, err := New(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.Add(context.Background(), "tree", "", dirPath); !errors.Is(err, ErrSymlinkEscapes) {
		t.Fatalf("Store.Add() error = %v, want %v", err, ErrSymlinkEscapes)
	}

	// no temporary layer may survive a rejected pack
	count := 0
	s.tmpFiles.Range(func(_, _ any) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("found %d leftover temporary layers", count)
	}
}

// gateAtSentry runs Add with an ignore rule that blocks while the sentinel
// entry is being snapshotted. The returned release function lets the test
// mutate another entry after its snapshot was frozen but before the walk
// resumes, and wait blocks until Add finishes.
func gateAtSentry(t *testing.T, s *Store, dir, sentryName string, ignore IgnoreFunc) (release func(), wait func() error) {
	t.Helper()
	reached := make(chan struct{})
	proceed := make(chan struct{})
	done := make(chan error, 1)

	gate := IgnoreFunc(func(path string, info os.FileInfo) bool {
		if path == sentryName {
			close(reached)
			<-proceed
		}
		if ignore != nil {
			return ignore(path, info)
		}
		return false
	})
	s.IgnoreRules = gate

	go func() {
		_, err := s.Add(context.Background(), "tree", "", dir)
		done <- err
	}()

	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the snapshot to reach the sentinel")
	}
	return func() { close(proceed) }, func() error { return <-done }
}

func TestStore_Dir_Add_fileChangedAfterSnapshot(t *testing.T) {
	tempDir := t.TempDir()
	dirPath := filepath.Join(tempDir, "tree")
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		t.Fatal(err)
	}
	// a.txt is snapshotted before the z.txt sentinel is reached
	target := filepath.Join(dirPath, "a.txt")
	if err := os.WriteFile(target, bytes.Repeat([]byte{'x'}, 4096), 0644); err != nil {
		t.Fatal(err)
	}
	sentry := filepath.Join(dirPath, "z.txt")
	if err := os.WriteFile(sentry, []byte("sentinel"), 0644); err != nil {
		t.Fatal(err)
	}

	s, err := New(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	release, wait := gateAtSentry(t, s, dirPath, "tree/z.txt", nil)
	// the snapshot has already frozen a.txt; change it before the write phase
	if err := os.WriteFile(target, []byte("changed after the snapshot was frozen"), 0644); err != nil {
		t.Fatal(err)
	}
	release()

	if err := wait(); !errors.Is(err, ErrFileChanged) {
		t.Fatalf("Store.Add() error = %v, want %v", err, ErrFileChanged)
	}
}

func TestStore_Dir_Add_failureRemovesTempLayer(t *testing.T) {
	tempDir := t.TempDir()
	dirPath := filepath.Join(tempDir, "tree")
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dirPath, "a.txt")
	if err := os.WriteFile(target, []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}
	sentry := filepath.Join(dirPath, "z.txt")
	if err := os.WriteFile(sentry, []byte("sentinel"), 0644); err != nil {
		t.Fatal(err)
	}

	s, err := New(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	release, wait := gateAtSentry(t, s, dirPath, "tree/z.txt", nil)
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	release()

	if err := wait(); !errors.Is(err, ErrFileChanged) {
		t.Fatalf("Store.Add() error = %v, want %v", err, ErrFileChanged)
	}

	count := 0
	s.tmpFiles.Range(func(_, _ any) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("found %d leftover temporary layers after a failed Add", count)
	}
}

func TestStore_Dir_Add_reproducibleDescriptor(t *testing.T) {
	tempDir := t.TempDir()
	build := func(root string) {
		if err := os.MkdirAll(filepath.Join(root, "sub"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("alpha"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "sub", "b.txt"), []byte("bravo"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	first := filepath.Join(tempDir, "first")
	second := filepath.Join(tempDir, "second")
	if err := os.MkdirAll(first, 0755); err != nil {
		t.Fatal(err)
	}
	build(first)
	if err := os.MkdirAll(second, 0755); err != nil {
		t.Fatal(err)
	}
	build(second)

	s1, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	s1.TarReproducible = true
	s2, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	s2.TarReproducible = true

	desc1, err := s1.Add(context.Background(), "layer", "", first)
	if err != nil {
		t.Fatal(err)
	}
	// give the second tree distinct timestamps so the test is meaningful
	future := time.Now().Add(2 * time.Hour)
	_ = filepath.Walk(second, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		return os.Chtimes(path, future, future)
	})
	desc2, err := s2.Add(context.Background(), "layer", "", second)
	if err != nil {
		t.Fatal(err)
	}

	if desc1.Digest != desc2.Digest || desc1.Size != desc2.Size {
		t.Errorf("descriptors not reproducible:\n%+v\n%+v", desc1, desc2)
	}
	if desc1.Annotations[AnnotationDigest] != desc2.Annotations[AnnotationDigest] {
		t.Error("uncompressed tar digests differ between identical trees")
	}
}

func TestStore_Dir_Add_descriptorMatchesLayerContent(t *testing.T) {
	tempDir := t.TempDir()
	dirPath := filepath.Join(tempDir, "tree")
	if err := os.MkdirAll(filepath.Join(dirPath, "sub"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirPath, "a.txt"), []byte("alpha"), 0644); err != nil {
		t.Fatal(err)
	}

	s, err := New(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	desc, err := s.Add(context.Background(), "tree", ocispec.MediaTypeImageLayerGzip, dirPath)
	if err != nil {
		t.Fatal(err)
	}

	rc, err := s.Fetch(context.Background(), desc)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	layer, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(layer)) != desc.Size {
		t.Errorf("layer size = %d, descriptor size = %d", len(layer), desc.Size)
	}
	if got := desc.Digest.Algorithm().FromBytes(layer); got != desc.Digest {
		t.Errorf("layer digest = %s, descriptor digest = %s", got, desc.Digest)
	}

	// the stored layer must contain exactly the snapshotted files
	gzr, err := gzip.NewReader(bytes.NewReader(layer))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gzr)
	names := map[string]bool{}
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names[header.Name] = true
	}
	for _, want := range []string{"tree", "tree/a.txt", "tree/sub"} {
		if !names[want] {
			t.Errorf("layer is missing entry %q", want)
		}
	}
}

func TestStore_File_Add_replacedBySymlink(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "f.txt")
	if err := os.WriteFile(path, []byte("original"), 0644); err != nil {
		t.Fatal(err)
	}

	s, err := New(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(tempDir, "target"), path); err != nil {
		t.Skip("symlinks not available:", err)
	}

	if _, err := s.descriptorFromFile(fi, "", path); !errors.Is(err, ErrFileChanged) {
		t.Fatalf("descriptorFromFile() error = %v, want %v", err, ErrFileChanged)
	}
}
