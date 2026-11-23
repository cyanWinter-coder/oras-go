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
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/errdef"
	"github.com/oras-project/oras-go/v3/internal/descriptor"
)

// ErrInvalidPlatform is returned by [PackImage] when a platform is invalid,
// for example when its OS or architecture is empty.
var ErrInvalidPlatform = errors.New("invalid platform")

// errInvalidFilePath is returned by [PackImage] when the path of a
// [PackImageFile] is invalid.
var errInvalidFilePath = errors.New("invalid file path")

// PackImageFile describes a file to be packed into the layer of a platform
// image by [PackImage].
type PackImageFile struct {
	// Path is the slash-separated path of the file in the layer tar. It must
	// be relative and must not escape the layer root. The path is normalized
	// with [path.Clean] before packing.
	Path string

	// Content is the content of the file.
	Content []byte
}

// PackImagePlatform describes the files and annotations of a single platform
// image packed by [PackImage].
type PackImagePlatform struct {
	// Platform is the platform of the image. OS and Architecture are
	// required. OS, Architecture and Variant are normalized to lower case,
	// and OSFeatures are trimmed, sorted and deduplicated before packing.
	Platform ocispec.Platform

	// Files are the files packed into the layer of the image. If several
	// files normalize to the same path, the last one wins.
	Files []PackImageFile

	// ManifestAnnotations is the annotation map of the image manifest of this
	// platform. See PackManifestOptions.ManifestAnnotations for the
	// requirements on the ocispec.AnnotationCreated key.
	ManifestAnnotations map[string]string
}

// PackImageOptions contains optional parameters for [PackImage].
type PackImageOptions struct {
	// IndexAnnotations is the annotation map of the top-level index. In order
	// to make [PackImage] reproducible, set the key ocispec.AnnotationCreated
	// (i.e. "org.opencontainers.image.created") to a fixed value. The value
	// must conform to RFC 3339.
	IndexAnnotations map[string]string

	// LayerAnnotations is the annotation map of each packed layer descriptor.
	LayerAnnotations map[string]string

	// DigestAlgorithm is the algorithm used to compute the digest of the
	// index, and of the manifests, configs and layers that [PackImage]
	// generates.
	// If empty, [digest.Canonical] (SHA-256) is used. SHA-384 and SHA-512 are
	// the other supported algorithms, and must be available, which requires
	// their hash package to be linked into the binary (for example by
	// importing crypto/sha512).
	DigestAlgorithm digest.Algorithm
}

// PackImage packs one image per platform, each with its files in a single
// uncompressed tar layer, and publishes a top-level OCI Image Index
// referencing the per-platform image manifests. The layout is consistent
// regardless of the platform count: even a single platform yields an index
// referencing one manifest.
//
// All inputs are validated and normalized before any content is written:
//   - Platforms are normalized (see [PackImagePlatform.Platform]) and must be
//     unique; a duplicate platform results in an error wrapping
//     [errdef.ErrAlreadyExists].
//   - File paths are normalized with [path.Clean] and must be relative paths
//     that do not escape the layer root. If several files of one platform
//     normalize to the same path, the last one wins, and the layer (and thus
//     its diffID) contains only the winning file.
//   - The ocispec.AnnotationCreated annotation of each manifest and of the
//     index is validated, or set to the current time if absent, before
//     packing begins.
//
// For each platform, the files are packed into an uncompressed tar layer in
// sorted path order with deterministic metadata, so the digest of the layer
// equals the diffID recorded in the image config. The layer is pushed with
// media type "application/vnd.oci.image.layer.v1.tar", and the config with
// media type "application/vnd.oci.image.config.v1+json".
//
// The top-level index is packed and pushed only after every platform
// manifest has been pushed successfully, so a failure never publishes a
// partial index.
//
// If packing fails or ctx is canceled, the content introduced by this call
// is removed best-effort in reverse push order, provided that target
// implements [content.Deleter]. Content that already existed in target
// before this call, or that is shared between platforms, is never deleted.
//
// If succeeded, returns a descriptor of the packed index.
func PackImage(ctx context.Context, target content.Storage, platforms []PackImagePlatform, opts PackImageOptions) (ocispec.Descriptor, error) {
	if opts.DigestAlgorithm == "" {
		opts.DigestAlgorithm = digest.Canonical
	}
	if !descriptor.IsSupportedAlgorithm(opts.DigestAlgorithm) || !opts.DigestAlgorithm.Available() {
		return ocispec.Descriptor{}, fmt.Errorf("digest algorithm %q: %w", opts.DigestAlgorithm, errdef.ErrUnsupported)
	}
	if len(platforms) == 0 {
		return ocispec.Descriptor{}, errors.New("no platform specified")
	}

	// Validate and normalize all inputs before writing any content, so that
	// an invalid input never leaves a partially packed image behind.
	indexAnnotations, err := ensureAnnotationCreated(opts.IndexAnnotations, ocispec.AnnotationCreated)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	type preparedPlatform struct {
		platform    ocispec.Platform
		files       []PackImageFile
		annotations map[string]string
	}
	prepared := make([]preparedPlatform, 0, len(platforms))
	seenPlatforms := make(map[string]struct{}, len(platforms))
	for _, p := range platforms {
		platform, err := normalizePlatform(p.Platform)
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		key := platformKey(platform)
		if _, ok := seenPlatforms[key]; ok {
			return ocispec.Descriptor{}, fmt.Errorf("duplicate platform %s: %w", key, errdef.ErrAlreadyExists)
		}
		seenPlatforms[key] = struct{}{}
		annotations, err := ensureAnnotationCreated(p.ManifestAnnotations, ocispec.AnnotationCreated)
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		files, err := normalizeImageFiles(p.Files)
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		prepared = append(prepared, preparedPlatform{
			platform:    platform,
			files:       files,
			annotations: annotations,
		})
	}

	// Pack each platform. The top-level index is packed and pushed only
	// after every platform manifest has been pushed successfully.
	ts := &trackingStorage{Storage: target, seen: make(map[digest.Digest]struct{})}
	cleanup := func(err error) (ocispec.Descriptor, error) {
		// use a detached context so that cleanup can proceed even when the
		// failure was caused by the cancellation of ctx
		if cerr := ts.cleanup(context.WithoutCancel(ctx)); cerr != nil {
			err = errors.Join(err, fmt.Errorf("failed to clean up partial content: %w", cerr))
		}
		return ocispec.Descriptor{}, err
	}

	manifests := make([]ocispec.Descriptor, 0, len(prepared))
	for _, pp := range prepared {
		layerDesc, layerData, err := packImageLayer(pp.files, opts.LayerAnnotations, opts.DigestAlgorithm)
		if err != nil {
			return cleanup(err)
		}
		if err := pushIfNotExist(ctx, ts, layerDesc, layerData); err != nil {
			return cleanup(fmt.Errorf("failed to push layer: %w", err))
		}

		configDesc, configData, err := packImageConfig(pp.platform, layerDesc.Digest, opts.DigestAlgorithm)
		if err != nil {
			return cleanup(err)
		}
		if err := pushIfNotExist(ctx, ts, configDesc, configData); err != nil {
			return cleanup(fmt.Errorf("failed to push config: %w", err))
		}

		manifestDesc, err := packManifest(ctx, ts, PackManifestVersion1_1, "", PackManifestOptions{
			ConfigDescriptor:    &configDesc,
			Layers:              []ocispec.Descriptor{layerDesc},
			ManifestAnnotations: pp.annotations,
			DigestAlgorithm:     opts.DigestAlgorithm,
		})
		if err != nil {
			return cleanup(err)
		}
		platform := pp.platform
		manifestDesc.Platform = &platform
		manifests = append(manifests, manifestDesc)
	}

	indexDesc, err := packIndex(ctx, ts, "", PackIndexOptions{
		Manifests:        manifests,
		IndexAnnotations: indexAnnotations,
		DigestAlgorithm:  opts.DigestAlgorithm,
	})
	if err != nil {
		return cleanup(err)
	}
	return indexDesc, nil
}

// normalizePlatform normalizes a platform and validates its required fields.
func normalizePlatform(p ocispec.Platform) (ocispec.Platform, error) {
	p.OS = strings.ToLower(strings.TrimSpace(p.OS))
	p.Architecture = strings.ToLower(strings.TrimSpace(p.Architecture))
	p.Variant = strings.ToLower(strings.TrimSpace(p.Variant))
	p.OSVersion = strings.TrimSpace(p.OSVersion)
	if len(p.OSFeatures) > 0 {
		features := make([]string, 0, len(p.OSFeatures))
		seen := make(map[string]struct{}, len(p.OSFeatures))
		for _, f := range p.OSFeatures {
			f = strings.TrimSpace(f)
			if f == "" {
				continue
			}
			if _, ok := seen[f]; ok {
				continue
			}
			seen[f] = struct{}{}
			features = append(features, f)
		}
		slices.Sort(features)
		p.OSFeatures = features
	}
	if p.OS == "" {
		return ocispec.Platform{}, fmt.Errorf("%w: OS is required", ErrInvalidPlatform)
	}
	if p.Architecture == "" {
		return ocispec.Platform{}, fmt.Errorf("%w: architecture is required", ErrInvalidPlatform)
	}
	return p, nil
}

// platformKey returns a canonical string identifying a normalized platform.
func platformKey(p ocispec.Platform) string {
	return strings.Join([]string{
		p.OS,
		p.OSVersion,
		p.Architecture,
		p.Variant,
		strings.Join(p.OSFeatures, ","),
	}, "/")
}

// normalizeImageFiles normalizes the path of each file, resolves duplicate
// paths on a last-one-wins basis, and sorts the files by path for a
// deterministic layer.
func normalizeImageFiles(files []PackImageFile) ([]PackImageFile, error) {
	if len(files) == 0 {
		return nil, nil
	}
	normalized := make([]PackImageFile, 0, len(files))
	indexByPath := make(map[string]int, len(files))
	for _, f := range files {
		p, err := normalizeFilePath(f.Path)
		if err != nil {
			return nil, err
		}
		f.Path = p
		if i, ok := indexByPath[p]; ok {
			// last one wins
			normalized[i] = f
			continue
		}
		indexByPath[p] = len(normalized)
		normalized = append(normalized, f)
	}
	slices.SortFunc(normalized, func(a, b PackImageFile) int {
		return strings.Compare(a.Path, b.Path)
	})
	return normalized, nil
}

// normalizeFilePath cleans a slash-separated file path and ensures that it
// is relative and does not escape the layer root.
func normalizeFilePath(p string) (string, error) {
	if p == "" {
		return "", fmt.Errorf("file path is empty: %w", errInvalidFilePath)
	}
	if strings.ContainsRune(p, 0) {
		return "", fmt.Errorf("file path %q contains a NUL character: %w", p, errInvalidFilePath)
	}
	cleaned := path.Clean(p)
	switch {
	case path.IsAbs(cleaned):
		return "", fmt.Errorf("file path %q is not relative: %w", p, errInvalidFilePath)
	case cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../"):
		return "", fmt.Errorf("file path %q escapes the layer root: %w", p, errInvalidFilePath)
	}
	return cleaned, nil
}

// packImageLayer packs files into an uncompressed tar layer with
// deterministic metadata and returns its descriptor along with the tar
// content. As the layer is uncompressed, the digest of the returned
// descriptor is also the diffID of the layer.
func packImageLayer(files []PackImageFile, annotations map[string]string, alg digest.Algorithm) (ocispec.Descriptor, []byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	// fixed metadata for reproducible layers
	modTime := time.Unix(0, 0).UTC()
	for _, f := range files {
		hdr := &tar.Header{
			Name:     f.Path,
			Mode:     0o644,
			Size:     int64(len(f.Content)),
			ModTime:  modTime,
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return ocispec.Descriptor{}, nil, fmt.Errorf("failed to write tar header for %q: %w", f.Path, err)
		}
		if _, err := tw.Write(f.Content); err != nil {
			return ocispec.Descriptor{}, nil, fmt.Errorf("failed to write tar content for %q: %w", f.Path, err)
		}
	}
	if err := tw.Close(); err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("failed to finalize tar layer: %w", err)
	}
	data := buf.Bytes()
	desc, err := content.NewDescriptorFromBytesWithAlgorithm(ocispec.MediaTypeImageLayer, data, alg)
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("failed to create layer descriptor: %w", err)
	}
	desc.Annotations = annotations
	return desc, data, nil
}

// packImageConfig generates an image config for platform recording diffID as
// the single diffID of the rootfs, and returns its descriptor along with the
// config content.
func packImageConfig(platform ocispec.Platform, diffID digest.Digest, alg digest.Algorithm) (ocispec.Descriptor, []byte, error) {
	image := ocispec.Image{
		Platform: platform,
		RootFS: ocispec.RootFS{
			Type:    "layers",
			DiffIDs: []digest.Digest{diffID},
		},
	}
	data, err := json.Marshal(image)
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("failed to marshal image config: %w", err)
	}
	desc, err := content.NewDescriptorFromBytesWithAlgorithm(ocispec.MediaTypeImageConfig, data, alg)
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("failed to create config descriptor: %w", err)
	}
	return desc, data, nil
}

// trackedBlob records a descriptor pushed during a [PackImage] operation and
// whether the operation introduced the content to the target.
type trackedBlob struct {
	desc  ocispec.Descriptor
	owned bool
}

// trackingStorage wraps a [content.Storage] and records the descriptors
// pushed through it, so that a failed operation can remove the content it
// introduced while leaving pre-existing (shared) content untouched.
type trackingStorage struct {
	content.Storage

	mu      sync.Mutex
	tracked []trackedBlob
	seen    map[digest.Digest]struct{}
}

// Push pushes content to the underlying storage and records the descriptor
// the first time its digest is seen.
func (ts *trackingStorage) Push(ctx context.Context, expected ocispec.Descriptor, contentReader io.Reader) error {
	ts.mu.Lock()
	_, seen := ts.seen[expected.Digest]
	if !seen {
		ts.seen[expected.Digest] = struct{}{}
	}
	ts.mu.Unlock()

	owned := false
	if !seen {
		exists, err := ts.Storage.Exists(ctx, expected)
		if err != nil {
			// The existence is unknown. Conservatively treat the content as
			// pre-existing so that cleanup never removes content that this
			// operation did not introduce.
			exists = true
		}
		owned = !exists
	}

	err := ts.Storage.Push(ctx, expected, contentReader)
	if !seen && (err == nil || errors.Is(err, errdef.ErrAlreadyExists)) {
		if errors.Is(err, errdef.ErrAlreadyExists) {
			owned = false
		}
		ts.mu.Lock()
		ts.tracked = append(ts.tracked, trackedBlob{desc: expected, owned: owned})
		ts.mu.Unlock()
	}
	return err
}

// cleanup removes the content introduced through ts, in reverse push order.
// It is best-effort: if the underlying storage does not implement
// [content.Deleter], nothing is removed. Content that no longer exists is
// ignored.
func (ts *trackingStorage) cleanup(ctx context.Context) error {
	deleter, ok := ts.Storage.(content.Deleter)
	if !ok {
		return nil
	}
	ts.mu.Lock()
	tracked := slices.Clone(ts.tracked)
	ts.mu.Unlock()
	var errs []error
	for i := len(tracked) - 1; i >= 0; i-- {
		if !tracked[i].owned {
			continue
		}
		if err := deleter.Delete(ctx, tracked[i].desc); err != nil && !errors.Is(err, errdef.ErrNotFound) {
			errs = append(errs, fmt.Errorf("failed to delete %s: %w", tracked[i].desc.Digest, err))
		}
	}
	return errors.Join(errs...)
}
