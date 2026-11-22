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

package cas

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	contentpkg "github.com/oras-project/oras-go/v3/content"
	"github.com/oras-project/oras-go/v3/errdef"
	"github.com/oras-project/oras-go/v3/internal/descriptor"
	"github.com/oras-project/oras-go/v3/internal/syncutil"
)

// Memory is a memory based CAS.
type Memory struct {
	content sync.Map // map[descriptor.Descriptor][]byte
	// flights coalesces concurrent pushes of the same descriptor so the
	// content is read and verified once and every waiter observes the same
	// committed terminal state.
	flights syncutil.Flight[descriptor.Descriptor, struct{}]
}

// NewMemory creates a new Memory CAS.
func NewMemory() *Memory {
	return &Memory{}
}

// Fetch fetches the content identified by the descriptor.
func (m *Memory) Fetch(_ context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	key := descriptor.FromOCI(target)
	content, exists := m.content.Load(key)
	if !exists {
		return nil, fmt.Errorf("%s: %s: %w", key.Digest, key.MediaType, errdef.ErrNotFound)
	}
	return io.NopCloser(bytes.NewReader(content.([]byte))), nil
}

// Push pushes the content, matching the expected descriptor.
//
// Concurrent pushes of the same descriptor are coalesced into one write: the
// content is read and verified against the descriptor before it is published,
// and every concurrent caller shares the result. Failed or cancelled writes
// never publish a partial value, so Fetch can only observe complete content.
func (m *Memory) Push(ctx context.Context, expected ocispec.Descriptor, content io.Reader) error {
	key := descriptor.FromOCI(expected)
	_, err := m.flights.Do(ctx, key, func(context.Context) (struct{}, error) {
		// Re-check under the coalesced call: a waiter may have found an
		// entry created after the fast-path check, or an earlier call may
		// have committed the content.
		if _, exists := m.content.Load(key); exists {
			return struct{}{}, fmt.Errorf("%s: %s: %w", key.Digest, key.MediaType, errdef.ErrAlreadyExists)
		}

		// Read and verify the whole content into a private buffer first. It
		// is published to the index only after verification succeeds, so a
		// short write or a digest mismatch leaves no reachable state.
		value, err := contentpkg.ReadAll(content, expected)
		if err != nil {
			return struct{}{}, err
		}
		if _, exists := m.content.LoadOrStore(key, value); exists {
			return struct{}{}, fmt.Errorf("%s: %s: %w", key.Digest, key.MediaType, errdef.ErrAlreadyExists)
		}
		return struct{}{}, nil
	})
	return err
}

// Exists returns true if the described content exists.
func (m *Memory) Exists(_ context.Context, target ocispec.Descriptor) (bool, error) {
	key := descriptor.FromOCI(target)
	_, exists := m.content.Load(key)
	return exists, nil
}

// Map dumps the memory into a built-in map structure.
// Like other operations, calling Map() is go-routine safe. However, it does not
// necessarily correspond to any consistent snapshot of the storage contents.
func (m *Memory) Map() map[descriptor.Descriptor][]byte {
	res := make(map[descriptor.Descriptor][]byte)
	m.content.Range(func(key, value any) bool {
		res[key.(descriptor.Descriptor)] = value.([]byte)
		return true
	})
	return res
}
