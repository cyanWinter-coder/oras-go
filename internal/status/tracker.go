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

package status

import (
	"sync"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/internal/descriptor"
)

// Tracker tracks content status described by a descriptor.
type Tracker struct {
	status sync.Map // map[descriptor.Descriptor]chan struct{}
}

// NewTracker creates a new content status tracker.
func NewTracker() *Tracker {
	return &Tracker{}
}

// TryCommit tries to commit the work for the target descriptor.
// Returns true if committed. A channel is also returned for sending
// notifications. Once the work is done, the channel should be closed.
// Returns false if the work is done or still in progress.
func (t *Tracker) TryCommit(target ocispec.Descriptor) (chan struct{}, bool) {
	key := descriptor.FromOCI(target)
	// Check for an existing entry first so the common case (the descriptor
	// is already committed or in progress) does not pay for a channel
	// allocation that LoadOrStore would immediately discard.
	if status, exists := t.status.Load(key); exists {
		return status.(chan struct{}), false
	}
	status, exists := t.status.LoadOrStore(key, make(chan struct{}))
	return status.(chan struct{}), !exists
}

// Status represents the completion state of the work tracked for a
// descriptor.
type Status int

const (
	// StatusPending means no work has been committed for the descriptor yet.
	StatusPending Status = iota
	// StatusCommitted means the work for the descriptor has been committed
	// but has not completed. A descriptor whose work failed or was cancelled
	// remains in this state: only actual completion advances the status.
	StatusCommitted
	// StatusDone means the work for the descriptor completed successfully.
	StatusDone
)

// String returns a human-readable representation of the status.
func (s Status) String() string {
	switch s {
	case StatusPending:
		return "Pending"
	case StatusCommitted:
		return "Committed"
	case StatusDone:
		return "Done"
	default:
		return "Unknown"
	}
}

// StatusOf returns the completion status of the work tracked for target.
// It reflects the real per-node completion state: a node is StatusDone only
// if its work finished successfully while the operation was still active.
func (t *Tracker) StatusOf(target ocispec.Descriptor) Status {
	key := descriptor.FromOCI(target)
	status, exists := t.status.Load(key)
	if !exists {
		return StatusPending
	}
	select {
	case <-status.(chan struct{}):
		return StatusDone
	default:
		return StatusCommitted
	}
}
