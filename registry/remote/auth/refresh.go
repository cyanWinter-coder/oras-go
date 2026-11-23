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

package auth

import (
	"context"
	"sync"
)

// refreshGroup coalesces concurrent credential refreshes that target the same
// registry, scheme and scope key into a single shared fetch.
//
// Unlike a singleflight keyed on the caller's context, the fetch runs with a
// context detached from every individual caller: cancelling one waiting request
// only stops that request from waiting for the result, and neither cancels the
// shared fetch nor prevents the other callers from receiving its result.
type refreshGroup struct {
	mu      sync.Mutex
	entries map[string]*refreshCall
}

// refreshCall is one in-flight shared fetch.
type refreshCall struct {
	done   chan struct{}
	result string
	err    error
}

// newRefreshGroup creates an empty refresh group.
func newRefreshGroup() *refreshGroup {
	return &refreshGroup{
		entries: make(map[string]*refreshCall),
	}
}

// do returns the result of the fetch identified by key. When a fetch for the
// same key is already in flight, do waits for it and shares its result;
// otherwise the given fetch is started and its result is shared with every
// caller that joins before it completes.
//
// The fetch itself runs with a context detached from ctx, so ctx cancellation
// only removes the caller's own wait. A caller whose context ends before the
// result is ready returns ctx.Err() without affecting the fetch or any other
// waiter. The detached context retains the values of ctx, including the scope
// hints and token-request settings carried from Client.Do.
func (g *refreshGroup) do(ctx context.Context, key string, fetch func(context.Context) (string, error)) (string, error) {
	g.mu.Lock()
	if call, ok := g.entries[key]; ok {
		g.mu.Unlock()
		return waitRefresh(ctx, call)
	}
	call := &refreshCall{
		done: make(chan struct{}),
	}
	g.entries[key] = call
	g.mu.Unlock()

	go func() {
		// Detach cancellation and deadlines: the shared fetch must outlive any
		// single caller so that cancelling one request cannot abort the token
		// acquisition the other callers are waiting for. Context values are
		// preserved, so credential resolution and token requests keep working.
		call.result, call.err = fetch(context.WithoutCancel(ctx))

		// Remove the entry before broadcasting completion. Otherwise a caller
		// arriving in the gap between close(done) and the delete would join
		// the finished call and receive its stale result instead of starting
		// the next generation.
		g.mu.Lock()
		// Only remove the entry while it still represents this call. A fetch
		// for a later generation may have replaced it after this one finished.
		if current := g.entries[key]; current == call {
			delete(g.entries, key)
		}
		g.mu.Unlock()
		close(call.done)
	}()

	return waitRefresh(ctx, call)
}

// waitRefresh waits for the shared call to finish, unless ctx ends first.
func waitRefresh(ctx context.Context, call *refreshCall) (string, error) {
	select {
	case <-call.done:
		return call.result, call.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
