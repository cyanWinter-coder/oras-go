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

package syncutil

import (
	"context"
	"fmt"
	"sync"
)

// flightCall represents a single in-flight call of a Flight.
type flightCall[V any] struct {
	// done is closed when the call completes and its result is published.
	done chan struct{}
	// result and err are the outcome of the call, set before done is closed.
	result V
	err    error
}

// Flight shares the result of one in-flight call among the callers of the
// same key: while a call for a key is running, additional Do calls for that
// key wait for its result instead of starting their own. When the call
// completes, its entry is removed, so the next Do call for the key starts a
// fresh call; completed results are not retained.
//
// Every caller waits with its own context. A caller whose context is
// cancelled while waiting returns the context error immediately, but the
// shared call keeps running: cancelling one waiter must never abort the
// write the other waiters depend on. If fn panics, the panic is converted
// into an error that all waiters of the call receive.
//
// The zero value is ready to use.
type Flight[K comparable, V any] struct {
	lock  sync.Mutex
	calls map[K]*flightCall[V]
}

// Do runs fn for key once, sharing its result with every concurrent Do call
// for the same key. The caller that starts the call runs fn in its own
// goroutine with the context it passed; waiters block until either the call
// publishes its result or their own context is done.
//
// A nil error means the action represented by fn has reached its committed
// terminal state for all callers. A non-nil error is shared with every
// waiter and the entry is removed, so a later Do call for the same key may
// retry the action.
func (f *Flight[K, V]) Do(ctx context.Context, key K, fn func(ctx context.Context) (V, error)) (V, error) {
	f.lock.Lock()
	if f.calls == nil {
		f.calls = make(map[K]*flightCall[V])
	}
	call, inFlight := f.calls[key]
	if !inFlight {
		call = &flightCall[V]{done: make(chan struct{})}
		f.calls[key] = call
		go f.run(key, call, ctx, fn)
	}
	f.lock.Unlock()

	select {
	case <-call.done:
		return call.result, call.err
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err()
	}
}

// run executes fn for a leader call and publishes its outcome to the waiters.
func (f *Flight[K, V]) run(key K, call *flightCall[V], ctx context.Context, fn func(ctx context.Context) (V, error)) {
	result, err := f.execute(ctx, fn)

	f.lock.Lock()
	// Only the current call for key publishes a result; a later call that
	// replaced it keeps its own entry.
	if f.calls[key] == call {
		delete(f.calls, key)
	}
	f.lock.Unlock()

	call.result = result
	call.err = err
	close(call.done)
}

// execute calls fn, converting a panic into an error so that a panicking
// call cannot crash its waiters, which run in other goroutines.
func (f *Flight[K, V]) execute(ctx context.Context, fn func(ctx context.Context) (V, error)) (result V, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("syncutil: panic in shared call: %v", r)
		}
	}()
	return fn(ctx)
}
