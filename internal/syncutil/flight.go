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
	// cancel cancels the context the call runs with.
	cancel context.CancelFunc
	// waiting counts the subscribers still waiting for the result.
	waiting int
	// result and err are the outcome of the call, set before done is closed.
	result V
	err    error
}

// Flight shares the result of one in-flight call among the subscribers of
// the same key: while a call for a key is running, additional Do calls for
// that key wait for its result instead of starting their own.
//
// Every subscriber waits with its own context. A subscriber whose context is
// cancelled stops waiting and returns the context error, but does not cancel
// the shared call; the context the call runs with is cancelled only when its
// last subscriber goes away. The result of a completed call is not retained:
// a Do call made after the previous call for the same key finished starts a
// new call.
//
// The zero value is ready to use.
type Flight[K comparable, V any] struct {
	lock  sync.Mutex
	calls map[K]*flightCall[V]
}

// Do subscribes to the call identified by key, starting it with fn if no
// call for key is in flight, and returns the shared result.
//
// fn runs in its own goroutine with a context that carries the values of the
// first subscriber's context but is cancelled only when the last subscriber
// of the call goes away, so the cancellation of any single subscriber never
// cancels the shared work for the others. If fn panics, the panic is
// converted to an error so that a panicking call cannot crash its
// subscribers, which wait in other goroutines.
func (f *Flight[K, V]) Do(ctx context.Context, key K, fn func(ctx context.Context) (V, error)) (V, error) {
	f.lock.Lock()
	if f.calls == nil {
		f.calls = make(map[K]*flightCall[V])
	}
	call, inFlight := f.calls[key]
	if !inFlight {
		workCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		call = &flightCall[V]{
			done:   make(chan struct{}),
			cancel: cancel,
		}
		f.calls[key] = call
		go f.run(key, call, workCtx, fn)
	}
	call.waiting++
	f.lock.Unlock()

	select {
	case <-call.done:
		return call.result, call.err
	case <-ctx.Done():
		f.leave(key, call)
		var zero V
		return zero, ctx.Err()
	}
}

// run executes fn and publishes its result to the subscribers of call.
func (f *Flight[K, V]) run(key K, call *flightCall[V], ctx context.Context, fn func(ctx context.Context) (V, error)) {
	result, err := f.execute(ctx, fn)

	f.lock.Lock()
	defer f.lock.Unlock()
	if f.calls[key] != call {
		// The call was abandoned by its last subscriber and may have been
		// replaced by a new one; discard the result.
		return
	}
	delete(f.calls, key)
	call.result = result
	call.err = err
	close(call.done)
}

// execute calls fn, converting a panic into an error.
func (f *Flight[K, V]) execute(ctx context.Context, fn func(ctx context.Context) (V, error)) (result V, err error) {
	defer func() {
		if r := recover(); r != nil {
			var zero V
			result = zero
			err = fmt.Errorf("syncutil: panic in shared call: %v", r)
		}
	}()
	return fn(ctx)
}

// leave removes a cancelled subscriber from call, cancelling the shared work
// when its last subscriber goes away.
func (f *Flight[K, V]) leave(key K, call *flightCall[V]) {
	f.lock.Lock()
	defer f.lock.Unlock()
	call.waiting--
	if call.waiting == 0 && f.calls[key] == call {
		delete(f.calls, key)
		call.cancel()
	}
}
