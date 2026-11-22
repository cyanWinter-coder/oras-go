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
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlight_SingleCall(t *testing.T) {
	var f Flight[string, int]
	var calls atomic.Int32

	got, err := f.Do(context.Background(), "k", func(context.Context) (int, error) {
		calls.Add(1)
		return 42, nil
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if got != 42 {
		t.Errorf("Do() = %v, want 42", got)
	}
	if calls.Load() != 1 {
		t.Errorf("fn called %d times, want 1", calls.Load())
	}
}

func TestFlight_PropagatesError(t *testing.T) {
	var f Flight[string, int]
	wantErr := errors.New("boom")

	_, err := f.Do(context.Background(), "k", func(context.Context) (int, error) {
		return 0, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("Do() error = %v, want %v", err, wantErr)
	}

	// a failed call is removed, so the key can be retried.
	var calls atomic.Int32
	got, err := f.Do(context.Background(), "k", func(context.Context) (int, error) {
		calls.Add(1)
		return 7, nil
	})
	if err != nil {
		t.Fatalf("retry Do() error = %v", err)
	}
	if got != 7 {
		t.Errorf("retry Do() = %v, want 7", got)
	}
}

// waitForBlockedDo spins until at least n goroutines are parked inside
// Flight.Do waiting on an in-flight call. Stack inspection is used because
// subscription happens under Flight's lock and is not observable from
// outside; a goroutine blocked in Do has necessarily completed subscribe().
func waitForBlockedDo(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	marker := []byte("syncutil.(*Flight[...]).Do")
	for {
		// grow the buffer until every goroutine's stack fits, so a tight
		// buffer cannot truncate the frames being counted.
		buf := make([]byte, 1<<16)
		size := runtime.Stack(buf, true)
		for size == len(buf) {
			buf = make([]byte, len(buf)*2)
			size = runtime.Stack(buf, true)
		}
		if bytes.Count(buf[:size], marker) >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d goroutines blocked in Flight.Do; stacks:\n%s", n, buf[:size])
		}
		runtime.Gosched()
	}
}

// coalescedCall runs fn through f.Do for key from n goroutines at once,
// guaranteeing they coalesce into a single fn execution. All goroutines are
// started and parked first, then released together. The leader's fn stays
// blocked until every goroutine is observed parked inside Do, so the map
// entry cannot be removed while a goroutine is still about to subscribe.
func coalescedCall[K comparable, V any](t *testing.T, f *Flight[K, V], n int, key K,
	body func() (V, error)) (*sync.WaitGroup, []error, []V, *atomic.Int32) {
	t.Helper()
	var ready sync.WaitGroup
	ready.Add(n)
	proceed := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32

	fn := func(context.Context) (V, error) {
		calls.Add(1)
		waitForBlockedDo(t, n)
		<-release
		return body()
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	results := make([]V, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ready.Done()
			<-proceed
			results[i], errs[i] = f.Do(context.Background(), key, fn)
		}(i)
	}
	ready.Wait()
	close(proceed)
	waitForBlockedDo(t, n)
	close(release)
	return &wg, errs, results, &calls
}

func TestFlight_CoalescesConcurrentCalls(t *testing.T) {
	var f Flight[string, int]

	wg, errs, results, calls := coalescedCall(t, &f, 32, "k", func() (int, error) {
		return 99, nil
	})
	wg.Wait()

	if calls.Load() != 1 {
		t.Errorf("fn called %d times, want 1", calls.Load())
	}
	for i := range errs {
		if errs[i] != nil {
			t.Errorf("waiter %d error = %v", i, errs[i])
		}
		if results[i] != 99 {
			t.Errorf("waiter %d result = %v, want 99", i, results[i])
		}
	}
}

func TestFlight_SharesFailure(t *testing.T) {
	var f Flight[string, int]
	wantErr := errors.New("write failed")

	wg, errs, _, calls := coalescedCall(t, &f, 8, "k", func() (int, error) {
		return 0, wantErr
	})
	wg.Wait()

	if calls.Load() != 1 {
		t.Errorf("fn called %d times, want 1", calls.Load())
	}
	for i, err := range errs {
		if !errors.Is(err, wantErr) {
			t.Errorf("waiter %d error = %v, want %v", i, err, wantErr)
		}
	}

	// the failed entry is gone: a later call retries.
	var retries atomic.Int32
	got, err := f.Do(context.Background(), "k", func(context.Context) (int, error) {
		retries.Add(1)
		return 1, nil
	})
	if err != nil {
		t.Fatalf("retry Do() error = %v", err)
	}
	if retries.Load() != 1 {
		t.Errorf("retry fn called %d times, want 1", retries.Load())
	}
	if got != 1 {
		t.Errorf("retry Do() = %v, want 1", got)
	}
}

func TestFlight_KeysAreIndependent(t *testing.T) {
	var f Flight[string, int]
	var calls atomic.Int32
	var wg sync.WaitGroup
	for _, key := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			_, err := f.Do(context.Background(), key, func(context.Context) (int, error) {
				calls.Add(1)
				return 0, nil
			})
			if err != nil {
				t.Errorf("Do(%q) error = %v", key, err)
			}
		}(key)
	}
	wg.Wait()
	if calls.Load() != 3 {
		t.Errorf("fn called %d times, want 3", calls.Load())
	}
}

func TestFlight_SequentialCallsRunAgain(t *testing.T) {
	var f Flight[string, int]
	var calls atomic.Int32
	for range 3 {
		if _, err := f.Do(context.Background(), "k", func(context.Context) (int, error) {
			calls.Add(1)
			return 0, nil
		}); err != nil {
			t.Fatalf("Do() error = %v", err)
		}
	}
	if calls.Load() != 3 {
		t.Errorf("fn called %d times, want 3", calls.Load())
	}
}

func TestFlight_WaiterCancellationDoesNotCancelCall(t *testing.T) {
	var f Flight[string, int]
	started := make(chan struct{})
	release := make(chan struct{})
	var leaderDone atomic.Bool

	leaderErr := make(chan error, 1)
	go func() {
		_, err := f.Do(context.Background(), "k", func(ctx context.Context) (int, error) {
			close(started)
			<-release
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			return 5, nil
		})
		leaderDone.Store(true)
		leaderErr <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("leader call never started")
	}

	// a waiter gives up early.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.Do(ctx, "k", func(context.Context) (int, error) {
		t.Fatal("cancelled waiter must not start a new call")
		return 0, nil
	}); !errors.Is(err, context.Canceled) {
		t.Errorf("waiter Do() error = %v, want %v", err, context.Canceled)
	}

	if leaderDone.Load() {
		t.Fatal("shared call must keep running after a waiter cancels")
	}
	close(release)

	select {
	case err := <-leaderErr:
		if err != nil {
			t.Errorf("leader Do() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("leader call never completed")
	}
}

func TestFlight_LeaderFailureAllowsRetry(t *testing.T) {
	var f Flight[string, int]
	wantErr := errors.New("short write")

	if _, err := f.Do(context.Background(), "k", func(context.Context) (int, error) {
		return 0, wantErr
	}); !errors.Is(err, wantErr) {
		t.Errorf("Do() error = %v, want %v", err, wantErr)
	}

	// a failed leader does not poison the key: a later call retries.
	var calls atomic.Int32
	got, err := f.Do(context.Background(), "k", func(context.Context) (int, error) {
		calls.Add(1)
		return 1, nil
	})
	if err != nil {
		t.Fatalf("retry Do() error = %v", err)
	}
	if got != 1 || calls.Load() != 1 {
		t.Errorf("retry result = %d, calls = %d, want 1 and 1", got, calls.Load())
	}
}

func TestFlight_LeaderContextCancellationFailsCall(t *testing.T) {
	var f Flight[string, int]
	leaderCtx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	go func() {
		_, _ = f.Do(leaderCtx, "k", func(ctx context.Context) (int, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("leader call never started")
	}
	cancel()

	// The cancelled leader publishes its failure and removes its entry; a
	// later call then starts a fresh call. Poll until the entry is released,
	// since the leader's own Do may return on ctx.Done() slightly before the
	// entry is removed.
	deadline := time.Now().Add(time.Second)
	var calls atomic.Int32
	for {
		got, err := f.Do(context.Background(), "k", func(context.Context) (int, error) {
			calls.Add(1)
			return 1, nil
		})
		if errors.Is(err, context.Canceled) {
			if time.Now().After(deadline) {
				t.Fatal("cancelled leader never released the key")
			}
			time.Sleep(time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("retry Do() error = %v", err)
		}
		if got != 1 || calls.Load() != 1 {
			t.Errorf("retry result = %d, calls = %d, want 1 and 1", got, calls.Load())
		}
		break
	}
}

func TestFlight_PanicIsSharedAsError(t *testing.T) {
	var f Flight[string, int]

	wg, errs, _, calls := coalescedCall(t, &f, 4, "k", func() (int, error) {
		panic("kaboom")
	})
	wg.Wait()

	if calls.Load() != 1 {
		t.Errorf("fn called %d times, want 1", calls.Load())
	}
	for i, err := range errs {
		if err == nil {
			t.Errorf("waiter %d did not receive the panic error", i)
			continue
		}
		if !strings.Contains(err.Error(), "kaboom") {
			t.Errorf("waiter %d error = %v, want it to mention the panic", i, err)
		}
	}

	// the panicking entry is gone: a later call is allowed.
	got, err := f.Do(context.Background(), "k", func(context.Context) (int, error) {
		return 1, nil
	})
	if err != nil {
		t.Fatalf("retry Do() error = %v", err)
	}
	if got != 1 {
		t.Errorf("retry Do() = %v, want 1", got)
	}
}

func TestFlight_ReusesEntryAfterCompletion(t *testing.T) {
	// guards against a stale entry blocking or coalescing later calls.
	var f Flight[int, string]
	for i := range 5 {
		got, err := f.Do(context.Background(), 1, func(context.Context) (string, error) {
			return fmt.Sprintf("v%d", i), nil
		})
		if err != nil {
			t.Fatalf("Do() %d error = %v", i, err)
		}
		if want := fmt.Sprintf("v%d", i); got != want {
			t.Errorf("Do() %d = %q, want %q", i, got, want)
		}
	}
}
