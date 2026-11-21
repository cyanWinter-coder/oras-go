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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlight_SharesInFlightCall(t *testing.T) {
	var flight Flight[string, int]
	var calls atomic.Int64
	started := make(chan struct{})
	release := make(chan struct{})
	fn := func(ctx context.Context) (int, error) {
		calls.Add(1)
		close(started)
		<-release
		return 42, nil
	}

	const subscribers = 5
	results := make([]int, subscribers)
	errs := make([]error, subscribers)
	var wg sync.WaitGroup
	for i := 0; i < subscribers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = flight.Do(context.Background(), "key", fn)
		}()
	}

	// wait for the shared call to start, then give the other subscribers a
	// chance to join before releasing it
	<-started
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Errorf("fn called %d times, want 1", got)
	}
	for i := range results {
		if errs[i] != nil {
			t.Errorf("subscriber %d: error = %v, want nil", i, errs[i])
		}
		if results[i] != 42 {
			t.Errorf("subscriber %d: result = %d, want 42", i, results[i])
		}
	}
}

func TestFlight_DifferentKeys(t *testing.T) {
	var flight Flight[string, int]
	var calls atomic.Int64
	fn := func(ctx context.Context) (int, error) {
		calls.Add(1)
		return 1, nil
	}

	var wg sync.WaitGroup
	for _, key := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := flight.Do(context.Background(), key, fn); err != nil {
				t.Errorf("Do() error = %v, want nil", err)
			}
		}()
	}
	wg.Wait()
	if got := calls.Load(); got != 3 {
		t.Errorf("fn called %d times, want 3", got)
	}
}

func TestFlight_SubscriberCancelDoesNotCancelSharedWork(t *testing.T) {
	var flight Flight[string, string]
	started := make(chan struct{})
	release := make(chan struct{})
	fn := func(ctx context.Context) (string, error) {
		close(started)
		select {
		case <-release:
			return "done", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	// the first subscriber cancels while the shared call is in flight
	ctx, cancel := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := flight.Do(ctx, "key", fn)
		firstResult <- err
	}()

	// the second subscriber keeps waiting
	secondResult := make(chan string, 1)
	secondErr := make(chan error, 1)
	go func() {
		result, err := flight.Do(context.Background(), "key", fn)
		secondResult <- result
		secondErr <- err
	}()

	<-started
	cancel()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled subscriber error = %v, want %v", err, context.Canceled)
	}

	// the shared call must still be running for the remaining subscriber
	select {
	case err := <-secondErr:
		t.Fatalf("shared call ended early with error %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-secondErr; err != nil {
		t.Errorf("remaining subscriber error = %v, want nil", err)
	}
	if result := <-secondResult; result != "done" {
		t.Errorf("remaining subscriber result = %q, want %q", result, "done")
	}
}

func TestFlight_LastSubscriberCancelCancelsWork(t *testing.T) {
	var flight Flight[string, struct{}]
	started := make(chan struct{})
	workCancelled := make(chan struct{})
	fn := func(ctx context.Context) (struct{}, error) {
		close(started)
		<-ctx.Done()
		close(workCancelled)
		return struct{}{}, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := flight.Do(ctx, "key", fn)
		done <- err
	}()

	<-started
	cancel()
	select {
	case <-workCancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("shared work was not cancelled after the last subscriber left")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Do() error = %v, want %v", err, context.Canceled)
	}
}

func TestFlight_SharesError(t *testing.T) {
	var flight Flight[string, struct{}]
	wantErr := errors.New("test error")
	fn := func(ctx context.Context) (struct{}, error) {
		return struct{}{}, wantErr
	}

	const subscribers = 3
	errs := make([]error, subscribers)
	var wg sync.WaitGroup
	for i := 0; i < subscribers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = flight.Do(context.Background(), "key", fn)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if !errors.Is(err, wantErr) {
			t.Errorf("subscriber %d: error = %v, want %v", i, err, wantErr)
		}
	}
}

func TestFlight_PanicBecomesError(t *testing.T) {
	var flight Flight[string, struct{}]
	fn := func(ctx context.Context) (struct{}, error) {
		panic("test panic")
	}
	if _, err := flight.Do(context.Background(), "key", fn); err == nil {
		t.Error("Do() error = nil, want an error for the panicking call")
	}
}

func TestFlight_NewCallAfterCompletion(t *testing.T) {
	var flight Flight[string, int]
	var calls atomic.Int64
	fn := func(ctx context.Context) (int, error) {
		return int(calls.Add(1)), nil
	}

	first, err := flight.Do(context.Background(), "key", fn)
	if err != nil {
		t.Fatalf("first Do() error = %v, want nil", err)
	}
	second, err := flight.Do(context.Background(), "key", fn)
	if err != nil {
		t.Fatalf("second Do() error = %v, want nil", err)
	}
	if first != 1 || second != 2 {
		t.Errorf("Do() results = %d, %d, want 1, 2: results must not be retained", first, second)
	}
}

func TestFlight_NewCallAfterAbandoned(t *testing.T) {
	var flight Flight[string, int]
	started := make(chan struct{})
	fn := func(ctx context.Context) (int, error) {
		close(started)
		<-ctx.Done()
		return 0, ctx.Err()
	}

	// the only subscriber leaves, abandoning the call
	ctx, cancel := context.WithCancel(context.Background())
	abandoned := make(chan error, 1)
	go func() {
		_, err := flight.Do(ctx, "key", fn)
		abandoned <- err
	}()
	<-started
	cancel()
	if err := <-abandoned; !errors.Is(err, context.Canceled) {
		t.Fatalf("abandoned Do() error = %v, want %v", err, context.Canceled)
	}

	// a new subscriber starts a fresh call rather than joining the abandoned one
	var calls atomic.Int64
	result, err := flight.Do(context.Background(), "key", func(ctx context.Context) (int, error) {
		return int(calls.Add(1)), nil
	})
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}
	if result != 1 {
		t.Errorf("Do() result = %d, want 1", result)
	}
}
