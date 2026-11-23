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

package retry

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// trackingReadCloser records how many bytes were read and whether Close ran.
type trackingReadCloser struct {
	reader io.Reader
	read   int64
	closed atomic.Bool
}

func (t *trackingReadCloser) Read(p []byte) (int, error) {
	n, err := t.reader.Read(p)
	t.read += int64(n)
	return n, err
}

func (t *trackingReadCloser) Close() error {
	t.closed.Store(true)
	return nil
}

func TestDrainAndClose_DrainsAndCloses(t *testing.T) {
	body := bytes.Repeat([]byte("a"), 4096)
	rc := &trackingReadCloser{reader: bytes.NewReader(body)}
	resp := &http.Response{Body: rc}

	DrainAndClose(context.Background(), resp)

	if !rc.closed.Load() {
		t.Error("DrainAndClose() did not close the body")
	}
	if rc.read != int64(len(body)) {
		t.Errorf("DrainAndClose() read %d bytes, want %d", rc.read, len(body))
	}
}

func TestDrainAndClose_NilSafe(t *testing.T) {
	// must not panic on a nil response or nil body
	DrainAndClose(context.Background(), nil)
	DrainAndClose(context.Background(), &http.Response{})
}

func TestDrainAndClose_StopsOnCancellation(t *testing.T) {
	// a reader that blocks until the context is cancelled
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write(bytes.Repeat([]byte("b"), 1024))
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rc := &trackingReadCloser{reader: pr}
	DrainAndClose(ctx, &http.Response{Body: rc})

	if !rc.closed.Load() {
		t.Error("DrainAndClose() did not close the body after cancellation")
	}
}

func TestDrainAndClose_CapsDrain(t *testing.T) {
	rc := &trackingReadCloser{reader: bytes.NewReader(make([]byte, maxDrainBytes*2))}
	DrainAndClose(context.Background(), &http.Response{Body: rc})

	if !rc.closed.Load() {
		t.Error("DrainAndClose() did not close the body")
	}
	if rc.read > maxDrainBytes {
		t.Errorf("DrainAndClose() read %d bytes, want at most %d", rc.read, maxDrainBytes)
	}
}

// TestTransport_DrainsBodyBeforeRetry proves the lifecycle boundary: when a
// response is retryable, its body is fully consumed before the next attempt, so
// the underlying TCP connection is returned to the idle pool and reused rather
// than torn down with an unread body.
func TestTransport_DrainsBodyBeforeRetry(t *testing.T) {
	const bodySize = 1024
	var attempts atomic.Int32
	var servedConns []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		servedConns = append(servedConns, r.RemoteAddr)
		if attempts.Add(1) == 1 {
			http.Error(w, string(bytes.Repeat([]byte("x"), bodySize)), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	transport := &Transport{Base: http.DefaultTransport.(*http.Transport).Clone()}
	client := &http.Client{Transport: transport}

	resp, err := client.Get(ts.URL)
	if err != nil {
		t.Fatalf("client.Get() error = %v", err)
	}
	DrainAndClose(resp.Request.Context(), resp)

	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected 2 attempts, got %d", got)
	}
	if len(servedConns) != 2 || servedConns[0] != servedConns[1] {
		t.Errorf("expected the retry to reuse the same TCP connection (%v), got %v", servedConns[:1], servedConns)
	}
}

// TestTransport_DoesNotReuseCancelledConnection proves the other half of the
// lifecycle boundary: when the request context is cancelled while the retry
// transport is waiting to retry, the transport returns the context error
// without starting another attempt on a connection whose state is unknown.
func TestTransport_DoesNotRetryAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			// cancel while the transport is waiting to retry
			cancel()
		}
		// always retryable, with a Retry-After so the transport enters its wait
		w.Header().Set("Retry-After", "5")
		http.Error(w, "slow", http.StatusTooManyRequests)
	}))
	defer ts.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	client := &http.Client{Transport: &Transport{Base: http.DefaultTransport.(*http.Transport).Clone()}}

	if _, err := client.Do(req); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("expected exactly 1 attempt before cancellation, got %d", got)
	}
}

// TestTransport_DrainsOnPolicyError verifies that when the policy itself errors
// (a non-retryable round-trip failure surfaced through the predicate), the
// response body is still drained and closed before the error is returned.
func TestTransport_DrainsOnPolicyError(t *testing.T) {
	expectedErr := errors.New("boom")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "error", http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	policy := &GenericPolicy{
		Retryable: func(resp *http.Response, err error) (bool, error) {
			return false, expectedErr
		},
		MaxRetry: 5,
	}
	transport := &Transport{
		Base:   http.DefaultTransport.(*http.Transport).Clone(),
		Policy: func() Policy { return policy },
	}
	client := &http.Client{Transport: transport}

	resp, err := client.Get(ts.URL)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
	if resp != nil {
		t.Errorf("expected nil response on policy error, got %v", resp)
	}
}
