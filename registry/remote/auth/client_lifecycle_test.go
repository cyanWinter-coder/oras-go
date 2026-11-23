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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oras-project/oras-go/v3/registry/remote/credentials"
	"github.com/oras-project/oras-go/v3/registry/remote/properties"
	"github.com/oras-project/oras-go/v3/registry/remote/retry"
)

// newLifecycleClient builds an auth.Client whose requests share one pooled
// transport, so a test can observe whether a connection is reused across the
// unauthenticated 401 attempt and the authenticated retry.
func newLifecycleClient(t *testing.T, credential credentials.CredentialFunc) *Client {
	t.Helper()
	return &Client{
		Client: &http.Client{
			Transport: retry.NewTransport(http.DefaultTransport.(*http.Transport).Clone()),
		},
		CredentialFunc: credential,
		Cache:          NewCache(),
	}
}

// TestClient_Do_Drains401BodyAndReusesConnection proves the response lifecycle
// boundary on a bearer challenge: the 401 response carries a sizable body that
// the client never returns to its caller, yet it must be drained before the
// authenticated retry so the TCP connection is reused rather than closed with
// an unread body.
func TestClient_Do_Drains401BodyAndReusesConnection(t *testing.T) {
	const accessToken = "lifecycle/access/token"
	const bodySize = 8 * 1024

	var registryConns []string
	var registryHits atomic.Int32

	as := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		// fully consume the token request body so its connection is reusable too
		_, _ = io.Copy(io.Discard, r.Body)
		fmt.Fprintf(w, `{"access_token":%q}`, accessToken)
	}))
	defer as.Close()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registryConns = append(registryConns, r.RemoteAddr)
		if r.Header.Get("Authorization") != "Bearer "+accessToken {
			w.Header().Set("Www-Authenticate",
				fmt.Sprintf(`Bearer realm=%q,service=%q`, as.URL, r.Host))
			w.WriteHeader(http.StatusUnauthorized)
			// a body larger than a single read, left entirely to the client
			// to drain
			_, _ = w.Write(bytes.Repeat([]byte("z"), bodySize))
			return
		}
		registryHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	uri, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("invalid test http server: %v", err)
	}
	client := newLifecycleClient(t, func(ctx context.Context, res properties.Resource) (credentials.Credential, error) {
		if res.Host() != uri.Host {
			t.Errorf("credential requested for unexpected host %q", res.Host())
		}
		return credentials.Credential{Username: "user", Password: "pass"}, nil
	})

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Client.Do() error = %v", err)
	}
	retry.DrainAndClose(req.Context(), resp)

	if got := registryHits.Load(); got != 1 {
		t.Fatalf("expected 1 authenticated success, got %d", got)
	}
	// two registry hits: the unauthenticated attempt and the authenticated retry
	if len(registryConns) != 2 {
		t.Fatalf("expected 2 registry requests, got %d", len(registryConns))
	}
	if registryConns[0] != registryConns[1] {
		t.Errorf("expected the authenticated retry to reuse the 401 connection %q, got %q",
			registryConns[0], registryConns[1])
	}
}

// TestClient_Do_RefreshesTokenAtMostOnce pins the authorization boundary: a
// token that the registry keeps rejecting results in exactly one refresh
// attempt, after which the second 401 is returned to the caller rather than
// triggering an unbounded re-authentication loop.
func TestClient_Do_RefreshesTokenAtMostOnce(t *testing.T) {
	var challengeHits atomic.Int32
	var tokenFetches atomic.Int32

	as := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenFetches.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		// every fetched token is immediately stale
		fmt.Fprintf(w, `{"access_token":%q}`, "stale")
	}))
	defer as.Close()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		challengeHits.Add(1)
		w.Header().Set("Www-Authenticate",
			fmt.Sprintf(`Bearer realm=%q,service=%q`, as.URL, r.Host))
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"errors":[{"code":"UNAUTHORIZED"}]}`))
	}))
	defer ts.Close()

	client := newLifecycleClient(t, func(ctx context.Context, res properties.Resource) (credentials.Credential, error) {
		return credentials.Credential{Username: "user", Password: "pass"}, nil
	})

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Client.Do() error = %v", err)
	}
	retry.DrainAndClose(req.Context(), resp)

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected the second 401 to be returned, got status %d", resp.StatusCode)
	}
	// one unauthenticated attempt + one authenticated retry; no third attempt
	if got := challengeHits.Load(); got != 2 {
		t.Errorf("expected exactly 2 registry requests, got %d", got)
	}
	if got := tokenFetches.Load(); got != 1 {
		t.Errorf("expected exactly 1 token refresh, got %d", got)
	}
}

// TestClient_Do_ConcurrentRequestsShareTokenButAuthIndependently pins the
// concurrency boundary: many requests against the same registry resource share
// one token cache, so the token endpoint is hit once even when they start
// together, while each request still runs its own challenge/authentication
// sequence and independently succeeds.
func TestClient_Do_ConcurrentRequestsShareTokenButAuthIndependently(t *testing.T) {
	const accessToken = "concurrent/access/token"
	const requestCount = 25

	var tokenFetches atomic.Int32
	var registrySuccesses atomic.Int32
	var registryChallenges atomic.Int32

	// The token endpoint blocks until every request has been challenged. Every
	// fetch the cache does not coalesce would arrive here and block too, so the
	// number of arrivals while blocked deterministically exposes whether the
	// singleflight held, independent of goroutine scheduling.
	releaseToken := make(chan struct{})
	allChallenged := make(chan struct{})

	as := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenFetches.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		<-releaseToken
		fmt.Fprintf(w, `{"access_token":%q}`, accessToken)
	}))
	defer as.Close()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+accessToken {
			if registryChallenges.Add(1) == requestCount {
				close(allChallenged)
			}
			w.Header().Set("Www-Authenticate",
				fmt.Sprintf(`Bearer realm=%q,service=%q`, as.URL, r.Host))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		registrySuccesses.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	uri, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("invalid test http server: %v", err)
	}
	client := newLifecycleClient(t, func(ctx context.Context, res properties.Resource) (credentials.Credential, error) {
		if res.Host() != uri.Host {
			t.Errorf("credential requested for unexpected host %q", res.Host())
		}
		return credentials.Credential{Username: "user", Password: "pass"}, nil
	})

	var wg sync.WaitGroup
	errs := make(chan error, requestCount)
	start := make(chan struct{})
	for range requestCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL, nil)
			if err != nil {
				errs <- err
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				errs <- err
				return
			}
			defer retry.DrainAndClose(req.Context(), resp)
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("unexpected status %d", resp.StatusCode)
			}
		}()
	}
	close(start)

	// Wait until every request has taken its own unauthenticated 401, by which
	// point each has entered the token acquisition phase. The blocked token
	// endpoint parks the first fetch, so the cache must coalesce the rest.
	select {
	case <-allChallenged:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for all requests to be challenged")
	}
	// Give the stragglers time to reach cache.Set; a non-coalescing cache would
	// show up as additional blocked arrivals at the token endpoint.
	time.Sleep(100 * time.Millisecond)
	if got := tokenFetches.Load(); got != 1 {
		t.Errorf("expected the shared cache to coalesce to 1 token fetch, got %d", got)
	}
	close(releaseToken)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent request failed: %v", err)
		}
	}
	if got := registrySuccesses.Load(); got != requestCount {
		t.Errorf("expected %d independent successful requests, got %d", requestCount, got)
	}
	if got := tokenFetches.Load(); got != 1 {
		t.Errorf("expected a single shared token fetch, got %d", got)
	}
	// every request still issued its own unauthenticated attempt first
	if got := registryChallenges.Load(); got != requestCount {
		t.Errorf("expected %d independent challenges, got %d", requestCount, got)
	}
}
