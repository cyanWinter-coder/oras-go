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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oras-project/oras-go/v3/registry/remote/credentials"
	"github.com/oras-project/oras-go/v3/registry/remote/properties"
	"github.com/oras-project/oras-go/v3/registry/remote/retry"
)

// newRefreshTestClient builds an auth.Client with the given cache (nil means
// no cache) and a credential that always resolves to a username/password pair.
// The composite token fetcher runs in legacy distribution-spec mode, so a
// rejected token fetch is not retried through the OAuth2 fallback and stays a
// single token-endpoint hit.
func newRefreshTestClient(t *testing.T, cache Cache) *Client {
	t.Helper()
	client := &Client{
		Client: &http.Client{
			Transport: retry.NewTransport(http.DefaultTransport.(*http.Transport).Clone()),
		},
		CredentialFunc: func(ctx context.Context, res properties.Resource) (credentials.Credential, error) {
			return credentials.Credential{Username: "user", Password: "pass"}, nil
		},
		Cache: cache,
	}
	client.TokenFetcher = NewCompositeTokenFetcher(client.Client, client.Header, client.ClientID, true)
	return client
}

// TestClient_Do_ConcurrentExpiryCoalescesWithoutCache proves requirement 1 for
// clients without a cache: many concurrent requests whose tokens expire
// together are challenged at the same time, and their credential refreshes
// coalesce into a single shared token fetch even though the client has no
// cache to fall back on.
func TestClient_Do_ConcurrentExpiryCoalescesWithoutCache(t *testing.T) {
	const requestCount = 25
	const accessToken = "shared/fresh/token"

	var tokenFetches atomic.Int32
	var registrySuccesses atomic.Int32
	var challenges atomic.Int32

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
			if challenges.Add(1) == requestCount {
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

	// nil cache: without the client-level refresh coalescing every request
	// would start its own fetch and block on the token endpoint.
	client := newRefreshTestClient(t, nil)

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
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("unexpected status %d", resp.StatusCode)
			}
		}()
	}
	close(start)

	select {
	case <-allChallenged:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for all requests to be challenged")
	}
	// Let the stragglers reach the refresh group. Any fetch the group does not
	// coalesce would itself block on the token endpoint and be counted.
	time.Sleep(100 * time.Millisecond)
	if got := tokenFetches.Load(); got != 1 {
		t.Errorf("expected a single shared refresh without a cache, got %d fetches", got)
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
		t.Errorf("expected %d successes, got %d", requestCount, got)
	}
	if got := tokenFetches.Load(); got != 1 {
		t.Errorf("expected a single shared refresh, got %d", got)
	}
}

// TestClient_Do_CancellingOneWaiterDoesNotCancelSharedRefresh proves
// requirement 5: when several callers wait on one shared refresh, cancelling
// one caller's context removes only its own wait. The shared fetch keeps
// running, the other callers receive the shared token, and a follow-up request
// from the cancelled caller reuses the result of the fetch it abandoned.
func TestClient_Do_CancellingOneWaiterDoesNotCancelSharedRefresh(t *testing.T) {
	const accessToken = "uncancellable/token"

	var tokenFetches atomic.Int32
	fetchStarted := make(chan struct{})
	releaseToken := make(chan struct{})

	as := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenFetches.Add(1)
		close(fetchStarted)
		<-releaseToken
		fmt.Fprintf(w, `{"access_token":%q}`, accessToken)
	}))
	defer as.Close()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+accessToken {
			w.Header().Set("Www-Authenticate",
				fmt.Sprintf(`Bearer realm=%q,service=%q`, as.URL, r.Host))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := newRefreshTestClient(t, NewCache())

	// two requests start together and must join the same refresh
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2 := context.Background()
	errs := make(chan error, 2)
	start := make(chan struct{})
	go func() {
		<-start
		req, err := http.NewRequestWithContext(ctx1, http.MethodGet, ts.URL, nil)
		if err != nil {
			errs <- err
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			errs <- err
			return
		}
		resp.Body.Close()
		errs <- nil
	}()
	go func() {
		<-start
		req, err := http.NewRequestWithContext(ctx2, http.MethodGet, ts.URL, nil)
		if err != nil {
			errs <- err
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			errs <- err
			return
		}
		resp.Body.Close()
		errs <- nil
	}()
	close(start)

	// wait until the single shared fetch is in flight, then cancel only the
	// first caller
	select {
	case <-fetchStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the shared refresh to start")
	}
	cancel1()

	// the cancelled caller must report its own cancellation and not be able to
	// take the fetch down with it
	select {
	case err := <-errs:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("cancelled waiter error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled waiter did not return")
	}

	// release the fetch; the other waiter must complete successfully with the
	// shared result, and no second fetch may have started
	close(releaseToken)
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("remaining waiter failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("remaining waiter did not complete")
	}
	if got := tokenFetches.Load(); got != 1 {
		t.Errorf("expected exactly one shared fetch, got %d", got)
	}

	// the cancelled caller retries after the shared refresh has completed; it
	// must reuse the cached token rather than trigger another refresh.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL, nil)
	if err != nil {
		t.Fatalf("failed to create retry request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("retry after cancellation failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retry status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := tokenFetches.Load(); got != 1 {
		t.Errorf("retry triggered another fetch: got %d, want 1", got)
	}
}

// TestClient_Do_RefreshFailureIsSharedAndRetried proves that a failed shared
// refresh hands the same error to every waiter, and a later request starts a
// fresh generation rather than being poisoned by the failed one.
func TestClient_Do_RefreshFailureIsSharedAndRetried(t *testing.T) {
	const waiterCount = 5
	var tokenFetches atomic.Int32
	var failRefresh atomic.Bool
	failRefresh.Store(true)

	// The token endpoint blocks until every waiter has been challenged and
	// joined the refresh group, so a fetch the group does not coalesce would
	// itself block here and be counted deterministically.
	allChallenged := make(chan struct{})
	releaseToken := make(chan struct{})
	var challenges atomic.Int32

	as := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenFetches.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		<-releaseToken
		if failRefresh.Load() {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fmt.Fprintf(w, `{"access_token":"recovered/token"}`)
	}))
	defer as.Close()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer recovered/token" {
			if challenges.Add(1) == waiterCount {
				close(allChallenged)
			}
			w.Header().Set("Www-Authenticate",
				fmt.Sprintf(`Bearer realm=%q,service=%q`, as.URL, r.Host))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := newRefreshTestClient(t, NewCache())
	var wg sync.WaitGroup
	errs := make(chan error, waiterCount)
	start := make(chan struct{})
	for range waiterCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL, nil)
			if err != nil {
				errs <- err
				return
			}
			_, err = client.Do(req)
			errs <- err
		}()
	}
	close(start)

	select {
	case <-allChallenged:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for all waiters to be challenged")
	}
	// let stragglers reach the refresh group before releasing the blocked
	// fetch; an uncoalesced fetch would block on the token endpoint as well
	time.Sleep(100 * time.Millisecond)
	if got := tokenFetches.Load(); got != 1 {
		t.Errorf("expected the failed refresh to be shared, got %d fetches", got)
	}
	close(releaseToken)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err == nil {
			t.Error("expected every waiter to receive the shared refresh error")
		}
	}
	if got := tokenFetches.Load(); got != 1 {
		t.Errorf("expected the failed refresh to be shared, got %d fetches", got)
	}

	// the failed generation must not block the next one
	failRefresh.Store(false)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL, nil)
	if err != nil {
		t.Fatalf("failed to create retry request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request after failed refresh failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := tokenFetches.Load(); got != 2 {
		t.Errorf("expected one retry fetch, got %d", got)
	}
}

// TestClient_Do_AuthenticationLoopFailsAfterOneRetry proves requirement 2: a
// registry that keeps challenging even a request carrying freshly fetched
// credentials causes exactly one authenticated retry and then a loop error,
// never an unbounded sequence of refreshes.
func TestClient_Do_AuthenticationLoopFailsAfterOneRetry(t *testing.T) {
	var registryHits atomic.Int32
	var tokenFetches atomic.Int32

	as := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenFetches.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		fmt.Fprintf(w, `{"access_token":"doomed/token"}`)
	}))
	defer as.Close()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registryHits.Add(1)
		// every token is rejected, so the authenticated retry is challenged
		// again
		w.Header().Set("Www-Authenticate",
			fmt.Sprintf(`Bearer realm=%q,service=%q`, as.URL, r.Host))
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer ts.Close()

	client := newRefreshTestClient(t, NewCache())
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	resp, err := client.Do(req)
	if !errors.Is(err, ErrAuthenticationLoop) {
		t.Fatalf("Client.Do() error = %v, wantErr %v", err, ErrAuthenticationLoop)
	}
	if resp != nil {
		t.Errorf("expected nil response on loop, got %v", resp)
	}
	// one unauthenticated attempt and one authenticated retry, nothing more
	if got := registryHits.Load(); got != 2 {
		t.Errorf("expected 2 registry requests, got %d", got)
	}
	if got := tokenFetches.Load(); got != 1 {
		t.Errorf("expected 1 token fetch, got %d", got)
	}
}

// TestClient_Do_NonRewindableBodyFailsExplicitlyOnChallenge proves requirement
// 4: a challenged request whose body cannot be replayed fails with
// ErrBodyNotRewindable instead of silently sending an unauthenticated or
// bodiless retry.
func TestClient_Do_NonRewindableBodyFailsExplicitlyOnChallenge(t *testing.T) {
	var registryHits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registryHits.Add(1)
		w.Header().Set("Www-Authenticate", `Basic realm="Test Server"`)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer ts.Close()

	client := newRefreshTestClient(t, NewCache())
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodPut, ts.URL+"/v2/test/blobs/uploads/",
		io.NopCloser(strings.NewReader("payload-bytes")))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	resp, err := client.Do(req)
	if !errors.Is(err, ErrBodyNotRewindable) {
		t.Fatalf("Client.Do() error = %v, wantErr %v", err, ErrBodyNotRewindable)
	}
	if resp != nil {
		t.Errorf("expected nil response, got %v", resp)
	}
	if got := registryHits.Load(); got != 1 {
		t.Errorf("expected only the original request, got %d registry hits", got)
	}
}

// TestClient_Do_RefreshOverwritesTokenGeneration proves requirement 3: tokens
// are keyed by challenge scope, and a successful refresh for a scope replaces
// the stale generation so subsequent requests with the same scope send the new
// token without another fetch, while a different scope gets its own entry.
func TestClient_Do_RefreshOverwritesTokenGeneration(t *testing.T) {
	const scopeA = "repository:repo:pull"
	const scopeB = "repository:repo:pull,push"
	var tokenVersion atomic.Int32
	var tokenFetches atomic.Int32

	as := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenFetches.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		version := tokenVersion.Add(1)
		fmt.Fprintf(w, `{"access_token":%q}`, fmt.Sprintf("token-%d", version))
	}))
	defer as.Close()

	// every token minted by the authorization service is accepted; the seeded
	// stale token is not, so its first use is turned into a refresh.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer token-") {
			w.Header().Set("Www-Authenticate",
				fmt.Sprintf(`Bearer realm=%q,service=%q,scope=%q`, as.URL, r.Host, scopeA))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	uri, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("invalid test server: %v", err)
	}

	cache := NewCache()
	client := newRefreshTestClient(t, cache)
	ctx := withScopesForHost(context.Background(), uri.Host, scopeA)

	// seed a stale generation under scope A
	staleKey := bearerCacheKey(mustParseScopes(scopeA))
	res := properties.Resource{Registry: canonicalRegistry(uri.Host)}
	if _, err := cache.Set(ctx, res.String(), SchemeBearer, staleKey, func(context.Context) (string, error) {
		return "stale-token", nil
	}); err != nil {
		t.Fatalf("cache.Set() error = %v", err)
	}

	// first real request refreshes and the registry accepts the new token
	token1 := doRefreshRoundTrip(t, client, ctx, ts.URL)
	if got := tokenFetches.Load(); got != 1 {
		t.Fatalf("expected 1 fetch after refresh, got %d", got)
	}

	// a second request with the same scope must reuse the refreshed generation
	token2 := doRefreshRoundTrip(t, client, ctx, ts.URL)
	if token2 != token1 {
		t.Errorf("expected cached token %q to be reused, got %q", token1, token2)
	}
	if got := tokenFetches.Load(); got != 1 {
		t.Errorf("expected no extra fetch for the same scope, got %d", got)
	}

	// a different scope gets its own token entry rather than replaying scope
	// A's token
	ctxB := withScopesForHost(context.Background(), uri.Host, scopeB)
	token3 := doRefreshRoundTrip(t, client, ctxB, ts.URL)
	if token3 == token1 {
		t.Errorf("expected a distinct token for scope %q", scopeB)
	}
	if got := tokenFetches.Load(); got != 2 {
		t.Errorf("expected a fetch for the new scope, got %d total", got)
	}

	// scope A's entry is untouched by scope B's refresh
	token4 := doRefreshRoundTrip(t, client, ctx, ts.URL)
	if token4 != token1 {
		t.Errorf("scope A token changed after scope B refresh: got %q, want %q", token4, token1)
	}
}

// doRefreshRoundTrip performs one GET and returns the bearer token the client
// authenticated with on its successful retry.
func doRefreshRoundTrip(t *testing.T, client *Client, ctx context.Context, target string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Client.Do() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	auth := resp.Request.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(auth, prefix) {
		t.Fatalf("authenticated request missing bearer token: %q", auth)
	}
	return auth[len(prefix):]
}
