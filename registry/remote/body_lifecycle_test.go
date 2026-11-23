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

package remote

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/oras-project/oras-go/v3/registry/remote/auth"
)

// drainTrackingBody wraps a response body and records whether it was read to
// EOF before Close. A connection whose response body is closed without reaching
// EOF cannot be returned to the idle pool.
type drainTrackingBody struct {
	io.ReadCloser
	readToEOF bool
}

func (b *drainTrackingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.readToEOF = true
	}
	return n, err
}

// drainTrackingTransport wraps registry 401 responses so a test can assert the
// chunked layer drained them before closing.
type drainTrackingTransport struct {
	base       http.RoundTripper
	mu         sync.Mutex
	challenges []*drainTrackingBody
}

func (t *drainTrackingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		return resp, err
	}
	tracker := &drainTrackingBody{ReadCloser: resp.Body}
	resp.Body = tracker
	t.mu.Lock()
	t.challenges = append(t.challenges, tracker)
	t.mu.Unlock()
	return resp, nil
}

func (t *drainTrackingTransport) allDrained() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.challenges) == 0 {
		return false
	}
	for _, c := range t.challenges {
		if !c.readToEOF {
			return false
		}
	}
	return true
}

// TestRepository_Push_Chunked401DrainsResponse proves the response lifecycle
// boundary in sendChunkRequest. The session POST authenticates and caches its
// token on the upload; the first PATCH presents that cached token, which the
// registry rejects with 401 inside the chunked layer (the request already
// carried Authorization). That response is never returned to the caller, so its
// body must be read to EOF before the rebuilt, re-authenticated PATCH; closing
// it unread would discard the connection.
func TestRepository_Push_Chunked401DrainsResponse(t *testing.T) {
	const session = "/v2/test/blobs/uploads/session"
	const firstToken = "first-token"
	const refreshedToken = "refreshed-token"
	const bodySize = 4096

	var mu sync.Mutex
	currentToken := firstToken
	rotateOnFirstPatch := true

	// token endpoint on a separate host, as in the production token flow
	authSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		tok := currentToken
		mu.Unlock()
		fmt.Fprintf(w, `{"access_token":%q}`, tok)
	}))
	defer authSrv.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		challenge := func() {
			w.Header().Set("Www-Authenticate",
				fmt.Sprintf(`Bearer realm=%q,service=%q,scope=%q`,
					authSrv.URL, r.Host, "repository:test:pull,push"))
			w.WriteHeader(http.StatusUnauthorized)
		}
		// rotate the accepted token exactly once, just before the first PATCH,
		// so the credential cached from the POST is stale on that PATCH
		if r.Method == http.MethodPatch {
			mu.Lock()
			if rotateOnFirstPatch {
				rotateOnFirstPatch = false
				currentToken = refreshedToken
			}
			mu.Unlock()
		}
		switch r.Method {
		case http.MethodPost:
			if r.Header.Get("Authorization") != "Bearer "+firstToken {
				challenge()
				return
			}
			w.Header().Set("Location", session)
			w.WriteHeader(http.StatusAccepted)
		case http.MethodPatch:
			// any non-matching bearer is challenged, so the rebuilt,
			// unauthenticated PATCH can fetch the rotated token and retry
			if r.Header.Get("Authorization") != "Bearer "+currentToken {
				challenge()
				// an unread body the client must drain before the rebuilt PATCH
				_, _ = w.Write(bytes.Repeat([]byte("q"), bodySize))
				return
			}
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Location", session)
			if cr := r.Header.Get("Content-Range"); cr != "" {
				if _, rng, ok := bytes.Cut([]byte(cr), []byte(" ")); ok {
					w.Header().Set("Range", "bytes="+string(rng))
				}
			}
			w.WriteHeader(http.StatusAccepted)
		case http.MethodPut:
			if r.Header.Get("Authorization") != "Bearer "+currentToken {
				challenge()
				return
			}
			w.Header().Set("Docker-Content-Digest", digest.FromString("hello").String())
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	uri, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("invalid test http server: %v", err)
	}
	tracker := &drainTrackingTransport{base: http.DefaultTransport.(*http.Transport).Clone()}
	repo, err := NewRepository(uri.Host + "/test")
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	repo.Registry.PlainHTTP = true
	// no auth cache so the rebuilt PATCH re-fetches the rotated token
	repo.Registry.Client = &auth.Client{Client: &http.Client{Transport: tracker}}
	repo.MaxChunkSize = 2

	blob := []byte("hello")
	desc := ocispec.Descriptor{
		MediaType: "application/octet-stream",
		Digest:    digest.FromBytes(blob),
		Size:      int64(len(blob)),
	}
	if err := repo.Blobs().Push(context.Background(), desc, bytes.NewReader(blob)); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	if len(tracker.challenges) == 0 {
		t.Fatal("the chunked 401 path was not exercised; the test setup is wrong")
	}
	if !tracker.allDrained() {
		t.Errorf("the 401 response body was closed before being read to EOF")
	}
}
