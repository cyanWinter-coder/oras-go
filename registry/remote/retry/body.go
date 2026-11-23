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
	"context"
	"io"
	"net/http"
)

// maxDrainBytes bounds how much of an abandoned response body is read before
// closing the connection. A registry error body fits in a few KiB, so a body
// larger than this is either streaming content or an abnormal peer; in both
// cases giving up the connection costs less than draining it. The value mirrors
// the limit the standard library uses when it drains before putting a
// connection back to the idle pool.
const maxDrainBytes = 256 * 1024 // 256 KiB

// DrainAndClose drains up to maxDrainBytes of resp.Body and then closes it.
//
// Closing a response body without reading it to EOF makes the underlying
// transport tear down the TCP connection instead of returning it to the idle
// pool, so the next request pays for a new handshake even though the existing
// connection is healthy. Draining first lets the connection be reused. The
// drain is bounded so a peer that streams an unbounded body, or stalls while
// the request context is done, cannot hold the caller: once the limit or an
// error (including context cancellation) is reached the body is closed and the
// connection is discarded rather than reused.
//
// It is safe to call on a nil body. The close error is intentionally dropped:
// the caller has no use for the body anymore and a drain failure already means
// the connection will not be reused.
func DrainAndClose(ctx context.Context, resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	drainBody(ctx, resp.Body)
	resp.Body.Close()
}

// drainBody reads up to maxDrainBytes from body, stopping early when ctx is
// cancelled or the reader errors. It never returns an error: any failure simply
// means the connection is not safe to reuse and will be closed by the caller.
func drainBody(ctx context.Context, body io.ReadCloser) {
	if ctx == nil {
		ctx = context.Background()
	}
	buf := make([]byte, 1024)
	var total int64
	for total < maxDrainBytes {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := body.Read(buf)
		total += int64(n)
		if err != nil {
			return
		}
	}
}
