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

package errutil

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/oras-project/oras-go/v3/registry/remote/errcode"
)

// maxErrorBytes specifies the default limit on how many response bytes are
// allowed in the server's error response.
// A typical error message is around 200 bytes. Hence, 8 KiB should be
// sufficient.
const maxErrorBytes int64 = 8 * 1024 // 8 KiB

// headerRetryAfter is the header key for Retry-After.
const headerRetryAfter = "Retry-After"

// ParseErrorResponse parses the error returned by the remote registry.
func ParseErrorResponse(resp *http.Response) error {
	resultErr := &errcode.ErrorResponse{
		Method:     resp.Request.Method,
		URL:        resp.Request.URL,
		StatusCode: resp.StatusCode,
		RetryAfter: parseRetryAfter(resp),
	}
	var body struct {
		Errors errcode.Errors `json:"errors"`
	}
	lr := io.LimitReader(resp.Body, maxErrorBytes)
	if err := json.NewDecoder(lr).Decode(&body); err == nil {
		resultErr.Errors = body.Errors
	}
	return resultErr
}

// parseRetryAfter parses the Retry-After header of resp, handling both the
// delta-seconds and the HTTP-date forms. It returns 0 when the header is
// missing, unparseable, zero, or in the past.
func parseRetryAfter(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	v := resp.Header.Get(headerRetryAfter)
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs <= 0 {
			return 0
		}
		// saturate instead of overflowing time.Duration
		if secs > int64(math.MaxInt64/time.Second) {
			return time.Duration(math.MaxInt64)
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// IsErrorCode returns true if err is an Error and its Code equals to code.
func IsErrorCode(err error, code string) bool {
	var ec errcode.Error
	return errors.As(err, &ec) && ec.Code == code
}
