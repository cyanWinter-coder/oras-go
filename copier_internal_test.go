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

package oras

import (
	"errors"
	"net/http"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/oras-project/oras-go/v3/registry/remote/errcode"
)

func TestIsSourceAuthError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "unrelated error",
			err:  errors.New("some other error"),
			want: false,
		},
		{
			name: "401 error response",
			err: &errcode.ErrorResponse{
				StatusCode: http.StatusUnauthorized,
			},
			want: true,
		},
		{
			name: "403 error response",
			err: &errcode.ErrorResponse{
				StatusCode: http.StatusForbidden,
				Errors:     errcode.Errors{{Code: errcode.ErrorCodeDenied}},
			},
			want: false,
		},
		{
			name: "single UNAUTHORIZED",
			err:  errcode.Error{Code: errcode.ErrorCodeUnauthorized},
			want: true,
		},
		{
			name: "multiple errors with UNAUTHORIZED",
			err: errcode.Errors{
				{Code: errcode.ErrorCodeNameUnknown},
				{Code: errcode.ErrorCodeUnauthorized},
			},
			want: true,
		},
		{
			name: "multiple errors without UNAUTHORIZED",
			err: errcode.Errors{
				{Code: errcode.ErrorCodeNameUnknown},
				{Code: errcode.ErrorCodeDenied},
			},
			want: false,
		},
		{
			name: "source CopyError wrapping 401",
			err: newCopyError("Fetch", CopyErrorOriginSource, ocispec.Descriptor{}, &errcode.ErrorResponse{
				StatusCode: http.StatusUnauthorized,
			}),
			want: true,
		},
		{
			name: "destination CopyError wrapping 401",
			err: newCopyError("Push", CopyErrorOriginDestination, ocispec.Descriptor{}, &errcode.ErrorResponse{
				StatusCode: http.StatusUnauthorized,
			}),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSourceAuthError(tt.err); got != tt.want {
				t.Errorf("isSourceAuthError() = %v, want %v", got, tt.want)
			}
		})
	}
}
