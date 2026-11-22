//go:build !unix

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

package file

import (
	"io/fs"
	"os"
)

// openNoFollow opens path for reading. O_NOFOLLOW is not available on this
// platform, so the Lstat re-validation performed by the caller is what
// prevents reading through a freshly planted symbolic link.
func openNoFollow(path string) (*os.File, error) {
	return os.Open(path)
}

// isSymlinkNoFollowError is always false on platforms without O_NOFOLLOW: an
// open failure cannot be caused by the no-follow open mode itself.
func isSymlinkNoFollowError(_ error) bool {
	return false
}

// sameFile has no inode identity to compare on this platform. The mode, size
// and modification time checks performed by the caller remain in effect.
func sameFile(_, _ fs.FileInfo) bool {
	return true
}
