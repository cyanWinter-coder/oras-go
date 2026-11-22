//go:build unix

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
	"errors"
	"io/fs"
	"os"
	"syscall"
)

// openNoFollow opens path for reading without following a symbolic link at the
// final component. Together with the Lstat re-validation at write time, this
// closes the window in which a regular file captured by the snapshot is
// replaced by a symbolic link before its content is read.
func openNoFollow(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}

// isSymlinkNoFollowError reports whether err is the failure produced by
// openNoFollow when the final component is a symbolic link.
func isSymlinkNoFollowError(err error) bool {
	return errors.Is(err, syscall.ELOOP)
}

// sameFile reports whether two file infos describe the same inode. A replaced
// file that happens to keep the same size and modification time is therefore
// still detected when its inode changes.
func sameFile(a, b fs.FileInfo) bool {
	sa, okA := a.Sys().(*syscall.Stat_t)
	sb, okB := b.Sys().(*syscall.Stat_t)
	if !okA || !okB {
		return true
	}
	return sa.Dev == sb.Dev && sa.Ino == sb.Ino
}
