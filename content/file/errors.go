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

import "errors"

var (
	ErrMissingName             = errors.New("missing name")
	ErrDuplicateName           = errors.New("duplicate name")
	ErrPathTraversalDisallowed = errors.New("path traversal disallowed")
	ErrOverwriteDisallowed     = errors.New("overwrite disallowed")
	ErrStoreClosed             = errors.New("store already closed")
	ErrInvalidStoreOptions     = errors.New("invalid store options")

	// ErrFileChanged is returned by [Store.Add] when a file or directory is
	// modified, replaced or deleted between the moment its packing snapshot is
	// taken and the moment its content is read into the layer. The descriptor of
	// such a layer cannot be trusted to match the packed content, so packing
	// fails instead of publishing it. Re-run [Store.Add] against a stable tree.
	ErrFileChanged = errors.New("file changed during packing")

	// ErrDuplicatePath is returned by [Store.Add] when two entries in the
	// packed tree map to the same archive path. This can only be caused by a
	// race while the snapshot is taken (for example an entry is renamed onto an
	// existing path), since a clean directory tree has unique paths.
	ErrDuplicatePath = errors.New("duplicate path in packing snapshot")

	// ErrSymlinkEscapes is returned by [Store.Add] when a symbolic link in the
	// packed tree points outside the packed root, either through an absolute
	// target or through a ".." traversal. Such a link cannot be validated to
	// stay inside the tree and is rejected before any layer content is written.
	ErrSymlinkEscapes = errors.New("symbolic link escapes the packing root")
)

var errSkipUnnamed = errors.New("unnamed descriptor skipped")
