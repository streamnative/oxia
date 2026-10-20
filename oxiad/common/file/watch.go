// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package file

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"

	"github.com/fsnotify/fsnotify"

	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/common/failpoint"
)

// WatchFile signals each time the file at path is created, written, renamed
// or removed. The channel is closed when the watch fails or ctx is done.
func WatchFile(ctx context.Context, path string) (<-chan struct{}, error) {
	return watchFile(ctx, path, nil)
}

func watchFile(ctx context.Context, path string, failpoints *failpoint.Failpoints) (<-chan struct{}, error) {
	logger := slog.With(slog.String("component", "file-watch"), slog.String("path", path))
	watcher, err := fsnotify.NewWatcher()
	if err = failpoints.Fail("watchFile.newWatcher", err); err != nil {
		if watcher != nil {
			err = errors.Join(err, watcher.Close())
		}
		return nil, err
	}
	if err := watcher.Add(filepath.Dir(path)); err != nil {
		return nil, errors.Join(err, watcher.Close())
	}

	changes := make(chan struct{}, 1)
	go func() {
		defer close(changes)
		defer func() {
			if err := failpoints.Fail("watchFile.close", watcher.Close()); err != nil {
				logger.Warn("Failed to close file watcher", slog.Any("error", err))
			}
		}()
		watchedPath := filepath.Clean(path)
		// The watcher closes its channels only once it is closed, when this
		// loop has returned
		for {
			var err error
			select {
			case <-ctx.Done():
				return
			case err = <-watcher.Errors:
			case event := <-watcher.Events:
				if err = failpoints.Trigger("watchFile.event"); err == nil {
					if filepath.Clean(event.Name) == watchedPath &&
						event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename|fsnotify.Remove) != 0 {
						channel.PushNoBlock(changes, struct{}{})
					}
					continue
				}
			}
			logger.Warn("File watch failed", slog.Any("error", err))
			return
		}
	}()
	return changes, nil
}
