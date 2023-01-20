// Copyright 2023 StreamNative, Inc.
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

package oxia

import (
	"context"
	"errors"
	"io"
)

const (
	VersionNotExists int64 = -1
)

var (
	ErrorKeyNotFound         = errors.New("key not found")
	ErrorUnexpectedVersionId = errors.New("unexpected version id")
	ErrorUnknownStatus       = errors.New("unknown status")
)

type AsyncClient interface {
	io.Closer

	Put(key string, payload []byte, options ...PutOption) <-chan PutResult
	Delete(key string, options ...DeleteOption) <-chan error
	DeleteRange(minKeyInclusive string, maxKeyExclusive string) <-chan error
	Get(key string) <-chan GetResult
	List(minKeyInclusive string, maxKeyExclusive string) <-chan ListResult

	GetNotifications() (Notifications, error)
}

type SyncClient interface {
	io.Closer

	Put(ctx context.Context, key string, payload []byte, options ...PutOption) (Version, error)
	Delete(ctx context.Context, key string, options ...DeleteOption) error
	DeleteRange(ctx context.Context, minKeyInclusive string, maxKeyExclusive string) error
	Get(ctx context.Context, key string) ([]byte, Version, error)
	List(ctx context.Context, minKeyInclusive string, maxKeyExclusive string) ([]string, error)

	GetNotifications() (Notifications, error)
}

type Version struct {
	VersionId          int64
	CreatedTimestamp   uint64
	ModifiedTimestamp  uint64
	ModificationsCount int64
}

type PutResult struct {
	Version Version
	Err     error
}

type GetResult struct {
	Payload []byte
	Version Version
	Err     error
}

type ListResult struct {
	Keys []string
	Err  error
}

type Notifications interface {
	io.Closer

	Ch() <-chan *Notification
}

type NotificationType int

const (
	KeyCreated NotificationType = iota
	KeyModified
	KeyDeleted
)

func (n NotificationType) String() string {
	switch n {
	case KeyCreated:
		return "KeyCreated"
	case KeyModified:
		return "KeyModified"
	case KeyDeleted:
		return "KeyDeleted"
	}

	return "Unknown"
}

type Notification struct {
	Type    NotificationType
	Key     string
	Version int64
}
