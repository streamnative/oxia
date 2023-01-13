package oxia

import (
	"errors"
	"io"
)

var (
	//not easy to use as a pointer if a const
	VersionNotExists int64 = -1

	ErrorKeyNotFound       = errors.New("key not found")
	ErrorUnexpectedVersion = errors.New("unexpected version")
	ErrorUnknownStatus     = errors.New("unknown status")
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

	Put(key string, payload []byte, options ...PutOption) (Stat, error)
	Delete(key string, options ...DeleteOption) error
	DeleteRange(minKeyInclusive string, maxKeyExclusive string) error
	Get(key string) ([]byte, Stat, error)
	List(minKeyInclusive string, maxKeyExclusive string) ([]string, error)

	GetNotifications() (Notifications, error)
}

type Stat struct {
	Version           int64
	CreatedTimestamp  uint64
	ModifiedTimestamp uint64
}

type PutResult struct {
	Stat Stat
	Err  error
}

type GetResult struct {
	Payload []byte
	Stat    Stat
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
