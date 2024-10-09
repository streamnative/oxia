package wal

import (
	"github.com/pkg/errors"
	"github.com/streamnative/oxia/server/wal/codec"
	"os"
)

func WriteIndex(path string, index []byte, _codec codec.Codec) error {
	idxFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return errors.Wrapf(err, "failed to open index file %s", path)
	}

	if err = _codec.WriteIndex(idxFile, index); err != nil {
		return errors.Wrapf(err, "failed write index file %s", path)
	}
	return idxFile.Close()
}
