//go:build !unix

package wal

import (
	"github.com/spf13/afero"
)

func doFSync(file afero.File) error {
	return l.sfile.Sync()
}
