//go:build unix

package wal

import (
	"github.com/spf13/afero"
	"golang.org/x/sys/unix"
	"os"
)

func doFSync(file afero.File) error {
	if f, ok := file.(*os.File); ok {
		return unix.Fsync(int(f.Fd()))
	} else {
		return file.Sync()
	}
}
