package codec

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
)

func TestCodec_GetOrCreate(t *testing.T) {
	baseDir := os.TempDir()
	nonExistFileName := "0"
	v1FileName := "1"
	v2FileName := "2"
	_, err := os.Create(path.Join(baseDir, v1FileName+v1.GetTxnExtension()))
	assert.NoError(t, err)
	_, err = os.Create(path.Join(baseDir, v2FileName+v2.GetTxnExtension()))
	assert.NoError(t, err)

	codec, exist := GetOrCreate(path.Join(baseDir, nonExistFileName))
	assert.EqualValues(t, v2, codec)
	assert.EqualValues(t, false, exist)

	codec, exist = GetOrCreate(path.Join(baseDir, v1FileName))
	assert.EqualValues(t, v1, codec)
	assert.EqualValues(t, true, exist)

	codec, exist = GetOrCreate(path.Join(baseDir, v2FileName))
	assert.EqualValues(t, v2, codec)
	assert.EqualValues(t, true, exist)
}
