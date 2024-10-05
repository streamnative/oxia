package codec

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestV1_GetHeaderSize(t *testing.T) {
	assert.EqualValues(t, v1.GetHeaderSize(), 4)
}

func TestV1_Codec(t *testing.T) {
	buf := make([]byte, 100)
	payload := []byte{1}
	recordSize, _ := v1.WriteRecord(buf, 0, 0, payload)
	assert.EqualValues(t, recordSize, 5)
	getRecordSize, err := v1.GetRecordSize(buf, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, getRecordSize, recordSize)

	getPayload, err := v1.ReadRecordWithValidation(buf, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload, getPayload)
}
