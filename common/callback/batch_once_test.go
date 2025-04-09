package callback

import "testing"

func TestBatchOnce_FlushByCounter(t *testing.T) {
	streamOnceCb := NewBatchStreamOnce[int](5, 2048, func(t int) int {

	}, func(container []int) error {

	}, func(err error) {

	})

}

func TestBatchOnce_FlushByBytes(t *testing.T) {

}

func TestBatchOnce_Error(t *testing.T) {

}
