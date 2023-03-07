package common

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestGenerateShards(t *testing.T) {
	type args struct {
		numShards uint32
	}
	tests := []struct {
		name string
		args args
		want []Shard
	}{
		{"1", args{1}, []Shard{
			{0, 0, math.MaxUint32},
		}},
		{"2", args{2}, []Shard{
			{0, 0, 2147483647},
			{1, 2147483648, math.MaxUint32},
		}},
		{"3", args{3}, []Shard{
			{0, 0, 1431655765},
			{1, 1431655766, 2863311531},
			{2, 2863311532, math.MaxUint32},
		}},
		{"4", args{4}, []Shard{
			{0, 0, 1073741823},
			{1, 1073741824, 2147483647},
			{2, 2147483648, 3221225471},
			{3, 3221225472, math.MaxUint32},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, GenerateShards(tt.args.numShards), "GenerateShards(%v)", tt.args.numShards)
		})
	}
}
