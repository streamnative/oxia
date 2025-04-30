package channel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/streamnative/oxia/common/entities"
	"github.com/stretchr/testify/assert"
)

func TestReadAll(t *testing.T) {
	type testItem struct {
		value int
	}

	ErrUnknown := errors.New("error unknown")
	tests := []struct {
		name        string
		setupFunc   func() (context.Context, chan *entities.TWithError[testItem])
		expected    []testItem
		expectedErr error
	}{
		{
			name: "ReadAll completes successfully",
			setupFunc: func() (context.Context, chan *entities.TWithError[testItem]) {
				ch := make(chan *entities.TWithError[testItem], 3)
				ch <- &entities.TWithError[testItem]{T: testItem{value: 1}}
				ch <- &entities.TWithError[testItem]{T: testItem{value: 2}}
				ch <- &entities.TWithError[testItem]{T: testItem{value: 3}}
				close(ch)
				return context.Background(), ch
			},
			expected:    []testItem{{1}, {2}, {3}},
			expectedErr: nil,
		},
		{
			name: "Channel is empty and closed",
			setupFunc: func() (context.Context, chan *entities.TWithError[testItem]) {
				ch := make(chan *entities.TWithError[testItem])
				close(ch)
				return context.Background(), ch
			},
			expected:    []testItem{},
			expectedErr: nil,
		},
		{
			name: "Context is canceled before reading",
			setupFunc: func() (context.Context, chan *entities.TWithError[testItem]) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				ch := make(chan *entities.TWithError[testItem])
				return ctx, ch
			},
			expected:    nil,
			expectedErr: context.Canceled,
		},
		{
			name: "Context times out during read",
			setupFunc: func() (context.Context, chan *entities.TWithError[testItem]) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				time.Sleep(20 * time.Millisecond)
				cancel()
				ch := make(chan *entities.TWithError[testItem])
				return ctx, ch
			},
			expected:    nil,
			expectedErr: context.DeadlineExceeded,
		},
		{
			name: "Read until error in channel",
			setupFunc: func() (context.Context, chan *entities.TWithError[testItem]) {
				ch := make(chan *entities.TWithError[testItem], 3)
				ch <- &entities.TWithError[testItem]{T: testItem{value: 1}}
				ch <- &entities.TWithError[testItem]{T: testItem{value: 2}, Err: ErrUnknown}
				close(ch)
				return context.Background(), ch
			},
			expected:    nil,
			expectedErr: ErrUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, ch := tt.setupFunc()
			result, err := ReadAll(ctx, ch)
			assert.ErrorIs(t, err, tt.expectedErr)
			assert.Equal(t, len(result), len(tt.expected))
			for i, v := range result {
				assert.Equal(t, v, tt.expected[i])
			}
		})
	}
}
