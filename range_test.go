package casscanner

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math"
	"reflect"
	"testing"
)

func TestSplitTokenRing(t *testing.T) {
	type args struct {
		nbSplits int
	}

	ptr := func(i int64) *int64 {
		return &i
	}

	for i := 1; i < 100; i++ {
		i := i

		t.Run(fmt.Sprintf("test splitTokenRing(%d)", i), func(t *testing.T) {
			ranges := splitTokenRing(i)

			require.Len(t, ranges, i)

			var zero *int64
			require.Equal(t, ranges[0].from, zero)

			require.Equal(t, ranges[len(ranges)-1].to, ptr(math.MaxInt64))

			for j := 1; j < len(ranges); j++ {
				require.Equal(t, ranges[j-1].to, ranges[j].from)
			}
		})
	}

	tests := []struct {
		name string
		args args
		want []tokenRange
	}{
		{
			name: "1 split",
			args: args{nbSplits: 1},
			want: []tokenRange{
				{from: nil, to: ptr(math.MaxInt64)},
			},
		},
		{
			name: "2 splits",
			args: args{nbSplits: 2},
			want: []tokenRange{
				{from: nil, to: ptr(-1)},
				{from: ptr(-1), to: ptr(math.MaxInt64)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := splitTokenRing(tt.args.nbSplits); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitTokenRing() = %v, want %v", got, tt.want)
			}
		})
	}
}
