package timeutil_test

import (
	"testing"

	"github.com/cafecito-games/goenet/internal/timeutil"
	"github.com/stretchr/testify/assert"
)

func TestDifferenceMatchesENet(t *testing.T) {
	tests := []struct {
		name string
		a, b uint32
		want uint32
	}{
		{name: "forward without wrap", a: 2000, b: 1000, want: 1000},
		{name: "reverse within overflow window", a: 1000, b: 2000, want: 1000},
		{name: "across wrap boundary", a: 10, b: timeutil.TimeOverflow - 5, want: 86399985},
		{name: "equal", a: 1000, b: 1000, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, timeutil.Difference(tc.a, tc.b))
		})
	}
}

func TestLessAcrossWrap(t *testing.T) {
	cases := []struct {
		name string
		a, b uint32
		want bool
	}{
		{"a<b within window", 100, 200, true},
		{"a>b within window", 200, 100, false},
		{"equal", 100, 100, false},
		// Across uint32 wrap: maxUint32 is "just before" 5 in ENet's overflow-safe ordering.
		{"wrap maxUint32 less than small", ^uint32(0), 5, true},
		// Inside the overflow distance the comparison should track the longer direction:
		// a=5, b=TimeOverflow+10 places b a long way "after" a, so a < b holds.
		{"wrap inside window", 5, timeutil.TimeOverflow + 10, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, timeutil.Less(tc.a, tc.b))
		})
	}
}
