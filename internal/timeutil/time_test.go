package timeutil_test

import (
	"testing"

	"github.com/cafecito-games/goenet/internal/timeutil"
)

func TestDifferenceMatchesENetWithoutWrap(t *testing.T) {
	const a uint32 = 1000
	const b uint32 = 2000

	if got := timeutil.Difference(a, b); got != 1000 {
		t.Fatalf("Difference() = %d, want 1000", got)
	}
}

func TestDifferenceMatchesENetMacroAcrossWrapBoundary(t *testing.T) {
	const a uint32 = 10
	const b uint32 = timeutil.TimeOverflow - 5

	if got := timeutil.Difference(a, b); got != 86399985 {
		t.Fatalf("Difference() = %d, want 86399985", got)
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
			if got := timeutil.Less(tc.a, tc.b); got != tc.want {
				t.Errorf("Less(%d,%d) = %v want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
