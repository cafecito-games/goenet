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
