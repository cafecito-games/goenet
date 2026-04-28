package timeutil_test

import (
	"testing"

	"github.com/cafecito-games/goenet/internal/timeutil"
)

func TestDifferenceWrapsLikeENet(t *testing.T) {
	const a uint32 = 10
	const b uint32 = 86400000 - 5

	if got := timeutil.Difference(a, b); got != 15 {
		t.Fatalf("Difference() = %d, want 15", got)
	}
}
