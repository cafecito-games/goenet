package engine

import "testing"

func TestPeerSessionIDForIndexUsesSeededRotation(t *testing.T) {
	tests := []struct {
		index int
		seed  uint8
		want  uint8
	}{
		{index: 0, seed: 0, want: 1},
		{index: 1, seed: 0, want: 2},
		{index: 2, seed: 0, want: 3},
		{index: 0, seed: 1, want: 2},
		{index: 1, seed: 1, want: 3},
		{index: 2, seed: 1, want: 1},
		{index: 0, seed: 2, want: 3},
		{index: 1, seed: 2, want: 1},
		{index: 2, seed: 2, want: 2},
	}

	for _, tt := range tests {
		if got := peerSessionIDForIndex(tt.index, tt.seed); got != tt.want {
			t.Fatalf("peerSessionIDForIndex(%d, %d) = %d, want %d", tt.index, tt.seed, got, tt.want)
		}
	}
}
