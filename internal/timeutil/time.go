package timeutil

const TimeOverflow uint32 = 86400000

// Difference matches ENet's overflow-safe millisecond arithmetic.
func Difference(a, b uint32) uint32 {
	if a-b >= TimeOverflow {
		return b - a
	}

	return a - b
}
