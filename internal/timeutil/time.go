package timeutil

const TimeOverflow uint32 = 86400000

// Difference matches ENet's overflow-safe millisecond arithmetic.
func Difference(a, b uint32) uint32 {
	if a >= b {
		return a - b
	}

	return TimeOverflow - b + a
}
