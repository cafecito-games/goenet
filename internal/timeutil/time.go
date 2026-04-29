// Package timeutil mirrors ENet's overflow-safe millisecond time helpers.
package timeutil

// TimeOverflow is ENet's wraparound threshold for millisecond arithmetic.
const TimeOverflow uint32 = 86400000

// Difference matches ENet's overflow-safe millisecond arithmetic.
func Difference(a, b uint32) uint32 {
	if a-b >= TimeOverflow {
		return b - a
	}

	return a - b
}

// Less matches ENet's overflow-safe time ordering.
func Less(a, b uint32) bool {
	return a-b >= TimeOverflow
}
