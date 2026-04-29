package goenet

// Checksummer computes a checksum across the provided buffer slices, treated as a
// single concatenated byte sequence.
type Checksummer interface {
	Checksum(buffers [][]byte) uint32
}

type checksummerAdapter struct {
	inner Checksummer
}

// Checksum adapts the public checksum interface to the internal core form.
func (a checksummerAdapter) Checksum(buffers [][]byte) uint32 {
	return a.inner.Checksum(buffers)
}
