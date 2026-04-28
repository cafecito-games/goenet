package goenet

// Buffer is one byte slice passed to checksum and compression hooks.
type Buffer struct {
	Data []byte
}

// Checksummer computes a checksum across the provided buffers.
type Checksummer interface {
	Checksum(buffers []Buffer) uint32
}
