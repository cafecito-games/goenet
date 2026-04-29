package goenet

import "github.com/cafecito-games/goenet/internal/core"

// Buffer is one byte slice passed to checksum and compression hooks.
type Buffer struct {
	Data []byte
}

// Checksummer computes a checksum across the provided buffers.
type Checksummer interface {
	Checksum(buffers []Buffer) uint32
}

type checksummerAdapter struct {
	inner Checksummer
}

// Checksum adapts the public checksum interface to the internal core form.
func (a checksummerAdapter) Checksum(buffers []core.Buffer) uint32 {
	return a.inner.Checksum(fromCoreBuffers(buffers))
}

type coreChecksummerAdapter struct {
	inner core.Checksummer
}

// Checksum adapts the internal core checksum interface to the public form.
func (a coreChecksummerAdapter) Checksum(buffers []Buffer) uint32 {
	return a.inner.Checksum(toCoreBuffers(buffers))
}

func fromCoreBuffers(buffers []core.Buffer) []Buffer {
	if len(buffers) == 0 {
		return nil
	}

	publicBuffers := make([]Buffer, len(buffers))
	for i, buffer := range buffers {
		publicBuffers[i] = Buffer{Data: buffer.Data}
	}

	return publicBuffers
}

func toCoreBuffers(buffers []Buffer) []core.Buffer {
	if len(buffers) == 0 {
		return nil
	}

	coreBuffers := make([]core.Buffer, len(buffers))
	for i, buffer := range buffers {
		coreBuffers[i] = core.Buffer{Data: buffer.Data}
	}

	return coreBuffers
}
