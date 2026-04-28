package goenet

import "github.com/cafecito-games/goenet/internal/core"

// Compressor compresses and decompresses ENet payload bytes around the protocol header.
type Compressor interface {
	Compress(buffers []Buffer, inLimit int, out []byte) (int, error)
	Decompress(in []byte, out []byte) (int, error)
}

type compressorAdapter struct {
	inner Compressor
}

func (a compressorAdapter) Compress(buffers []core.Buffer, inLimit int, out []byte) (int, error) {
	return a.inner.Compress(fromCoreBuffers(buffers), inLimit, out)
}

func (a compressorAdapter) Decompress(in []byte, out []byte) (int, error) {
	return a.inner.Decompress(in, out)
}

type coreCompressorAdapter struct {
	inner core.Compressor
}

func (a coreCompressorAdapter) Compress(buffers []Buffer, inLimit int, out []byte) (int, error) {
	return a.inner.Compress(toCoreBuffers(buffers), inLimit, out)
}

func (a coreCompressorAdapter) Decompress(in []byte, out []byte) (int, error) {
	return a.inner.Decompress(in, out)
}
