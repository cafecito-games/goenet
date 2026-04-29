package goenet

// Compressor compresses and decompresses ENet payload bytes around the protocol header.
// buffers passed to Compress are concatenated input.
type Compressor interface {
	Compress(buffers [][]byte, inLimit int, out []byte) (int, error)
	Decompress(in []byte, out []byte) (int, error)
}

type compressorAdapter struct {
	inner Compressor
}

// Compress adapts the public compressor interface to the internal core form.
func (a compressorAdapter) Compress(buffers [][]byte, inLimit int, out []byte) (int, error) {
	return a.inner.Compress(buffers, inLimit, out)
}

// Decompress adapts the public compressor interface to the internal core form.
func (a compressorAdapter) Decompress(in, out []byte) (int, error) {
	return a.inner.Decompress(in, out)
}
