package goenet

// Compressor compresses and decompresses ENet payload bytes around the protocol header.
type Compressor interface {
	Compress(buffers []Buffer, inLimit int, out []byte) (int, error)
	Decompress(in []byte, out []byte) (int, error)
}
