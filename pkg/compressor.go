package goenet

import "github.com/cafecito-games/goenet/internal/core"

// Compressor compresses and decompresses ENet payload bytes around the protocol header.
// buffers passed to Compress are concatenated input.
type Compressor = core.Compressor
