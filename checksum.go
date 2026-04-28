package goenet

import "github.com/cafecito-games/goenet/internal/core"

// Buffer is one byte slice passed to checksum and compression hooks.
type Buffer = core.Buffer

// Checksummer computes a checksum across the provided buffers.
type Checksummer = core.Checksummer
