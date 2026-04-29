// Package socket provides datagram socket adapters for the ENet runtime.
package socket

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/cafecito-games/goenet/internal/core"
)

// UDP adapts a net.UDPConn to the engine's datagram socket interface.
type UDP struct {
	conn *net.UDPConn
}

// NewUDP wraps conn with the DatagramSocket interface.
func NewUDP(conn *net.UDPConn) *UDP {
	return &UDP{conn: conn}
}

// ReadPacket reads one datagram into buf and returns its source address.
func (s *UDP) ReadPacket(ctx context.Context, buf []byte) (int, core.Address, error) {
	if err := ctx.Err(); err != nil {
		return 0, core.Address{}, err
	}

	for {
		if err := s.conn.SetReadDeadline(nextPollDeadline(ctx)); err != nil {
			return 0, core.Address{}, err
		}

		n, addr, err := s.conn.ReadFromUDPAddrPort(buf)
		if err == nil {
			coreAddr, convErr := AddressFromAddrPort(addr)
			if convErr != nil {
				return 0, core.Address{}, convErr
			}
			return n, coreAddr, nil
		}
		if isTimeoutError(err) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return 0, core.Address{}, ctxErr
			}
			continue
		}
		return 0, core.Address{}, err
	}
}

// WritePacket writes one datagram to addr.
func (s *UDP) WritePacket(ctx context.Context, addr core.Address, payload []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	for {
		if err := s.conn.SetWriteDeadline(nextPollDeadline(ctx)); err != nil {
			return 0, err
		}

		n, err := s.conn.WriteToUDP(payload, UDPAddrFromAddress(addr))
		if err == nil {
			return n, nil
		}
		if isTimeoutError(err) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return 0, ctxErr
			}
			continue
		}
		return 0, err
	}
}

// Close closes the underlying UDP connection.
func (s *UDP) Close() error {
	return s.conn.Close()
}

func nextPollDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(10 * time.Millisecond)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		return ctxDeadline
	}
	return deadline
}

func isTimeoutError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
