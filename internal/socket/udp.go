// Package socket provides datagram socket adapters for the ENet runtime.
package socket

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/netip"
	"syscall"
	"time"

	"github.com/cafecito-games/goenet/internal/core"
)

// UDP adapts a net.UDPConn to the engine's datagram socket interface.
type UDP struct {
	conn                *net.UDPConn
	logger              *slog.Logger
	setReadDeadline     func(time.Time) error
	readFromUDPAddrPort func([]byte) (int, netip.AddrPort, error)
	setWriteDeadline    func(time.Time) error
	writeToUDP          func([]byte, *net.UDPAddr) (int, error)
}

// NewUDP wraps conn with the DatagramSocket interface.
func NewUDP(conn *net.UDPConn, logger *slog.Logger) *UDP {
	return &UDP{
		conn:                conn,
		logger:              core.ComponentLogger(logger, "socket"),
		setReadDeadline:     conn.SetReadDeadline,
		readFromUDPAddrPort: conn.ReadFromUDPAddrPort,
		setWriteDeadline:    conn.SetWriteDeadline,
		writeToUDP:          conn.WriteToUDP,
	}
}

// ReadPacket reads one datagram into buf and returns its source address.
func (s *UDP) ReadPacket(ctx context.Context, buf []byte) (int, core.Address, error) {
	if err := ctx.Err(); err != nil {
		return 0, core.Address{}, err
	}

	for {
		if err := s.setReadDeadline(nextPollDeadline(ctx)); err != nil {
			if shouldSuppressClosedConnError(err) {
				return 0, core.Address{}, err
			}
			s.logger.Error("socket read deadline failed", "err", err)
			return 0, core.Address{}, err
		}

		n, addr, err := s.readFromUDPAddrPort(buf)
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
		// On Linux and Windows a prior outbound datagram can surface as an
		// ICMP "destination unreachable" on the next read. C ENet silently
		// drops the error; matching that here keeps the read loop alive.
		if errors.Is(err, syscall.ECONNREFUSED) {
			s.logger.Debug("socket read ignored conn refused", "err", err)
			continue
		}
		if shouldSuppressClosedConnError(err) {
			return 0, core.Address{}, err
		}
		s.logger.Error("socket read failed", "err", err)
		return 0, core.Address{}, err
	}
}

// WritePacket writes one datagram to addr.
func (s *UDP) WritePacket(ctx context.Context, addr core.Address, payload []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	for {
		if err := s.setWriteDeadline(nextPollDeadline(ctx)); err != nil {
			if shouldSuppressClosedConnError(err) {
				return 0, err
			}
			s.logger.Error("socket write deadline failed", "err", err)
			return 0, err
		}

		n, err := s.writeToUDP(payload, UDPAddrFromAddress(addr))
		if err == nil {
			return n, nil
		}
		if isTimeoutError(err) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return 0, ctxErr
			}
			continue
		}
		if shouldSuppressClosedConnError(err) {
			return 0, err
		}
		s.logger.Error("socket write failed", "err", err)
		return 0, err
	}
}

// Close closes the underlying UDP connection.
func (s *UDP) Close() error {
	return s.conn.Close()
}

// nextPollDeadline picks a read/write deadline that lets the goroutine wake up
// to check ctx for cancellation. When ctx has its own deadline we use it
// directly so the kernel can block until either the socket is ready or the
// caller-bounded tick expires. When ctx is unbounded but cancellable we use a
// 100ms heartbeat — long enough to avoid a tight 100Hz spin against an idle
// socket, short enough to react to ctx.Done() promptly.
func nextPollDeadline(ctx context.Context) time.Time {
	if ctxDeadline, ok := ctx.Deadline(); ok {
		return ctxDeadline
	}
	return time.Now().Add(100 * time.Millisecond)
}

func isTimeoutError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func shouldSuppressClosedConnError(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
