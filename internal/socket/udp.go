// Package socket provides datagram socket adapters for the ENet runtime.
package socket

import (
	"context"
	"net"
	"net/netip"
	"time"
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
func (s *UDP) ReadPacket(ctx context.Context, buf []byte) (int, netip.AddrPort, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := s.conn.SetReadDeadline(deadline); err != nil {
			return 0, netip.AddrPort{}, err
		}
	} else {
		if err := s.conn.SetReadDeadline(time.Time{}); err != nil {
			return 0, netip.AddrPort{}, err
		}
	}

	n, addr, err := s.conn.ReadFromUDPAddrPort(buf)
	return n, addr, err
}

// WritePacket writes one datagram to addr.
func (s *UDP) WritePacket(ctx context.Context, addr netip.AddrPort, payload []byte) (int, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := s.conn.SetWriteDeadline(deadline); err != nil {
			return 0, err
		}
	} else {
		if err := s.conn.SetWriteDeadline(time.Time{}); err != nil {
			return 0, err
		}
	}

	return s.conn.WriteToUDPAddrPort(payload, addr)
}

// Close closes the underlying UDP connection.
func (s *UDP) Close() error {
	return s.conn.Close()
}
