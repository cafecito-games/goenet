// Copyright (c) 2026 Cafecito Games LLC
// SPDX-License-Identifier: MIT

// Package lagrelay provides a transport-agnostic UDP impairment proxy that
// injects one-way latency, jitter, and packet loss between clients and an
// upstream UDP server.
//
// Because it forwards raw UDP datagrams over real sockets without inspecting
// ENet framing, both peers observe authentic network behavior including
// round-trip estimation, clock offset, adaptive throttling, and retransmission.
package lagrelay
