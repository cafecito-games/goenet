# Public API Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** expose a real usable server/client API from `goenet` for host creation, connect, service, send, flush, disconnect, reset, and close while keeping ENet protocol behavior inside `internal/engine`.

**Architecture:** add a thin exported runtime layer in `goenet` that owns a real UDP socket, wraps `internal/engine.Host`, and maintains stable exported peer handles. First extract the engine-consumed shared types out of `goenet` into an internal core package so `goenet -> internal/engine` becomes legal; then build the public runtime on top of that corrected dependency direction.

**Tech Stack:** Go 1.26, stdlib `net`, `net/netip`, `context`, `time`, existing `internal/engine`, existing `internal/socket`, `go test`, Task.

---

## File Structure

| Path | Responsibility |
| --- | --- |
| `host.go` | Exported host API: constructors, service loop, flush, close, broadcast, config snapshot, peer mapping. |
| `peer.go` | Exported peer API: state snapshot, send, disconnect, disconnect-later, reset. |
| `event.go` | Public event type already exists; may need small adjustments only if translation requires them. |
| `config.go` | Public config already exists; only modify if constructor defaults or validation helpers are needed. |
| `address.go` | Public address type; may become an alias or thin wrapper over internal core. |
| `packet.go` | Public packet types/flags; may become aliases over internal core. |
| `checksum.go` | Public checksum interfaces; may become aliases over internal core. |
| `compressor.go` | Public compressor interfaces; may become aliases over internal core. |
| `intercept.go` | Public intercept interfaces; may become aliases over internal core. |
| `host_test.go` | Public host lifecycle, listen/new-host, close, service/event translation tests. |
| `peer_test.go` | Public peer send/disconnect/reset tests. |
| `internal/core/types.go` | Shared transport-facing value types used by both engine and public API. |
| `internal/core/address.go` | Shared address type and helpers moved out of `goenet` to break the cycle. |
| `internal/socket/udp.go` | Concrete `net.UDPConn` implementation of `DatagramSocket`. |
| `internal/socket/udp_test.go` | Narrow socket adapter tests if needed. |
| `internal/engine/host.go` | Minimal outbound connect/disconnect/reset helpers and any state accessors the public layer needs. |
| `internal/engine/receive.go` | Only touch if public connect/disconnect helpers need existing runtime state aligned. |
| `internal/engine/send.go` | Only touch if new control-command helpers need send-path support. |
| `README.md` | Update usage/docs once public API exists for real. |
| `examples/basic_server_test.go` | Replace config-only example with real public server flow. |
| `examples/basic_client_test.go` | Replace packet/event-only example with real client flow. |

## Task 1: Concrete UDP Socket And Public Host Skeleton

**Files:**
- Create: `internal/socket/udp.go`
- Create: `host_test.go`
- Modify: `host.go`
- Test: `host_test.go`

- [ ] **Step 1: Write the failing public host construction tests**

```go
func TestListenReturnsUsableHost(t *testing.T) {
	host, err := goenet.Listen("127.0.0.1:0", goenet.Config{PeerCount: 4, ChannelLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	if host.Config().PeerCount != 4 {
		t.Fatalf("peer count = %d, want 4", host.Config().PeerCount)
	}
}

func TestNewHostReturnsClientCapableHost(t *testing.T) {
	host, err := goenet.NewHost(goenet.Config{PeerCount: 1, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	if host.Config().ChannelLimit != 1 {
		t.Fatalf("channel limit = %d, want 1", host.Config().ChannelLimit)
	}
}

func TestCloseMakesFurtherOperationsFail(t *testing.T) {
	host, err := goenet.Listen("127.0.0.1:0", goenet.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	if err := host.Flush(context.Background()); err == nil {
		t.Fatal("expected flush after close to fail")
	}
}
```

- [ ] **Step 2: Run the targeted tests to verify they fail**

Run: `go test ./... -run 'Test(ListenReturnsUsableHost|NewHostReturnsClientCapableHost|CloseMakesFurtherOperationsFail)' -count=1`
Expected: FAIL because the public constructors and lifecycle methods do not exist yet.

- [ ] **Step 3: Implement the concrete UDP socket adapter**

```go
package socket

import (
	"context"
	"net"
	"net/netip"
	"time"
)

type UDP struct {
	conn *net.UDPConn
}

func NewUDP(conn *net.UDPConn) *UDP {
	return &UDP{conn: conn}
}

func (s *UDP) ReadPacket(ctx context.Context, buf []byte) (int, netip.AddrPort, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = s.conn.SetReadDeadline(deadline)
	} else {
		_ = s.conn.SetReadDeadline(time.Time{})
	}
	n, addr, err := s.conn.ReadFromUDPAddrPort(buf)
	return n, addr, err
}

func (s *UDP) WritePacket(ctx context.Context, addr netip.AddrPort, payload []byte) (int, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = s.conn.SetWriteDeadline(deadline)
	} else {
		_ = s.conn.SetWriteDeadline(time.Time{})
	}
	return s.conn.WriteToUDPAddrPort(payload, addr)
}

func (s *UDP) Close() error {
	return s.conn.Close()
}
```

- [ ] **Step 4: Implement the exported host skeleton and constructors**

```go
type Host struct {
	config Config
	engine *engine.Host
	socket *net.UDPConn
	closed atomic.Bool
	peers  map[*ipeer.Peer]*Peer
}

func Listen(addr string, cfg Config) (*Host, error) {
	conn, err := net.ListenUDP("udp", mustResolveUDPAddr(addr))
	if err != nil {
		return nil, err
	}
	return newPublicHost(cfg, conn), nil
}

func NewHost(cfg Config) (*Host, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return nil, err
	}
	return newPublicHost(cfg, conn), nil
}

func (h *Host) Flush(ctx context.Context) error {
	if h.closed.Load() {
		return errHostClosed
	}
	return h.engine.Flush(ctx)
}

func (h *Host) Close() error {
	if !h.closed.CompareAndSwap(false, true) {
		return nil
	}
	return h.socket.Close()
}
```

- [ ] **Step 5: Run the targeted tests to verify they pass**

Run: `go test ./... -run 'Test(ListenReturnsUsableHost|NewHostReturnsClientCapableHost|CloseMakesFurtherOperationsFail)' -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add host.go host_test.go internal/socket/udp.go
git commit -m "feat: add public host constructors and udp socket"
```

## Task 2: Extract Shared Core Types And Break The Import Cycle

**Files:**
- Create: `internal/core/types.go`
- Create: `internal/core/address.go`
- Modify: `address.go`
- Modify: `config.go`
- Modify: `event.go`
- Modify: `packet.go`
- Modify: `checksum.go`
- Modify: `compressor.go`
- Modify: `intercept.go`
- Modify: `peer.go`
- Modify: `internal/engine/host.go`
- Modify: `internal/engine/receive.go`
- Modify: `internal/engine/send.go`
- Modify: `internal/peer/peer.go`
- Modify: `internal/peer/queue.go`
- Test: `address_test.go`

- [ ] **Step 1: Write the failing cycle-break smoke test**

```go
func TestPublicPackageStillExposesConfigAndPacketTypes(t *testing.T) {
	var cfg goenet.Config
	packet := goenet.Packet{Data: []byte("x")}

	if cfg.MTU == 0 {
		cfg = goenet.DefaultConfig()
	}
	if len(packet.Data) != 1 {
		t.Fatalf("packet length = %d, want 1", len(packet.Data))
	}
}
```

- [ ] **Step 2: Run the targeted tests to verify baseline behavior before the refactor**

Run: `go test ./... -run 'TestPublicPackageStillExposesConfigAndPacketTypes' -count=1`
Expected: PASS before refactor, then keep passing after the extraction.

- [ ] **Step 3: Create `internal/core` and move shared engine-consumed types there**

```go
package core

type PacketFlag uint32
type Packet struct {
	Data  []byte
	Flags PacketFlag
}

type EventType uint8
type PeerState uint8

type Config struct {
	PeerCount          int
	ChannelLimit       uint8
	MTU                uint32
	MaximumPacketSize  uint32
	MaximumWaitingData uint32
	Checksum           Checksummer
	Compressor         Compressor
	Intercept          Interceptor
}
```

- [ ] **Step 4: Make the public `goenet` types aliases or thin wrappers over `internal/core`**

```go
package goenet

import "github.com/cafecito-games/goenet/internal/core"

type Config = core.Config
type Packet = core.Packet
type PacketFlag = core.PacketFlag
type EventType = core.EventType
type Event = core.Event
type PeerState = core.PeerState
type Address = core.Address
type Checksummer = core.Checksummer
type Compressor = core.Compressor
type Interceptor = core.Interceptor
type InterceptDecision = core.InterceptDecision
type InterceptResult = core.InterceptResult
```

- [ ] **Step 5: Repoint internal packages from `goenet` to `internal/core`**

```go
import "github.com/cafecito-games/goenet/internal/core"

type Host struct {
	config core.Config
}
```

- [ ] **Step 6: Run the full suite to verify the cycle is gone and behavior remains green**

Run: `go test ./... -count=1`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add address.go config.go event.go packet.go checksum.go compressor.go intercept.go peer.go internal/core internal/engine internal/peer address_test.go
git commit -m "refactor: extract shared core transport types"
```

## Task 3: Stable Public Peer Mapping And Event Translation

**Files:**
- Modify: `host.go`
- Modify: `peer.go`
- Modify: `host_test.go`
- Test: `host_test.go`

- [ ] **Step 1: Write the failing event translation and stable peer tests**

```go
func TestServiceReturnsStablePeerHandlesAcrossEvents(t *testing.T) {
	server, client := newPairedHosts(t)
	defer server.Close()
	defer client.Close()

	peer, connect := mustConnectPair(t, client, server)
	if connect.Peer != peer {
		t.Fatalf("connect peer handle mismatch")
	}

	if err := peer.Send(0, &goenet.Packet{Data: []byte("ping"), Flags: goenet.PacketFlagReliable}); err != nil {
		t.Fatal(err)
	}

	event := mustServiceUntil(t, server, 2*time.Second, func(e goenet.Event) bool {
		return e.Type == goenet.EventReceive
	})
	if event.Peer == nil {
		t.Fatal("expected peer handle")
	}
}
```

- [ ] **Step 2: Run the targeted tests to verify they fail**

Run: `go test ./... -run 'TestServiceReturnsStablePeerHandlesAcrossEvents' -count=1`
Expected: FAIL because public service/event translation and peer mapping do not exist yet.

- [ ] **Step 3: Expand exported peer and host runtime mapping**

```go
type Peer struct {
	host  *Host
	raw   *ipeer.Peer
	state PeerState
}

func (h *Host) wrapPeer(raw *ipeer.Peer) *Peer {
	if existing, ok := h.peers[raw]; ok {
		existing.raw = raw
		existing.state = PeerState(raw.State)
		return existing
	}
	wrapped := &Peer{host: h, raw: raw, state: PeerState(raw.State)}
	h.peers[raw] = wrapped
	return wrapped
}

func (h *Host) translateEvent(ev engine.Event) Event {
	var wrapped *Peer
	if ev.Peer != nil {
		wrapped = h.wrapPeer(ev.Peer)
		wrapped.state = PeerState(ev.Peer.State)
	}
	return Event{
		Type:      ev.Type,
		Peer:      wrapped,
		ChannelID: ev.ChannelID,
		Data:      ev.Data,
		Packet:    ev.Packet,
	}
}
```

- [ ] **Step 4: Implement public `Service` and `Peer.State` behavior**

```go
func (h *Host) Service(ctx context.Context, timeout time.Duration) (Event, error) {
	if h.closed.Load() {
		return Event{}, errHostClosed
	}
	ev, err := h.engine.Service(ctx, durationMillis(timeout))
	if err != nil {
		return Event{}, err
	}
	return h.translateEvent(ev), nil
}

func (p *Peer) State() PeerState {
	if p.raw != nil {
		p.state = PeerState(p.raw.State)
	}
	return p.state
}
```

- [ ] **Step 5: Run the targeted tests to verify they pass**

Run: `go test ./... -run 'TestServiceReturnsStablePeerHandlesAcrossEvents' -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add host.go peer.go host_test.go
git commit -m "feat: translate engine events into public peers"
```

## Task 4: Public Connect Flow And Minimal Engine Connect Support

**Files:**
- Modify: `host.go`
- Modify: `internal/engine/host.go`
- Modify: `internal/engine/send.go`
- Modify: `internal/engine/receive.go`
- Modify: `host_test.go`
- Test: `host_test.go`

- [ ] **Step 1: Write the failing public connect tests**

```go
func TestConnectEstablishesClientServerPair(t *testing.T) {
	server, err := goenet.Listen("127.0.0.1:0", goenet.Config{PeerCount: 8, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client, err := goenet.NewHost(goenet.Config{PeerCount: 1, ChannelLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	peer, err := client.Connect(server.LocalAddr().String(), 1, 42)
	if err != nil {
		t.Fatal(err)
	}

	clientEvent := mustServiceUntil(t, client, 2*time.Second, func(e goenet.Event) bool { return e.Type == goenet.EventConnect })
	serverEvent := mustServiceUntil(t, server, 2*time.Second, func(e goenet.Event) bool { return e.Type == goenet.EventConnect })

	if clientEvent.Peer != peer {
		t.Fatalf("client connect peer mismatch")
	}
	if serverEvent.Data != 42 {
		t.Fatalf("server connect data = %d, want 42", serverEvent.Data)
	}
}
```

- [ ] **Step 2: Run the targeted tests to verify they fail**

Run: `go test ./... -run 'TestConnectEstablishesClientServerPair' -count=1`
Expected: FAIL because public connect and outbound connect support do not exist yet.

- [ ] **Step 3: Add minimal engine outbound connect helper**

```go
func (h *Host) Connect(addr goenet.Address, channelCount uint8, data uint32) (*peer.Peer, error) {
	p := h.AddPeer(addr, goenet.PeerStateConnecting)
	p.ConnectID = nextConnectID()
	p.Channels = make([]peer.Channel, channelCount)
	for i := range p.Channels {
		p.Channels[i] = peer.NewChannel()
	}

	h.runtime[p].eventData = data

	cmd := protocol.Connect{ ... }
	err := h.queueOutgoingControlCommand(p, peer.Command{
		Header: peer.Header{
			Command:   protocol.CommandConnect,
			ChannelID: 0xFF,
			Flags:     protocol.CommandFlagAcknowledge,
		},
		Payload: &cmd,
	})
	return p, err
}
```

- [ ] **Step 4: Add public `Host.Connect` on top of the engine helper**

```go
func (h *Host) Connect(addr string, channelCount uint8, data uint32) (*Peer, error) {
	if h.closed.Load() {
		return nil, errHostClosed
	}
	address, err := resolveAddress(addr)
	if err != nil {
		return nil, err
	}
	raw, err := h.engine.Connect(address, channelCount, data)
	if err != nil {
		return nil, err
	}
	return h.wrapPeer(raw), nil
}
```

- [ ] **Step 5: Run the targeted tests to verify they pass**

Run: `go test ./... -run 'TestConnectEstablishesClientServerPair' -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add host.go host_test.go internal/engine/host.go internal/engine/send.go internal/engine/receive.go
git commit -m "feat: add public connect flow"
```

## Task 5: Public Send, Broadcast, Disconnect, DisconnectLater, And Reset

**Files:**
- Modify: `peer.go`
- Modify: `host.go`
- Modify: `internal/engine/host.go`
- Modify: `peer_test.go`
- Create: `peer_test.go`
- Test: `peer_test.go`

- [ ] **Step 1: Write the failing peer operation tests**

```go
func TestPeerSendDeliversPacketAcrossPublicHosts(t *testing.T) {
	server, client := newConnectedPublicHosts(t)
	defer server.Close()
	defer client.Close()

	peer := clientPeer(t, client)
	if err := peer.Send(0, &goenet.Packet{Data: []byte("hello"), Flags: goenet.PacketFlagReliable}); err != nil {
		t.Fatal(err)
	}

	event := mustServiceUntil(t, server, 2*time.Second, func(e goenet.Event) bool { return e.Type == goenet.EventReceive })
	if got := string(event.Packet.Data); got != "hello" {
		t.Fatalf("packet = %q, want hello", got)
	}
}

func TestPeerDisconnectProducesDisconnectEvent(t *testing.T) {
	server, client := newConnectedPublicHosts(t)
	defer server.Close()
	defer client.Close()

	peer := clientPeer(t, client)
	if err := peer.Disconnect(9); err != nil {
		t.Fatal(err)
	}

	event := mustServiceUntil(t, server, 2*time.Second, func(e goenet.Event) bool { return e.Type == goenet.EventDisconnect })
	if event.Data != 9 {
		t.Fatalf("disconnect data = %d, want 9", event.Data)
	}
}
```

- [ ] **Step 2: Run the targeted tests to verify they fail**

Run: `go test ./... -run 'Test(PeerSendDeliversPacketAcrossPublicHosts|PeerDisconnectProducesDisconnectEvent)' -count=1`
Expected: FAIL because public peer operations are missing.

- [ ] **Step 3: Implement public peer operations and broadcast**

```go
func (p *Peer) Send(channelID uint8, packet *Packet) error {
	if p.host == nil || p.host.closed.Load() {
		return errHostClosed
	}
	return p.host.engine.Send(p.raw, channelID, packet)
}

func (h *Host) Broadcast(channelID uint8, packet *Packet) error {
	for _, wrapped := range h.peers {
		if wrapped.raw != nil && wrapped.raw.State == PeerStateConnected {
			if err := h.engine.Send(wrapped.raw, channelID, packet); err != nil {
				return err
			}
		}
	}
	return nil
}
```

- [ ] **Step 4: Add minimal engine helpers for disconnect-later and reset**

```go
func (h *Host) Disconnect(p *peer.Peer, data uint32) error { ... }
func (h *Host) DisconnectLater(p *peer.Peer, data uint32) error { ... }
func (h *Host) Reset(p *peer.Peer) { ... }
```

- [ ] **Step 5: Wire exported disconnect/reset methods and rerun tests**

Run: `go test ./... -run 'Test(PeerSendDeliversPacketAcrossPublicHosts|PeerDisconnectProducesDisconnectEvent)' -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add host.go peer.go peer_test.go internal/engine/host.go
git commit -m "feat: add public peer operations"
```

## Task 6: Examples, README, And Final Public API Verification

**Files:**
- Modify: `README.md`
- Modify: `examples/basic_server_test.go`
- Modify: `examples/basic_client_test.go`
- Modify: `host_test.go`
- Modify: `peer_test.go`

- [ ] **Step 1: Replace placeholder/current-surface docs with real public API usage**

```go
func Example_basicServer() {
	host, _ := goenet.Listen("127.0.0.1:7777", goenet.Config{PeerCount: 8, ChannelLimit: 1})
	defer host.Close()
	fmt.Println(host.Config().ChannelLimit)
	// Output:
	// 1
}
```

- [ ] **Step 2: Add a full public smoke test**

```go
func TestPublicAPIEndToEnd(t *testing.T) {
	server, client := newConnectedPublicHosts(t)
	defer server.Close()
	defer client.Close()

	peer := clientPeer(t, client)
	if err := peer.Send(0, &goenet.Packet{Data: []byte("e2e"), Flags: goenet.PacketFlagReliable}); err != nil {
		t.Fatal(err)
	}

	event := mustServiceUntil(t, server, 2*time.Second, func(e goenet.Event) bool { return e.Type == goenet.EventReceive })
	if string(event.Packet.Data) != "e2e" {
		t.Fatalf("unexpected packet %q", event.Packet.Data)
	}
}
```

- [ ] **Step 3: Run the focused public API tests**

Run: `go test ./... -run 'Test(PublicAPIEndToEnd|ConnectEstablishesClientServerPair|PeerSendDeliversPacketAcrossPublicHosts|PeerDisconnectProducesDisconnectEvent)' -count=1`
Expected: PASS

- [ ] **Step 4: Run final repo verification**

Run: `go test ./... -count=1`
Expected: PASS

Run: `go test ./interop -count=1`
Expected: PASS

Run: `go test ./examples -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add README.md examples/basic_server_test.go examples/basic_client_test.go host_test.go peer_test.go
git commit -m "docs: expose real public goenet api usage"
```

## Self-Review

Spec coverage:

- usable host construction and socket lifecycle: Task 1
- cycle-breaking internal core extraction required by runtime layering: Task 2
- public peer mapping and event translation: Task 3
- public client/server connect flow: Task 4
- send/flush/disconnect/reset operations: Task 5
- honest docs/examples/final public verification: Task 6

Placeholder scan:

- no `TODO`/`TBD` placeholders remain
- each task includes explicit files, tests, commands, and commit points

Type consistency:

- public API names used throughout the plan match the approved spec:
  `Listen`, `NewHost`, `Connect`, `Service`, `Flush`, `Broadcast`, `Close`, `Send`, `Disconnect`, `DisconnectLater`, `Reset`, `State`
