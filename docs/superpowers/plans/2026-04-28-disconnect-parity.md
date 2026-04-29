# Disconnect Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Match the local ENet fork's disconnect behavior for handshake states and add public `Peer.DisconnectNow(data uint32) error`.

**Architecture:** Keep the behavior change at the engine boundary where ENet state semantics belong. Extend `internal/engine.Host.Disconnect` with the missing non-connected branch, add `internal/engine.Host.DisconnectNow`, and expose a thin public wrapper on `goenet.Peer`. Verify behavior from the public API boundary with wire-shape and state-reset tests.

**Tech Stack:** Go 1.26, `go test`, Taskfile, existing fake socket test harness, local ENet fork semantics from `/Users/christian/CafecitoGames/enet/include/enet.h`

---

### Task 1: Add failing public tests for handshake disconnect and DisconnectNow

**Files:**
- Modify: `host_test.go`

- [ ] **Step 1: Write the failing tests**

Add tests that exercise the missing behavior from the public API boundary:

```go
func TestDisconnectOnConnectingPeerFlushesUnsequencedDisconnectAndResets(t *testing.T) {
	host, sock := newTestHost()

	peer, err := host.Connect("127.0.0.1:9001", 1, 0xCAFE)
	if err != nil {
		t.Fatal(err)
	}

	if err := peer.Disconnect(0xDEAD); err != nil {
		t.Fatal(err)
	}

	if got := peer.State(); got != PeerStateDisconnected {
		t.Fatalf("peer state = %d, want %d", got, PeerStateDisconnected)
	}

	write := sock.MustWrite(t, 0)
	header, command := mustSingleCommand(t, write.Payload)
	if header.PeerID != protocol.MaximumPeerID {
		t.Fatalf("header peer id = %d, want %d", header.PeerID, protocol.MaximumPeerID)
	}

	disconnect, ok := command.(protocol.Disconnect)
	if !ok {
		t.Fatalf("disconnect command type = %T", command)
	}
	if disconnect.Data != 0xDEAD {
		t.Fatalf("disconnect data = %#x, want %#x", disconnect.Data, uint32(0xDEAD))
	}
}

func TestDisconnectNowFlushesUnsequencedDisconnectAndResetsConnectedPeer(t *testing.T) {
	host, sock := newConnectedTestHost(t)

	peer := connectedPublicPeer(t, host)
	if err := peer.DisconnectNow(0xBEEF); err != nil {
		t.Fatal(err)
	}

	if got := peer.State(); got != PeerStateDisconnected {
		t.Fatalf("peer state = %d, want %d", got, PeerStateDisconnected)
	}

	write := sock.MustWrite(t, 0)
	_, command := mustSingleCommand(t, write.Payload)
	disconnect, ok := command.(protocol.Disconnect)
	if !ok {
		t.Fatalf("disconnect command type = %T", command)
	}
	if disconnect.Data != 0xBEEF {
		t.Fatalf("disconnect data = %#x, want %#x", disconnect.Data, uint32(0xBEEF))
	}
}

func TestDisconnectNowOnDisconnectedPeerIsSafe(t *testing.T) {
	host, _ := newTestHost()
	peer := &Peer{host: host}

	if err := peer.DisconnectNow(1); err != nil {
		t.Fatal(err)
	}
}
```

- [ ] **Step 2: Run the targeted tests to verify they fail**

Run: `go test ./... -run 'TestDisconnectOnConnectingPeerFlushesUnsequencedDisconnectAndResets|TestDisconnectNowFlushesUnsequencedDisconnectAndResetsConnectedPeer|TestDisconnectNowOnDisconnectedPeerIsSafe' -count=1`

Expected: FAIL because `Peer.Disconnect` still rejects handshake-state peers and `Peer.DisconnectNow` does not exist.

- [ ] **Step 3: Commit the failing tests**

```bash
git add host_test.go
git commit -m "test: cover disconnect parity gaps"
```

### Task 2: Implement engine disconnect parity and public DisconnectNow

**Files:**
- Modify: `peer.go`
- Modify: `internal/engine/host.go`

- [ ] **Step 1: Add the public API wrapper**

In `peer.go`, add:

```go
func (p *Peer) DisconnectNow(data uint32) error {
	if p == nil || p.host == nil || p.raw == nil {
		return fmt.Errorf("goenet: nil peer")
	}
	if p.host.closed.Load() {
		return errHostClosed
	}
	if err := p.host.engine.DisconnectNow(p.raw, data); err != nil {
		return err
	}

	p.state = fromCorePeerState(p.raw.State)
	return nil
}
```

- [ ] **Step 2: Extend `internal/engine.Host.Disconnect` with the handshake-state branch**

In `internal/engine/host.go`, preserve the existing terminal-state early return and connected graceful path, but replace the current non-connected error with ENet behavior:

```go
func (h *Host) Disconnect(p *peer.Peer, data uint32) error {
	if p == nil {
		return fmt.Errorf("engine: nil peer")
	}
	if p.State == core.PeerStateDisconnecting ||
		p.State == core.PeerStateDisconnected ||
		p.State == core.PeerStateAcknowledgingDisconnect ||
		p.State == core.PeerStateZombie {
		return nil
	}

	h.clearPeerQueues(p)

	if p.State == core.PeerStateConnected || p.State == core.PeerStateDisconnectLater {
		if err := h.queueOutgoingControlCommand(p, peer.Command{
			Header: peer.Header{
				Command:   protocol.CommandDisconnect,
				ChannelID: 0xFF,
				Flags:     protocol.CommandFlagAcknowledge,
			},
			Payload: &protocol.Disconnect{Data: data},
		}); err != nil {
			return err
		}

		h.runtime[p].eventData = data
		h.runtime[p].disconnectLater = false
		p.State = core.PeerStateDisconnecting
		return nil
	}

	if err := h.queueOutgoingControlCommand(p, peer.Command{
		Header: peer.Header{
			Command:   protocol.CommandDisconnect,
			ChannelID: 0xFF,
			Flags:     protocol.CommandFlagUnsequenced,
		},
		Payload: &protocol.Disconnect{Data: data},
	}); err != nil {
		return err
	}
	if err := h.Flush(context.Background()); err != nil {
		return err
	}
	h.resetPeer(p)
	return nil
}
```

- [ ] **Step 3: Add ENet-style `DisconnectNow` at the engine boundary**

In `internal/engine/host.go`, add:

```go
func (h *Host) DisconnectNow(p *peer.Peer, data uint32) error {
	if p == nil {
		return fmt.Errorf("engine: nil peer")
	}
	if p.State == core.PeerStateDisconnected {
		return nil
	}
	if p.State != core.PeerStateZombie && p.State != core.PeerStateDisconnecting {
		h.clearPeerQueues(p)
		if err := h.queueOutgoingControlCommand(p, peer.Command{
			Header: peer.Header{
				Command:   protocol.CommandDisconnect,
				ChannelID: 0xFF,
				Flags:     protocol.CommandFlagUnsequenced,
			},
			Payload: &protocol.Disconnect{Data: data},
		}); err != nil {
			return err
		}
		if err := h.Flush(context.Background()); err != nil {
			return err
		}
	}

	h.resetPeer(p)
	return nil
}
```

- [ ] **Step 4: Run the targeted tests to verify they pass**

Run: `go test ./... -run 'TestDisconnectOnConnectingPeerFlushesUnsequencedDisconnectAndResets|TestDisconnectNowFlushesUnsequencedDisconnectAndResetsConnectedPeer|TestDisconnectNowOnDisconnectedPeerIsSafe' -count=1`

Expected: PASS

- [ ] **Step 5: Commit the implementation**

```bash
git add peer.go internal/engine/host.go host_test.go
git commit -m "feat: add disconnect parity and disconnect now"
```

### Task 3: Protect existing disconnect behavior and document the new API

**Files:**
- Modify: `host_test.go`
- Modify: `README.md`

- [ ] **Step 1: Add/keep the connected graceful disconnect assertion**

Ensure `host_test.go` has a connected-flow test that still expects the acknowledged disconnect path:

```go
func TestPeerDisconnectQueuesAcknowledgedDisconnectForConnectedPeer(t *testing.T) {
	host, sock := newConnectedTestHost(t)
	peer := connectedPublicPeer(t, host)

	if err := peer.Disconnect(0xABCD); err != nil {
		t.Fatal(err)
	}
	if err := host.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, command := mustSingleCommand(t, sock.MustWrite(t, 0).Payload)
	disconnect, ok := command.(protocol.Disconnect)
	if !ok {
		t.Fatalf("disconnect command type = %T", command)
	}
	if disconnect.Header.Flags&protocol.CommandFlagAcknowledge == 0 {
		t.Fatal("expected acknowledged disconnect for connected peer")
	}
}
```

- [ ] **Step 2: Document `DisconnectNow` in the README**

Add `Peer.DisconnectNow(data uint32) error` to the public API list and the event/packet semantics section with one honest sentence:

```md
- `Peer.DisconnectNow(data uint32) error`
```

```md
- `DisconnectNow` force-flushes an unsequenced disconnect and resets the local peer immediately.
```

- [ ] **Step 3: Run full verification**

Run:

```bash
go test ./... -count=1
task test:cover
```

Expected:
- `go test ./... -count=1` PASS
- `task test:cover` PASS

- [ ] **Step 4: Commit the verification/doc pass**

```bash
git add host_test.go README.md
git commit -m "docs: document disconnect now"
```

## Self-Review

- Spec coverage: this plan covers the handshake-state `Disconnect` branch, public `DisconnectNow`, focused public tests, and README update.
- Placeholder scan: no `TODO`/`TBD` placeholders remain.
- Type consistency: public method is consistently `DisconnectNow(data uint32) error`; engine helper is `DisconnectNow(p *peer.Peer, data uint32) error`.
