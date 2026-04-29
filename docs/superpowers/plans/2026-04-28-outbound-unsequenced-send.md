# Outbound Unsequenced Send Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** support `PacketFlagUnsequenced` for outbound sends end-to-end through the public API and engine wire encoding, while tracking interop coverage as a separate follow-up issue.

**Architecture:** extend the existing outbound send path with a third payload mode that emits ENet `SendUnsequenced` commands. Track the outbound unsequenced group on the peer, route unsequenced packets through the non-ACK queue, verify behavior in engine and public API tests, and record interop expansion separately.

**Tech Stack:** Go, ENet-compatible protocol encoding, existing fake socket and C interop harness

---

### Task 1: Add failing tests for engine and public API behavior

**Files:**
- Modify: `internal/engine/send_test.go`
- Modify: `host_test.go`

- [ ] **Step 1: Add an engine test for outbound unsequenced queueing and wire encoding**
- [ ] **Step 2: Run the focused engine test and confirm it fails because unsequenced sends are rejected**
- [ ] **Step 3: Add a public API test for `Peer.Send(...PacketFlagUnsequenced...)` and flush behavior**
- [ ] **Step 4: Run the focused public test and confirm it fails for the same reason**

### Task 2: Implement outbound unsequenced send support

**Files:**
- Modify: `internal/peer/peer.go`
- Modify: `internal/engine/send.go`

- [ ] **Step 1: Add peer state for outbound unsequenced group numbering**
- [ ] **Step 2: Add a send payload type that marshals `protocol.SendUnsequenced`**
- [ ] **Step 3: Route `PacketFlagUnsequenced` through the outbound send path with `CommandFlagUnsequenced`**
- [ ] **Step 4: Assign unsequenced group numbers during outgoing command preparation and allow those commands through validation**
- [ ] **Step 5: Re-run the focused engine and public tests until they pass**

### Task 3: Update docs and track interop follow-up

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Remove the README limitation and document unsequenced send as supported behavior**
- [ ] **Step 2: Open a follow-up GitHub issue for outbound unsequenced interop coverage**

### Task 4: Full verification and commits

**Files:**
- Modify: `docs/superpowers/plans/2026-04-28-outbound-unsequenced-send.md`

- [ ] **Step 1: Run `go test ./internal/engine -run TestUnsequencedSendQueuesOutboundCommandAndFlushes -count=1`**
- [ ] **Step 2: Run `go test ./... -run TestPeerSendQueuesOutboundUnsequencedPayloadAndFlushes -count=1`**
- [ ] **Step 3: Run `go test ./...` for full verification**
- [ ] **Step 4: Commit the plan and implementation changes in the worktree**
