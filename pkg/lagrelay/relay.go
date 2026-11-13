// Copyright (c) 2026 Cafecito Games LLC
// SPDX-License-Identifier: MIT

package lagrelay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultSessionIdleTimeout is the inactivity duration after which a client
	// session and its dedicated upstream UDP socket are closed.
	DefaultSessionIdleTimeout = 2 * time.Minute

	// maxDatagramBytes covers the maximum UDP datagram payload size.
	maxDatagramBytes = 65_535

	// queueCapacity bounds one direction of one session; when full, newly
	// arriving datagrams are tail-dropped like a saturated router queue.
	queueCapacity = 8_192
)

// Config configures a UDP impairment Relay.
type Config struct {
	// Listen is the local UDP address clients connect to. If empty, defaults
	// to "127.0.0.1:0".
	Listen string
	// Upstream is the required UDP address of the target server.
	Upstream string
	// InitialProfile is the starting impairment profile.
	InitialProfile Profile
	// Seed initializes the deterministic PRNG streams for jitter and loss.
	Seed uint64
	// IdleTimeout sets how long an inactive client session stays open. If zero
	// or negative, DefaultSessionIdleTimeout is used.
	IdleTimeout time.Duration
	// Logger receives structured operational events. If nil, logging is suppressed.
	Logger *slog.Logger
}

// Stats reports cumulative datagram counters since the relay was created,
// along with the current number of active client sessions.
type Stats struct {
	UpForwarded   uint64 `json:"up_forwarded"`
	UpDropped     uint64 `json:"up_dropped"`
	DownForwarded uint64 `json:"down_forwarded"`
	DownDropped   uint64 `json:"down_dropped"`
	Sessions      int    `json:"sessions"`
}

type directionCounters struct {
	forwarded atomic.Uint64
	dropped   atomic.Uint64
}

// Relay forwards UDP datagrams between clients and one upstream server while
// applying a live-configurable impairment Profile. Every distinct client
// address receives its own upstream UDP socket so the server observes one peer
// per client just as it would without a proxy.
type Relay struct {
	front       *net.UDPConn
	upstream    *net.UDPAddr
	profile     atomic.Pointer[Profile]
	seed        uint64
	idleTimeout time.Duration
	logger      *slog.Logger

	up   directionCounters
	down directionCounters

	mutex    sync.Mutex
	sessions map[netip.AddrPort]*session
	nextID   uint64
}

type session struct {
	client     netip.AddrPort
	server     *net.UDPConn
	toServer   *delayLine
	toClient   *delayLine
	lastActive atomic.Int64
	cancel     context.CancelFunc
}

type queuedDatagram struct {
	due     time.Time
	payload []byte
}

// delayLine releases datagrams in arrival order, each no earlier than its due time.
type delayLine struct {
	queue    chan queuedDatagram
	counters *directionCounters
	deliver  func([]byte) error

	mutex   sync.Mutex
	random  *rand.Rand
	lastDue time.Time
}

func newDelayLine(seed, stream uint64, counters *directionCounters, deliver func([]byte) error) *delayLine {
	return &delayLine{
		queue:    make(chan queuedDatagram, queueCapacity),
		counters: counters,
		deliver:  deliver,
		//nolint:gosec // Deterministic PRNG is intentional for reproducible network simulation.
		random: rand.New(rand.NewPCG(seed, stream)),
	}
}

func (l *delayLine) enqueue(now time.Time, payload []byte, delayMS, jitterMS, lossPercent float64) {
	l.mutex.Lock()
	//nolint:gosec // Deterministic PRNG is intentional for reproducible network simulation.
	lost := lossPercent > 0 && l.random.Float64()*100 < lossPercent
	delay := delayMS
	if jitterMS > 0 {
		//nolint:gosec // Deterministic PRNG is intentional for reproducible network simulation.
		delay += l.random.Float64() * jitterMS
	}
	due := now.Add(time.Duration(delay * float64(time.Millisecond)))
	if !lost {
		if due.Before(l.lastDue) {
			due = l.lastDue
		}
		l.lastDue = due
	}
	l.mutex.Unlock()

	if lost {
		l.counters.dropped.Add(1)
		return
	}
	select {
	case l.queue <- queuedDatagram{due: due, payload: payload}:
	default:
		l.counters.dropped.Add(1)
	}
}

func (l *delayLine) run(ctx context.Context) {
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case datagram := <-l.queue:
			if wait := time.Until(datagram.due); wait > 0 {
				timer.Reset(wait)
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
			}
			// Count before writing so a receiver reading the datagram immediately
			// observes the incremented counter; roll back if the write fails.
			l.counters.forwarded.Add(1)
			if err := l.deliver(datagram.payload); err != nil {
				l.counters.forwarded.Add(^uint64(0))
				l.counters.dropped.Add(1)
			}
		}
	}
}

// New binds the client-facing UDP socket and prepares a Relay targeting
// Config.Upstream. Call Run to begin forwarding datagrams, or Close if the
// relay is discarded before Run.
func New(config Config) (*Relay, error) {
	if config.Upstream == "" {
		return nil, errors.New("lagrelay: upstream address is required")
	}
	if err := config.InitialProfile.Validate(); err != nil {
		return nil, err
	}

	listen := config.Listen
	if listen == "" {
		listen = "127.0.0.1:0"
	}
	listenAddress, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		return nil, fmt.Errorf("lagrelay: resolve listen %q: %w", listen, err)
	}
	upstreamAddress, err := net.ResolveUDPAddr("udp", config.Upstream)
	if err != nil {
		return nil, fmt.Errorf("lagrelay: resolve upstream %q: %w", config.Upstream, err)
	}
	front, err := net.ListenUDP("udp", listenAddress)
	if err != nil {
		return nil, fmt.Errorf("lagrelay: bind listen %q: %w", listen, err)
	}

	idleTimeout := config.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = DefaultSessionIdleTimeout
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	} else {
		logger = logger.With("component", "lagrelay")
	}

	initial := config.InitialProfile
	relay := &Relay{
		front:       front,
		upstream:    upstreamAddress,
		seed:        config.Seed,
		idleTimeout: idleTimeout,
		logger:      logger,
		sessions:    make(map[netip.AddrPort]*session),
	}
	relay.profile.Store(&initial)
	return relay, nil
}

// Addr returns the local UDP address clients send datagrams to.
func (r *Relay) Addr() net.Addr {
	return r.front.LocalAddr()
}

// UpstreamAddr returns the resolved upstream UDP server address.
func (r *Relay) UpstreamAddr() net.Addr {
	return r.upstream
}

// Profile returns the impairment profile currently applied to new datagrams.
func (r *Relay) Profile() Profile {
	return *r.profile.Load()
}

// SetProfile validates and atomically updates the active impairment profile.
// Datagrams already queued keep the due times assigned on arrival.
func (r *Relay) SetProfile(profile Profile) error {
	if err := profile.Validate(); err != nil {
		return err
	}
	r.profile.Store(&profile)
	r.logger.Info("impairment profile updated",
		"up_delay_ms", profile.UpDelayMS,
		"down_delay_ms", profile.DownDelayMS,
		"jitter_ms", profile.JitterMS,
		"loss_percent", profile.LossPercent,
	)
	return nil
}

// Stats returns a snapshot of forwarded/dropped counters and active sessions.
func (r *Relay) Stats() Stats {
	r.mutex.Lock()
	sessions := len(r.sessions)
	r.mutex.Unlock()
	return Stats{
		UpForwarded:   r.up.forwarded.Load(),
		UpDropped:     r.up.dropped.Load(),
		DownForwarded: r.down.forwarded.Load(),
		DownDropped:   r.down.dropped.Load(),
		Sessions:      sessions,
	}
}

// Close closes the client-facing UDP socket. Closing an already-closed relay
// returns nil.
func (r *Relay) Close() error {
	if err := r.front.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

// Run forwards datagrams until ctx is cancelled or Close is called, then
// tears down all sessions and waits for background workers to exit.
func (r *Relay) Run(ctx context.Context) error {
	runContext, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() {
		cancel()
		r.closeSessions()
		workers.Wait()
	}()

	workers.Add(2)
	go func() {
		defer workers.Done()
		<-runContext.Done()
		_ = r.front.Close()
	}()
	go func() {
		defer workers.Done()
		r.reapIdleSessions(runContext)
	}()

	buffer := make([]byte, maxDatagramBytes)
	for {
		length, client, err := r.front.ReadFromUDPAddrPort(buffer)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("lagrelay: read from client: %w", err)
		}
		active, err := r.sessionFor(runContext, &workers, client)
		if err != nil {
			r.up.dropped.Add(1)
			r.logger.Warn("failed to open upstream session", "client", client.String(), "error", err)
			continue
		}
		now := time.Now()
		active.lastActive.Store(now.UnixNano())
		profile := r.profile.Load()
		payload := append([]byte(nil), buffer[:length]...)
		active.toServer.enqueue(now, payload, profile.UpDelayMS, profile.JitterMS, profile.LossPercent)
	}
}

func (r *Relay) sessionFor(ctx context.Context, workers *sync.WaitGroup, client netip.AddrPort) (*session, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if existing, found := r.sessions[client]; found {
		return existing, nil
	}
	server, err := net.DialUDP("udp", nil, r.upstream)
	if err != nil {
		return nil, err
	}
	sessionContext, cancel := context.WithCancel(ctx)
	r.nextID++
	created := &session{client: client, server: server, cancel: cancel}
	created.lastActive.Store(time.Now().UnixNano())
	created.toServer = newDelayLine(r.seed, r.nextID*2, &r.up, func(payload []byte) error {
		_, writeErr := server.Write(payload)
		return writeErr
	})
	created.toClient = newDelayLine(r.seed, r.nextID*2+1, &r.down, func(payload []byte) error {
		_, writeErr := r.front.WriteToUDPAddrPort(payload, client)
		return writeErr
	})
	r.sessions[client] = created

	workers.Add(3)
	go func() {
		defer workers.Done()
		created.toServer.run(sessionContext)
	}()
	go func() {
		defer workers.Done()
		created.toClient.run(sessionContext)
	}()
	go func() {
		defer workers.Done()
		r.readFromServer(sessionContext, created)
	}()

	r.logger.Info("client session opened",
		"client", client.String(),
		"upstream_local", server.LocalAddr().String(),
		"sessions", len(r.sessions),
	)
	return created, nil
}

func (r *Relay) readFromServer(ctx context.Context, active *session) {
	buffer := make([]byte, maxDatagramBytes)
	retryTimer := time.NewTimer(0)
	if !retryTimer.Stop() {
		<-retryTimer.C
	}
	defer retryTimer.Stop()

	for {
		length, err := active.server.Read(buffer)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			// A connected UDP socket reports ICMP port-unreachable as a read
			// error while the upstream server is starting or restarting. Keep
			// the session alive and back off briefly.
			retryTimer.Reset(50 * time.Millisecond)
			select {
			case <-ctx.Done():
				return
			case <-retryTimer.C:
			}
			continue
		}
		now := time.Now()
		active.lastActive.Store(now.UnixNano())
		profile := r.profile.Load()
		payload := append([]byte(nil), buffer[:length]...)
		active.toClient.enqueue(now, payload, profile.DownDelayMS, profile.JitterMS, profile.LossPercent)
	}
}

func (r *Relay) reapIdleSessions(ctx context.Context) {
	interval := max(r.idleTimeout/4, 5*time.Millisecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-r.idleTimeout).UnixNano()
			var expired []netip.AddrPort
			var remaining int
			r.mutex.Lock()
			for client, active := range r.sessions {
				if active.lastActive.Load() < cutoff {
					active.close()
					delete(r.sessions, client)
					expired = append(expired, client)
				}
			}
			remaining = len(r.sessions)
			r.mutex.Unlock()

			for _, client := range expired {
				r.logger.Info("idle client session expired", "client", client.String(), "sessions", remaining)
			}
		}
	}
}

func (r *Relay) closeSessions() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for client, active := range r.sessions {
		active.close()
		delete(r.sessions, client)
	}
}

func (s *session) close() {
	s.cancel()
	_ = s.server.Close()
}
