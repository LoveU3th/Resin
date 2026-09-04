package proxy

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type OutboundTransportConfig struct {
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration
	// DialTimeout bounds a single outbound dial. Without it a node that never
	// answers leaves the goroutine and its fd parked indefinitely.
	DialTimeout time.Duration
	// TLSHandshakeTimeout bounds the upstream TLS handshake.
	TLSHandshakeTimeout time.Duration
	// ResponseHeaderTimeout bounds the wait for upstream response headers.
	// It deliberately does not cover body transfer, so long-lived streams and
	// large downloads are unaffected.
	ResponseHeaderTimeout time.Duration
	// KeepAlivePeriod is the TCP keep-alive probe interval. It applies only to
	// direct (bypass) connections, which are dialed by net.Dialer here.
	// Connections through a node outbound are configured by sing-box and
	// cannot be reached from this side — see dialOutbound.
	KeepAlivePeriod time.Duration
}

const (
	defaultTransportMaxIdleConns        = 1024
	defaultTransportMaxIdleConnsPerHost = 64
	// defaultTransportIdleConnTimeout is deliberately shorter than the ~60s
	// idle cutoff most upstreams and proxy nodes apply. It is the only
	// practical defence against half-open connections on the node path: a peer
	// that vanished without a FIN never shows up on a background read, and the
	// TCP keep-alive applied by sing-box only starts probing after 10 idle
	// minutes, far too late to matter here. Setting IdleConnTimeout above the
	// upstream's own cutoff is what produces "it connects, then breaks
	// immediately".
	defaultTransportIdleConnTimeout       = 45 * time.Second
	defaultTransportDialTimeout           = 10 * time.Second
	defaultTransportTLSHandshakeTimeout   = 10 * time.Second
	// Generous because a false 502 breaks working traffic: this bounds the wait
	// for response headers only, so SSE and large downloads are unaffected, but
	// long-polling and slow-first-byte APIs would be. Matches the default in
	// config/env.go — keep the two in step.
	defaultTransportResponseHeaderTimeout = 60 * time.Second
	defaultTransportKeepAlivePeriod       = 30 * time.Second
)

func normalizeOutboundTransportConfig(cfg OutboundTransportConfig) OutboundTransportConfig {
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = defaultTransportMaxIdleConns
	}
	if cfg.MaxIdleConnsPerHost <= 0 {
		cfg.MaxIdleConnsPerHost = defaultTransportMaxIdleConnsPerHost
	}
	if cfg.IdleConnTimeout <= 0 {
		cfg.IdleConnTimeout = defaultTransportIdleConnTimeout
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultTransportDialTimeout
	}
	if cfg.TLSHandshakeTimeout <= 0 {
		cfg.TLSHandshakeTimeout = defaultTransportTLSHandshakeTimeout
	}
	// 0 is meaningful for this one: it disables the limit, matching net/http.
	// Only a negative value falls back to the default.
	if cfg.ResponseHeaderTimeout < 0 {
		cfg.ResponseHeaderTimeout = defaultTransportResponseHeaderTimeout
	}
	if cfg.KeepAlivePeriod <= 0 {
		cfg.KeepAlivePeriod = defaultTransportKeepAlivePeriod
	}
	return cfg
}

// OutboundTransportPool manages reusable outbound HTTP transports keyed by node hash.
// A single instance should be shared by forward/reverse proxies so keep-alive pools
// are reused and can be evicted on node removal.
type OutboundTransportPool struct {
	config     OutboundTransportConfig
	transports *xsync.Map[node.Hash, *http.Transport]
}

func newOutboundTransportPool() *OutboundTransportPool {
	return NewOutboundTransportPool(OutboundTransportConfig{})
}

func newOutboundTransportPoolWithConfig(cfg OutboundTransportConfig) *OutboundTransportPool {
	return NewOutboundTransportPool(cfg)
}

// NewOutboundTransportPool creates a transport pool with normalized settings.
func NewOutboundTransportPool(cfg OutboundTransportConfig) *OutboundTransportPool {
	return &OutboundTransportPool{
		config:     normalizeOutboundTransportConfig(cfg),
		transports: xsync.NewMap[node.Hash, *http.Transport](),
	}
}

// Get returns a reusable transport for the given node hash.
func (p *OutboundTransportPool) Get(
	hash node.Hash,
	ob adapter.Outbound,
	sink MetricsEventSink,
) *http.Transport {
	transport, _ := p.transports.LoadOrCompute(hash, func() (*http.Transport, bool) {
		return p.newReusableOutboundTransport(ob, sink), false
	})
	return transport
}

// Evict closes idle connections for one node transport and removes it from pool.
func (p *OutboundTransportPool) Evict(hash node.Hash) {
	transport, ok := p.transports.LoadAndDelete(hash)
	if !ok || transport == nil {
		return
	}
	transport.CloseIdleConnections()
}

// CloseAll closes idle connections and clears all pooled transports.
func (p *OutboundTransportPool) CloseAll() {
	p.transports.Range(func(_ node.Hash, transport *http.Transport) bool {
		if transport != nil {
			transport.CloseIdleConnections()
		}
		return true
	})
	p.transports.Clear()
}

func (p *OutboundTransportPool) newReusableOutboundTransport(ob adapter.Outbound, sink MetricsEventSink) *http.Transport {
	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := p.dialOutbound(ctx, ob, network, addr)
			if err != nil {
				return nil, err
			}
			if sink != nil {
				sink.OnConnectionLifecycle(ConnectionOutbound, ConnectionOpen)
				conn = newCountingConn(conn, sink)
			}
			return conn, nil
		},
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          p.config.MaxIdleConns,
		MaxIdleConnsPerHost:   p.config.MaxIdleConnsPerHost,
		IdleConnTimeout:       p.config.IdleConnTimeout,
		TLSHandshakeTimeout:   p.config.TLSHandshakeTimeout,
		ResponseHeaderTimeout: p.config.ResponseHeaderTimeout,
	}
}

// dialOutbound dials through the node outbound with a bounded timeout.
//
// It does not set TCP keep-alive: see the note inside for why.
//
// The timeout must bound the dial only. context.WithTimeout cannot be used
// here: its timer fires unconditionally at the deadline, and outbound
// implementations may tie a connection's lifetime to the dial context, so a
// fired timer would tear down a connection still serving traffic — which is
// the very "connects, then breaks" symptom this code exists to prevent.
func (p *OutboundTransportPool) dialOutbound(
	ctx context.Context,
	ob adapter.Outbound,
	network, addr string,
) (net.Conn, error) {
	// Bounded dial, unbounded connection: the timer is stopped once the dial
	// succeeds, so it can never fire against a live connection.
	dialCtx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(p.config.DialTimeout, cancel)
	defer timer.Stop()

	conn, err := ob.DialContext(dialCtx, network, M.ParseSocksaddr(addr))
	if err != nil {
		// Decide before cancelling: once we cancel, dialCtx.Err() is set
		// regardless of whether the timer or the dial failed.
		timedOut := dialCtx.Err() != nil && ctx.Err() == nil
		cancel()
		if timedOut {
			// The caller's context is still live, so this was our own timer.
			err = &dialTimeoutError{Err: err}
		}
		return nil, err
	}

	// TCP keep-alive on this path is set by sing-box, not here. Proxy protocols
	// (shadowsocks, vmess, trojan, ...) hand back an encrypted wrapper around
	// the real connection, so there is no *net.TCPConn to reach from this side;
	// the underlying TCP connection is owned and configured by sing-box's own
	// dialer (common/dialer/default.go sets KeepAlive and a keep-alive period).
	//
	// That sing-box default is TCPKeepAliveInitial = 10 minutes and is not
	// configurable, so it does nothing for upstreams that drop idle connections
	// after ~60s. IdleConnTimeout is what actually bounds how long a connection
	// the peer may have forgotten can sit in the pool and be handed out again.
	//
	// The cancel is deferred to connection close rather than run here: some
	// outbound protocols tie their underlying connection lifetime to the dial
	// context, so cancelling while the connection is in use would break it.
	return &dialCancelConn{Conn: conn, cancel: cancel}, nil
}

// dialTimeoutError marks a dial that was aborted by our own timeout.
type dialTimeoutError struct{ Err error }

func (e *dialTimeoutError) Error() string {
	return "dial outbound: timed out (" + e.Err.Error() + ")"
}

// Timeout makes os.IsTimeout classify this as a timeout so the request is
// answered with 504 and the failure is counted against the node.
func (e *dialTimeoutError) Timeout() bool { return true }

// Unwrap deliberately does not expose the underlying context.Canceled:
// classifyUpstreamError reads that as "the client went away" and would discard
// the failure without recording it, hiding a node that cannot dial at all.
func (e *dialTimeoutError) Unwrap() error { return context.DeadlineExceeded }

// dialCancelConn releases the dial context when the connection closes.
type dialCancelConn struct {
	net.Conn
	closeOnce sync.Once
	cancel    context.CancelFunc
}

func (c *dialCancelConn) Close() error {
	err := c.Conn.Close()
	c.closeOnce.Do(c.cancel)
	return err
}

// Unwrap exposes the wrapped connection, following the usual convention for
// connection wrappers so callers can reach the underlying connection.
func (c *dialCancelConn) Unwrap() net.Conn { return c.Conn }

func newDirectHTTPTransport(cfg OutboundTransportConfig, sink MetricsEventSink) *http.Transport {
	cfg = normalizeOutboundTransportConfig(cfg)
	dialer := &net.Dialer{
		Timeout:   cfg.DialTimeout,
		KeepAlive: cfg.KeepAlivePeriod,
	}
	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if sink != nil {
				sink.OnConnectionLifecycle(ConnectionOutbound, ConnectionOpen)
				conn = newCountingConn(conn, sink)
			}
			return conn, nil
		},
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
	}
}
