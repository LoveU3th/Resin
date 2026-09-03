package proxy

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/node"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type noopOutbound struct {
	adapter.Outbound
}

func (n *noopOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("not used in transport-pool tests")
}

func (n *noopOutbound) Tag() string  { return "noop" }
func (n *noopOutbound) Type() string { return "noop" }

func TestOutboundTransportPool_ReusesByNodeHash(t *testing.T) {
	pool := newOutboundTransportPool()
	hash := node.Hash{1}

	t1 := pool.Get(hash, &noopOutbound{}, nil)
	t2 := pool.Get(hash, &noopOutbound{}, nil)

	if t1 != t2 {
		t.Fatal("expected same transport instance for identical node hash")
	}
}

func TestOutboundTransportPool_SplitsByNodeHash(t *testing.T) {
	pool := newOutboundTransportPool()
	ob := &noopOutbound{}
	hash1 := node.Hash{1}
	hash2 := node.Hash{2}

	base := pool.Get(hash1, ob, nil)
	byNodeHash := pool.Get(hash2, ob, nil)
	if base == byNodeHash {
		t.Fatal("expected different transport for different node hash")
	}
}

func TestOutboundTransportPool_UsesKeepAliveTransport(t *testing.T) {
	pool := newOutboundTransportPool()
	ob := &noopOutbound{}
	hash := node.Hash{1}

	transport := pool.Get(hash, ob, nil)
	if transport.DisableKeepAlives {
		t.Fatal("expected keep-alive enabled transport")
	}
}

func TestOutboundTransportPool_EvictRemovesNodeTransport(t *testing.T) {
	pool := newOutboundTransportPool()
	hash := node.Hash{1}
	ob := &noopOutbound{}

	t1 := pool.Get(hash, ob, nil)
	pool.Evict(hash)
	t2 := pool.Get(hash, ob, nil)

	if t1 == t2 {
		t.Fatal("expected a new transport after evict")
	}
}

func TestOutboundTransportPool_AppliesConfiguredLimits(t *testing.T) {
	pool := newOutboundTransportPoolWithConfig(OutboundTransportConfig{
		MaxIdleConns:        9,
		MaxIdleConnsPerHost: 3,
		IdleConnTimeout:     12 * time.Second,
	})
	ob := &noopOutbound{}
	hash := node.Hash{1}

	transport := pool.Get(hash, ob, nil)
	if transport.MaxIdleConns != 9 {
		t.Fatalf("MaxIdleConns: got %d, want %d", transport.MaxIdleConns, 9)
	}
	if transport.MaxIdleConnsPerHost != 3 {
		t.Fatalf("MaxIdleConnsPerHost: got %d, want %d", transport.MaxIdleConnsPerHost, 3)
	}
	if transport.IdleConnTimeout != 12*time.Second {
		t.Fatalf("IdleConnTimeout: got %s, want %s", transport.IdleConnTimeout, 12*time.Second)
	}
}

func TestOutboundTransportPool_CloseAllClearsEntries(t *testing.T) {
	pool := newOutboundTransportPool()
	ob := &noopOutbound{}

	hashA := node.Hash{1}
	hashB := node.Hash{2}
	t1 := pool.Get(hashA, ob, nil)
	_ = pool.Get(hashB, ob, nil)

	pool.CloseAll()

	t2 := pool.Get(hashA, ob, nil)
	if t1 == t2 {
		t.Fatal("expected a new transport after CloseAll")
	}
}

// closeCountingConn records how many times Close was called and returns a
// scripted error from it.
type closeCountingConn struct {
	net.Conn
	closes   atomic.Int32
	closeErr error
}

func (c *closeCountingConn) Close() error {
	c.closes.Add(1)
	return c.closeErr
}

// scriptedOutbound lets a test drive the dial: it keeps the context it was
// handed and can either fail, block until that context ends, or succeed.
type scriptedOutbound struct {
	adapter.Outbound

	blockUntilCtxDone bool
	dialErr           error
	delay             time.Duration

	mu      sync.Mutex
	dialCtx context.Context
}

func (s *scriptedOutbound) DialContext(
	ctx context.Context,
	_ string,
	_ M.Socksaddr,
) (net.Conn, error) {
	s.mu.Lock()
	s.dialCtx = ctx
	s.mu.Unlock()

	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if s.blockUntilCtxDone {
		<-ctx.Done()
		if s.dialErr != nil {
			return nil, s.dialErr
		}
		return nil, ctx.Err()
	}
	if s.dialErr != nil {
		return nil, s.dialErr
	}
	client, _ := net.Pipe()
	return client, nil
}

func (s *scriptedOutbound) dialedCtx() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dialCtx
}

func TestDialCancelConn_CloseCancelsOnceAndReturnsUnderlyingError(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	var cancels atomic.Int32
	closeErr := errors.New("close failed")
	conn := &dialCancelConn{
		Conn:   &closeCountingConn{Conn: client, closeErr: closeErr},
		cancel: func() { cancels.Add(1) },
	}

	if got := conn.Close(); !errors.Is(got, closeErr) {
		t.Fatalf("Close: got %v, want %v", got, closeErr)
	}
	if got := conn.Close(); !errors.Is(got, closeErr) {
		t.Fatalf("second Close: got %v, want %v", got, closeErr)
	}
	if got := cancels.Load(); got != 1 {
		t.Fatalf("cancel calls = %d, want 1", got)
	}
}

func TestDialCancelConn_UnwrapExposesBaseConn(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	base := &closeCountingConn{Conn: client}
	conn := &dialCancelConn{Conn: base, cancel: func() {}}

	if got := conn.Unwrap(); got != net.Conn(base) {
		t.Fatalf("Unwrap: got %T, want the wrapped *closeCountingConn", got)
	}
}

func TestDialCancelConn_ConcurrentCloseIsSafe(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	var cancels atomic.Int32
	conn := &dialCancelConn{Conn: client, cancel: func() { cancels.Add(1) }}

	const closers = 32
	var wg sync.WaitGroup
	wg.Add(closers)
	for i := 0; i < closers; i++ {
		go func() {
			defer wg.Done()
			_ = conn.Close()
		}()
	}
	wg.Wait()

	if got := cancels.Load(); got != 1 {
		t.Fatalf("cancel calls = %d, want 1", got)
	}
}

// The dial timeout must bound the dial only. If it keeps running against a
// successful connection, any connection outliving the timeout is cancelled
// mid-flight — which is exactly the "connects, then breaks" symptom, and the
// resulting error reads as a client cancel so the node is never blamed.
func TestDialOutbound_DialContextStaysLiveWhileConnOpen(t *testing.T) {
	pool := newOutboundTransportPoolWithConfig(OutboundTransportConfig{
		DialTimeout: 100 * time.Millisecond,
	})
	ob := &scriptedOutbound{}

	conn, err := pool.dialOutbound(context.Background(), ob, "tcp", "example.com:443")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Sleep well past the timeout: the timer must have been stopped.
	time.Sleep(300 * time.Millisecond)

	if got := ob.dialedCtx().Err(); got != nil {
		t.Fatalf("dial context cancelled while connection still open: %v", got)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := ob.dialedCtx().Err(); !errors.Is(got, context.Canceled) {
		t.Fatalf("dial context after close: got %v, want context.Canceled", got)
	}
}

// A timed-out dial must read as a timeout. Left as a bare context.Canceled it
// would be classified as a client abort: no 504, and no failure recorded
// against a node that cannot dial at all.
func TestDialOutbound_DialTimeoutAbortsSlowDial(t *testing.T) {
	pool := newOutboundTransportPoolWithConfig(OutboundTransportConfig{
		DialTimeout: 100 * time.Millisecond,
	})
	ob := &scriptedOutbound{blockUntilCtxDone: true}

	start := time.Now()
	_, err := pool.dialOutbound(context.Background(), ob, "tcp", "example.com:443")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected the slow dial to fail")
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("dial returned after %v, want at least the 100ms timeout", elapsed)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatal("timed-out dial must not read as context.Canceled (client abort)")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("errors.Is(err, context.DeadlineExceeded) = false, err = %v", err)
	}
	if proxyErr := classifyUpstreamError(err); proxyErr != ErrUpstreamTimeout {
		t.Fatalf("classifyUpstreamError: got %v, want ErrUpstreamTimeout", proxyErr)
	}
}

// A dial that failed for its own reason must not be reported as our timeout,
// and must still release the dial context so no timer is left behind.
func TestDialOutbound_FailedDialCancelsImmediately(t *testing.T) {
	pool := newOutboundTransportPoolWithConfig(OutboundTransportConfig{
		DialTimeout: time.Minute, // never fires during this test
	})
	ob := &scriptedOutbound{dialErr: errors.New("connection refused")}

	_, err := pool.dialOutbound(context.Background(), ob, "tcp", "example.com:443")
	if err == nil {
		t.Fatal("expected the dial to fail")
	}
	if got := ob.dialedCtx().Err(); got == nil {
		t.Fatal("dial context must be cancelled when the dial fails")
	}
	if proxyErr := classifyUpstreamError(err); proxyErr != ErrUpstreamRequestFailed {
		t.Fatalf("classifyUpstreamError: got %v, want ErrUpstreamRequestFailed", proxyErr)
	}
}

// The outbound transport defaults are written down twice: as constants here
// and as defaults in config/env.go. config cannot import proxy, so they cannot
// be shared — and they already drifted once (ResponseHeaderTimeout was raised
// in one place only). Assert they agree.
func TestTransportDefaultsMatchEnvConfigDefaults(t *testing.T) {
	for k, v := range map[string]string{
		"RESIN_AUTH_VERSION": "LEGACY_V0",
		"RESIN_ADMIN_TOKEN":  "admin-secret",
		"RESIN_PROXY_TOKEN":  "proxy-secret",
	} {
		t.Setenv(k, v)
	}

	cfg, err := config.LoadEnvConfig()
	if err != nil {
		t.Fatalf("LoadEnvConfig: %v", err)
	}

	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"IdleConnTimeout", defaultTransportIdleConnTimeout, cfg.ProxyTransportIdleConnTimeout},
		{"DialTimeout", defaultTransportDialTimeout, cfg.ProxyTransportDialTimeout},
		{"TLSHandshakeTimeout", defaultTransportTLSHandshakeTimeout, cfg.ProxyTransportTLSHandshakeTimeout},
		{"ResponseHeaderTimeout", defaultTransportResponseHeaderTimeout, cfg.ProxyTransportResponseHeaderTimeout},
		{"KeepAlivePeriod", defaultTransportKeepAlivePeriod, cfg.ProxyTransportKeepAlivePeriod},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s: proxy default %v != config default %v", tc.name, tc.got, tc.want)
		}
	}
}

// A caller that goes away must still read as a client cancel, so the node is
// not blamed for the client's own cancellation.
func TestDialOutbound_CallerCancelIsNotATimeout(t *testing.T) {
	pool := newOutboundTransportPoolWithConfig(OutboundTransportConfig{
		DialTimeout: time.Minute,
	})
	ob := &scriptedOutbound{blockUntilCtxDone: true, delay: 50 * time.Millisecond}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err := pool.dialOutbound(ctx, ob, "tcp", "example.com:443")
	if err == nil {
		t.Fatal("expected the dial to fail")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("errors.Is(err, context.Canceled) = false, err = %v", err)
	}
	if proxyErr := classifyUpstreamError(err); proxyErr != nil {
		t.Fatalf("classifyUpstreamError: got %v, want nil (client cancel)", proxyErr)
	}
}
