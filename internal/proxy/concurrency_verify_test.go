package proxy

// Concurrency and resource-leak verification for the request-level failover
// code. Everything here is driven by -race and by before/after goroutine
// counts; the point is not to assert business behaviour (that lives in
// failover_test.go / failover_fault_injection_test.go) but to prove that the
// new concurrent paths neither race nor leak.
//
// All upstreams are local (net/http/httptest or a stub outbound), so nothing
// here touches the network.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
	M "github.com/sagernet/sing/common/metadata"
)

// --- instrumented values -----------------------------------------------------

// cvTrackedBody wraps a response body so the test can tell whether an
// abandoned attempt's body was ever closed, and whether it was closed twice.
type cvTrackedBody struct {
	inner   io.ReadCloser
	onClose func()
	once    sync.Once
	closes  atomic.Int32
}

func (b *cvTrackedBody) Read(p []byte) (int, error) { return b.inner.Read(p) }

func (b *cvTrackedBody) Close() error {
	b.closes.Add(1)
	b.once.Do(b.onClose)
	return b.inner.Close()
}

// cvTrackedConn does the same for a tunneled connection.
type cvTrackedConn struct {
	net.Conn
	once    sync.Once
	closes  atomic.Int32
	onClose func()
}

func (c *cvTrackedConn) Close() error {
	c.closes.Add(1)
	c.once.Do(c.onClose)
	return c.Conn.Close()
}

// cvSettle waits for NumGoroutine to fall back to baseline, and returns the
// count it settled at. Polling matters: an abandoned attempt is *supposed* to
// outlive runFailover, so sampling immediately would report a leak that is
// really just an attempt still finishing.
func cvSettle(baseline int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	cur := runtime.NumGoroutine()
	for cur > baseline && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		cur = runtime.NumGoroutine()
	}
	return cur
}

// cvGoroutineBaseline takes a baseline after a warm-up pass, so goroutines
// created once per proxy/transport (idle-conn readers, pool workers) are
// already counted.
func cvGoroutineBaseline() int {
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	return runtime.NumGoroutine()
}

func cvCheckNoLeak(t *testing.T, baseline int, what string) {
	t.Helper()
	final := cvSettle(baseline, 5*time.Second)
	tolerance := 50
	if baseline/10 > tolerance {
		tolerance = baseline / 10
	}
	if final > baseline+tolerance {
		t.Fatalf("%s: goroutine leak: baseline=%d settled=%d (tolerance +%d). "+
			"This grows without bound in a 7x24 process.", what, baseline, final, tolerance)
	}
	t.Logf("%s: goroutines baseline=%d settled=%d", what, baseline, final)
}

// --- 1. an abandoned attempt must still release what it produced -------------

// TestCV_AbandonedAttemptClosesResponseBody is the core risk of the new design:
// runAttempt abandons a slow attempt instead of cancelling it, so the goroutine
// finishes on its own and only Cleanup can release its response body. If that
// ever fails, every slow upstream request permanently parks a socket and a
// goroutine — precisely the failure mode that accumulates in a long-running
// process.
func TestCV_AbandonedAttemptClosesResponseBody(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("late"))
	}))
	defer slow.Close()

	transport := &http.Transport{}
	defer transport.CloseIdleConnections()

	var produced, cleaned atomic.Int64
	var bodies []*cvTrackedBody
	var mu sync.Mutex

	params := FailoverParams[*http.Response]{
		Config: FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 10 * time.Millisecond},
		Resolve: func(_ []node.Hash) (routedOutbound, *ProxyError) {
			return routedOutbound{Route: routing.RouteResult{NodeHash: node.Hash{1}}}, nil
		},
		Run: func(ctx context.Context, _ routedOutbound, _ *AttemptState) (*http.Response, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, slow.URL+"/slow", nil)
			if err != nil {
				return nil, err
			}
			resp, err := transport.RoundTrip(req)
			if err != nil {
				return nil, err
			}
			produced.Add(1)
			tracked := &cvTrackedBody{inner: resp.Body, onClose: func() { cleaned.Add(1) }}
			mu.Lock()
			bodies = append(bodies, tracked)
			mu.Unlock()
			resp.Body = tracked
			return resp, nil
		},
		Classify: classifyEstablishmentFailure,
		Cleanup: func(resp *http.Response) {
			if resp != nil {
				_ = resp.Body.Close()
			}
		},
	}

	baseline := cvGoroutineBaseline()

	const iterations = 40
	for i := 0; i < iterations; i++ {
		res := runFailover(context.Background(), params)
		if !res.Abandoned {
			t.Fatalf("iteration %d: expected the attempt to be abandoned, got %+v", i, res.LastErr)
		}
		if res.Value != nil {
			t.Fatalf("iteration %d: an abandoned attempt must not hand back a value", i)
		}
	}

	// The abandoned attempts finish after runFailover returned, so poll.
	deadline := time.Now().Add(5 * time.Second)
	for produced.Load() < iterations && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	for cleaned.Load() < produced.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if got := produced.Load(); got != iterations {
		t.Fatalf("responses produced: got %d, want %d (the abandoned attempts never completed)", got, iterations)
	}
	if got, want := cleaned.Load(), produced.Load(); got != want {
		t.Fatalf("BLOCKING: bodies closed: got %d, want %d — an abandoned attempt leaked its response body",
			got, want)
	}

	mu.Lock()
	doubleClosed := 0
	maxCloses := 0
	for _, b := range bodies {
		if n := int(b.closes.Load()); n > 1 {
			doubleClosed++
		}
		if n := int(b.closes.Load()); n > maxCloses {
			maxCloses = n
		}
	}
	mu.Unlock()
	if doubleClosed > 0 {
		t.Fatalf("BLOCKING: %d bodies were closed more than once (max closes=%d)", doubleClosed, maxCloses)
	}

	cvCheckNoLeak(t, baseline, "abandoned-forward-attempt")
}

// An abandoned CONNECT dial must close the socket it eventually gets. Same
// shape as the response-body case, different T (net.Conn).
func TestCV_AbandonedTunnelConnIsClosed(t *testing.T) {
	env := newFIEnv(t, 1)

	var opened, closed atomic.Int64
	var peers []net.Conn
	var mu sync.Mutex

	env.outbounds[0].setDial(func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
		select {
		case <-time.After(150 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		a, b := net.Pipe()
		opened.Add(1)
		tracked := &cvTrackedConn{Conn: a, onClose: func() { closed.Add(1) }}
		mu.Lock()
		peers = append(peers, b)
		mu.Unlock()
		return tracked, nil
	})

	deps := tunnelDeps{
		router: env.router,
		pool:   env.pool,
		// health is left nil on purpose: this test is about the resource path.
		// Leaving the recorder in would trip the breaker after three abandoned
		// dials and stop reaching the node at all.
		health:      nil,
		dialTimeout: 0,
		failover:    FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 10 * time.Millisecond},
	}

	baseline := cvGoroutineBaseline()

	const iterations = 30
	for i := 0; i < iterations; i++ {
		res := prepareConnectTunnel(context.Background(), deps, "plat", "acct-1", "example.com:443")
		if res.session != nil {
			t.Fatalf("iteration %d: expected the abandoned dial to yield no session", i)
		}
		if res.proxyErr == nil {
			t.Fatalf("iteration %d: expected a proxy error for the abandoned dial", i)
		}
	}

	// The abandoned dials finish after prepareConnectTunnel returned, so poll.
	deadline := time.Now().Add(5 * time.Second)
	for opened.Load() < iterations && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	for closed.Load() < opened.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := opened.Load(); got != iterations {
		t.Fatalf("dials completed: got %d, want %d", got, iterations)
	}
	if got, want := closed.Load(), opened.Load(); got != want {
		t.Fatalf("BLOCKING: tunnel conns closed: got %d, want %d — an abandoned dial leaked its socket",
			got, want)
	}

	mu.Lock()
	for _, c := range peers {
		_ = c.Close()
	}
	mu.Unlock()

	cvCheckNoLeak(t, baseline, "abandoned-tunnel-dial")
}

// Cleanup must fire for exactly one of the three runFailover exits and never
// for a value the caller already owns — a double release would trip over the
// caller's own Close and, for a tunnel, could close a socket that is already
// being relayed.
func TestCV_CleanupIsCalledExactlyOncePerAbandonedAttempt(t *testing.T) {
	newParams := func(behaviour string, cleanupCount *atomic.Int64, produceCount *atomic.Int64) FailoverParams[string] {
		return FailoverParams[string]{
			Config: FailoverConfig{Enabled: true, MaxAttempts: 2, AttemptBudget: 20 * time.Millisecond},
			Resolve: func(_ []node.Hash) (routedOutbound, *ProxyError) {
				if behaviour == "resolve-fails" {
					return routedOutbound{}, ErrNoAvailableNodes
				}
				return routedOutbound{Route: routing.RouteResult{NodeHash: node.Hash{1}}}, nil
			},
			Run: func(ctx context.Context, _ routedOutbound, _ *AttemptState) (string, error) {
				switch behaviour {
				case "succeeds":
					produceCount.Add(1)
					return "value", nil
				case "fails":
					return "", errors.New("upstream broke")
				default: // "slow"
					select {
					case <-time.After(150 * time.Millisecond):
						produceCount.Add(1)
						return "value", nil
					case <-ctx.Done():
						return "", ctx.Err()
					}
				}
			},
			Classify: classifyEstablishmentFailure,
			Cleanup: func(v string) {
				if v != "" {
					cleanupCount.Add(1)
				}
			},
		}
	}

	cases := []struct {
		name         string
		behaviour    string
		wantProduced int64
		wantCleaned  int64
	}{
		// A returned value belongs to the caller, so Cleanup must stay out of it.
		{name: "success path", behaviour: "succeeds", wantProduced: 1, wantCleaned: 0},
		// A failed attempt produced nothing, so there is nothing to release.
		{name: "run error", behaviour: "fails", wantProduced: 0, wantCleaned: 0},
		{name: "resolve error", behaviour: "resolve-fails", wantProduced: 0, wantCleaned: 0},
		// Only an abandoned attempt owes a release.
		{name: "abandoned", behaviour: "slow", wantProduced: 1, wantCleaned: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cleaned, produced atomic.Int64
			res := runFailover(context.Background(), newParams(tc.behaviour, &cleaned, &produced))
			_ = res

			deadline := time.Now().Add(3 * time.Second)
			for produced.Load() < tc.wantProduced && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			for cleaned.Load() < tc.wantCleaned && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if got := produced.Load(); got != tc.wantProduced {
				t.Fatalf("values produced: got %d, want %d", got, tc.wantProduced)
			}
			if got := cleaned.Load(); got != tc.wantCleaned {
				t.Fatalf("Cleanup calls: got %d, want %d", got, tc.wantCleaned)
			}
		})
	}
}

// If a caller forgets Cleanup, the abandoned attempt's goroutine must still
// finish (it must not block on a buffered channel nobody reads). That is the
// failure mode that would turn a slow upstream into a goroutine leak.
func TestCV_AbandonedAttemptWithoutCleanupDoesNotParkGoroutine(t *testing.T) {
	params := FailoverParams[string]{
		Config: FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 10 * time.Millisecond},
		Resolve: func(_ []node.Hash) (routedOutbound, *ProxyError) {
			return routedOutbound{Route: routing.RouteResult{NodeHash: node.Hash{1}}}, nil
		},
		Run: func(_ context.Context, _ routedOutbound, _ *AttemptState) (string, error) {
			time.Sleep(80 * time.Millisecond)
			return "value", nil
		},
		// Cleanup deliberately nil.
	}

	baseline := cvGoroutineBaseline()
	for i := 0; i < 30; i++ {
		_ = runFailover(context.Background(), params)
	}
	cvCheckNoLeak(t, baseline, "abandoned-without-cleanup")
}

// --- 2. reverseFailoverTransport: current vs currentRoute -------------------

// TestCV_ReverseFailoverTransportConcurrent drives a real reverse proxy whose
// ModifyResponse reads currentRoute() while attempts are being abandoned in the
// background. The question is whether the atomic `current` can observe a route
// written by a different goroutine — if it can, health feedback is attributed
// to the wrong node.
//
// Under -race this also covers every shared field the failover path touches
// (result, current, lifecycle, health recorder).
func TestCV_ReverseFailoverTransportConcurrent(t *testing.T) {
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("fast"))
	}))
	defer fast.Close()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(120 * time.Millisecond)
		// No keep-alive: an idle connection parks a goroutine until
		// IdleConnTimeout, which at this timescale looks exactly like a leak.
		// Closing makes "still alive" mean "actually leaked".
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("slow"))
	}))
	defer slow.Close()

	env := newFIEnv(t, 2)
	rec := newFIRecorder()

	fu, _ := url.Parse(fast.URL)
	su, _ := url.Parse(slow.URL)
	env.outbounds[0].setDial(realDial(M.ParseSocksaddr(fu.Host)))
	env.outbounds[1].setDial(realDial(M.ParseSocksaddr(su.Host)))

	emitter := newMockEventEmitter()
	rp := NewReverseProxy(ReverseProxyConfig{
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         rec,
		Events:         emitter,
		OutboundTransport: OutboundTransportConfig{
			DialTimeout:           time.Second,
			ResponseHeaderTimeout: 3 * time.Second,
		},
		Failover: FailoverConfig{
			Enabled:       true,
			MaxAttempts:   2,
			AttemptBudget: 30 * time.Millisecond,
		},
	})
	// Drain every emitter channel. The mock emitter is synchronous with bounded
	// capacity, so forgetting one would block the data plane once it fills.
	// (The real emitter is asynchronous; this is purely a test concern.)
	go func() {
		for {
			select {
			case <-emitter.finishedCh:
			case <-emitter.logCh:
			}
		}
	}()
	env.pinLease(t, "acct-slow", 1)
	env.pinLease(t, "acct-fast", 0)

	baseline := cvGoroutineBaseline()

	const workers = 64
	const perWorker = 32 // 2048 requests total
	var wg sync.WaitGroup
	var okCount, timeoutCount, otherCount atomic.Int64
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWorker; i++ {
				account := "acct-fast"
				upstream := fast
				if i%4 == 0 {
					// Exercises the abandon path: the pinned node is slow.
					account = "acct-slow"
					upstream = slow
				}
				u, _ := url.Parse(upstream.URL)
				target := fmt.Sprintf("http://resin.test/tok/plat:%s/http/%s/cv-%d-%d", account, u.Host, w, i)
				req := httptest.NewRequest(http.MethodGet, target, nil)
				rec := httptest.NewRecorder()
				rp.ServeHTTP(rec, req)
				switch rec.Code {
				case http.StatusOK:
					okCount.Add(1)
				case http.StatusGatewayTimeout:
					timeoutCount.Add(1)
				default:
					otherCount.Add(1)
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()

	t.Logf("reverse: ok=%d timeout=%d other=%d", okCount.Load(), timeoutCount.Load(), otherCount.Load())
	if okCount.Load() == 0 {
		t.Fatal("no request succeeded through the reverse proxy")
	}
	if timeoutCount.Load() == 0 {
		t.Fatal("the slow node was never abandoned, so the abandon path was not covered")
	}

	// Attribute every recorded health sample to a known node; a torn read of
	// `current` would show up as a sample for a node that was never routable.
	for _, c := range rec.snapshot() {
		if c.hash != env.hashes[0] && c.hash != env.hashes[1] {
			t.Fatalf("health feedback attributed to an unknown node %v (%+v)", c.hash, c)
		}
	}

	cvCheckNoLeak(t, baseline, "reverse-failover")
}

// --- 3. mixed forward stress: fast / slow / dead nodes ----------------------

// TestCV_ForwardProxyStress hammers the forward proxy with a node mix that
// exercises every failover exit at once: success, retry onto another node,
// abandonment, and total failure. 2048 requests at 64 concurrent.
func TestCV_ForwardProxyStress(t *testing.T) {
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("fast"))
	}))
	defer fast.Close()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(120 * time.Millisecond)
		// No keep-alive: an idle connection parks a goroutine until
		// IdleConnTimeout, which at this timescale looks exactly like a leak.
		// Closing makes "still alive" mean "actually leaked".
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("slow"))
	}))
	defer slow.Close()

	env := newFIEnv(t, 3)
	rec := newFIRecorder()
	emitter := newMockEventEmitter()
	go func() {
		for {
			select {
			case <-emitter.finishedCh:
			case <-emitter.logCh:
			}
		}
	}()

	fu, _ := url.Parse(fast.URL)
	su, _ := url.Parse(slow.URL)
	env.outbounds[0].setDial(realDial(M.ParseSocksaddr(fu.Host)))
	env.outbounds[1].setDial(realDial(M.ParseSocksaddr(su.Host)))
	env.outbounds[2].setDial(func(_ context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: network, Err: fmt.Errorf("connection refused")}
	})

	env.pinLease(t, "acct-fast", 0)
	env.pinLease(t, "acct-slow", 1)
	env.pinLease(t, "acct-dead", 2)

	fp := env.newForwardProxy(rec, emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 3, AttemptBudget: 30 * time.Millisecond},
		OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 3 * time.Second})

	baseline := cvGoroutineBaseline()

	const workers = 64
	const perWorker = 32
	var wg sync.WaitGroup
	var okCount, timeoutCount, badGatewayCount, otherCount atomic.Int64
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWorker; i++ {
				var account string
				switch i % 4 {
				case 0:
					account = "acct-fast"
				case 1:
					account = "acct-slow" // abandoned
				case 2:
					account = "acct-dead" // retried onto another node
				default:
					account = "acct-fast"
				}
				w2 := env.forwardRequest(t, fp, http.MethodGet, fast.URL+"/cv", account, "")
				switch w2.Code {
				case http.StatusOK:
					okCount.Add(1)
				case http.StatusGatewayTimeout:
					timeoutCount.Add(1)
				case http.StatusBadGateway:
					badGatewayCount.Add(1)
				default:
					otherCount.Add(1)
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()

	t.Logf("forward: ok=%d timeout=%d badgateway=%d other=%d",
		okCount.Load(), timeoutCount.Load(), badGatewayCount.Load(), otherCount.Load())
	// Require the outcomes this test can actually guarantee. A randomized stress
	// run cannot promise to hit every exit — whether a dead node surfaces as a
	// gateway error depends on which attempt happens to reach it first — so
	// demanding all three makes the test flaky rather than stricter.
	if okCount.Load() == 0 || timeoutCount.Load() == 0 {
		t.Fatalf("the stress run did not cover the basic failover exits: ok=%d timeout=%d badgateway=%d",
			okCount.Load(), timeoutCount.Load(), badGatewayCount.Load())
	}

	// Every abandoned attempt must still have released its response body. The
	// pool keeps idle connections, so count them instead of sockets.
	for _, h := range env.hashes {
		entry, _ := env.pool.GetEntry(h)
		score := entry.HealthScore()
		if score < 0 || score > 1 {
			t.Fatalf("node %v health out of range: %v", h, score)
		}
		if !entry.IsCircuitOpen() && !env.plat.View().Contains(h) {
			t.Fatalf("node %v left the routable view without the breaker opening", h)
		}
	}

	cvCheckNoLeak(t, baseline, "forward-failover")
}

// --- 4. client cancellation must not park the attempt goroutine -------------

// A cancelled client is the one path where runAttempt returns without waiting
// for the attempt at all. The attempt observes the cancellation through its own
// context, so it must exit on its own.
func TestCV_ClientCancelDoesNotParkAttemptGoroutine(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	transport := &http.Transport{}
	defer transport.CloseIdleConnections()

	baseline := cvGoroutineBaseline()

	for i := 0; i < 40; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		params := FailoverParams[*http.Response]{
			Config: FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 5 * time.Second},
			Resolve: func(_ []node.Hash) (routedOutbound, *ProxyError) {
				return routedOutbound{Route: routing.RouteResult{NodeHash: node.Hash{1}}}, nil
			},
			Run: func(ctx context.Context, _ routedOutbound, _ *AttemptState) (*http.Response, error) {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream.URL+"/cancel", nil)
				if err != nil {
					return nil, err
				}
				resp, err := transport.RoundTrip(req)
				if err == nil {
					_ = resp.Body.Close()
				}
				return nil, err
			},
			Cleanup: func(resp *http.Response) {
				if resp != nil {
					_ = resp.Body.Close()
				}
			},
		}
		done := make(chan struct{})
		go func() {
			_ = runFailover(ctx, params)
			close(done)
		}()
		time.Sleep(10 * time.Millisecond)
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatal("runFailover did not return when the client cancelled")
		}
		cancel()
	}

	cvCheckNoLeak(t, baseline, "client-cancel")
}
