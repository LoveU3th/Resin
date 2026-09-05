package proxy

// Independent pre-deployment verification of commit 47ddcff.
//
// Unlike the fault-injection suite, which reports deviations through t.Logf,
// these tests assert hard: they are written so that breaking the slow/bad
// discrimination fails them outright.
//
//	A1 response-header timeout  -> health down, FailureCount untouched
//	A2 RST after connect        -> FailureCount up, breaker trips
//	A3 dial failure             -> FailureCount up, stage == connect
//	B  all four data-plane entry points report failover
//	C  platform opt-out is still honoured
//
// The health recorder wraps the *real* pool, so the breaker and the health
// score under test are the production ones rather than a stand-in.

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/topology"
	M "github.com/sagernet/sing/common/metadata"
)

// --- recorder: observes the channel, then forwards to the real pool ---

type finalHealthRecorder struct {
	*topology.GlobalNodePool

	mu     sync.Mutex
	stages []string
	slow   int
}

func (r *finalHealthRecorder) RecordPassiveStageResult(platformID string, h node.Hash, stage string, success bool) {
	r.mu.Lock()
	r.stages = append(r.stages, fmt.Sprintf("%s/%t", stage, success))
	r.mu.Unlock()
	r.GlobalNodePool.RecordPassiveStageResult(platformID, h, stage, success)
}

func (r *finalHealthRecorder) RecordPassiveSlowFailure(platformID string, h node.Hash) {
	r.mu.Lock()
	r.slow++
	r.mu.Unlock()
	r.GlobalNodePool.RecordPassiveSlowFailure(platformID, h)
}

func (r *finalHealthRecorder) snapshot() (stages []string, slow int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.stages))
	copy(out, r.stages)
	return out, r.slow
}

// waitFor polls because health feedback is emitted from a goroutine.
func (r *finalHealthRecorder) waitFor(t *testing.T, want int, timeout time.Duration) ([]string, int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		stages, slow := r.snapshot()
		if len(stages)+slow >= want {
			return stages, slow
		}
		if time.Now().After(deadline) {
			return stages, slow
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// --- fault servers ---

// newRSTServer accepts, reads the request, then resets: the request reached the
// origin, so this is breakage rather than slowness.
func newRSTServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				buf := make([]byte, 512)
				_, _ = c.Read(buf)
				if tc, ok := c.(*net.TCPConn); ok {
					_ = tc.SetLinger(0)
				}
				_ = c.Close()
			}(c)
		}
	}()
	return ln
}

func refuser() func(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return func(_ context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: network, Err: fmt.Errorf("connection refused")}
	}
}

// --- A1: a slow origin lowers health but must not feed the breaker ---

func TestFinal_SlowOriginLowersHealthWithoutTouchingBreaker(t *testing.T) {
	slowSrv := newSlowServer()
	defer slowSrv.Close()
	su, err := url.Parse(slowSrv.URL)
	if err != nil {
		t.Fatalf("parse slow server url: %v", err)
	}

	env := newFIEnv(t, 1)
	env.outbounds[0].setDial(realDial(M.ParseSocksaddr(su.Host)))
	rec := &finalHealthRecorder{GlobalNodePool: env.pool}
	entry, _ := env.pool.GetEntry(env.hashes[0])
	env.pinLease(t, "acct-1", 0)

	fp := env.newForwardProxy(rec, newMockEventEmitter(),
		FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 120 * time.Millisecond},
		OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 5 * time.Second})

	before := entry.HealthScore()

	for i := 0; i < 3; i++ {
		w := env.forwardRequest(t, fp, http.MethodGet, slowSrv.URL+"/slow", "acct-1", "")
		if w.Code != http.StatusGatewayTimeout {
			t.Fatalf("A1 request %d: status %d, want 504 (a slow origin is a gateway timeout)", i, w.Code)
		}
		time.Sleep(60 * time.Millisecond)
	}

	stages, slow := rec.waitFor(t, 3, 3*time.Second)
	t.Logf("A1 stages=%v slow=%d failureCount=%d circuitOpen=%v health=%.3f (before %.3f)",
		stages, slow, entry.FailureCount.Load(), entry.IsCircuitOpen(), entry.HealthScore(), before)

	if slow != 3 {
		t.Fatalf("A1: 3 abandoned attempts produced %d slow records (stages=%v): "+
			"a response-header timeout must go through RecordPassiveSlowFailure", slow, stages)
	}
	if len(stages) != 0 {
		t.Fatalf("A1: a response-header timeout reached the ordinary failure channel (%v): "+
			"that channel feeds the breaker and would evict a node that is still serving", stages)
	}
	if got := entry.FailureCount.Load(); got != 0 {
		t.Fatalf("A1: FailureCount=%d, want 0 — slowness must not count toward eviction", got)
	}
	if entry.IsCircuitOpen() {
		t.Fatal("A1: a merely slow node was isolated by the breaker")
	}
	if entry.HealthScore() >= before {
		t.Fatalf("A1: health did not drop (before %.3f, after %.3f): slowness must cost weight",
			before, entry.HealthScore())
	}
	if !env.plat.View().Contains(env.hashes[0]) {
		t.Fatal("A1: the slow node left the routable view")
	}
}

// --- A2: an RST after the request was written is breakage, not slowness ---

func TestFinal_RSTAfterConnectTripsBreaker(t *testing.T) {
	ln := newRSTServer(t)

	env := newFIEnv(t, 1)
	env.outbounds[0].setDial(realDial(M.ParseSocksaddr(ln.Addr().String())))
	rec := &finalHealthRecorder{GlobalNodePool: env.pool}
	entry, _ := env.pool.GetEntry(env.hashes[0])
	env.pinLease(t, "acct-1", 0)

	fp := env.newForwardProxy(rec, newMockEventEmitter(),
		FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 5 * time.Second},
		OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 2 * time.Second})

	for i := 0; i < 3; i++ {
		w := env.forwardRequest(t, fp, http.MethodGet, "http://example.com/rst", "acct-1", "")
		if w.Code == http.StatusOK {
			t.Fatalf("A2 request %d unexpectedly succeeded", i)
		}
		time.Sleep(80 * time.Millisecond)
	}

	stages, slow := rec.waitFor(t, 3, 3*time.Second)
	t.Logf("A2 stages=%v slow=%d failureCount=%d circuitOpen=%v health=%.3f",
		stages, slow, entry.FailureCount.Load(), entry.IsCircuitOpen(), entry.HealthScore())

	if slow != 0 {
		t.Fatalf("A2: an RST was recorded as %d slow failure(s): treating a reset as slowness "+
			"would hide real breakage from the breaker", slow)
	}
	if len(stages) < 3 {
		t.Fatalf("A2: expected at least 3 failure records, got %v", stages)
	}
	for _, s := range stages {
		if s != fmt.Sprintf("%s/false", node.PassiveStageTransfer) {
			t.Fatalf("A2: record %q, want a transfer-stage failure (an RST lands after the bytes went out)", s)
		}
	}
	if got := entry.FailureCount.Load(); got < 3 {
		t.Fatalf("A2: FailureCount=%d, want >= 3 (MaxConsecutiveFailures=3): "+
			"a node that resets mid-transfer must still be evicted", got)
	}
	if !entry.IsCircuitOpen() {
		t.Fatal("A2: a node that resets every connection was never isolated")
	}
}

// --- A3: a dial failure is connect-stage breakage ---

func TestFinal_DialFailureIsConnectStageAndTripsBreaker(t *testing.T) {
	env := newFIEnv(t, 1)
	env.outbounds[0].setDial(refuser())
	rec := &finalHealthRecorder{GlobalNodePool: env.pool}
	entry, _ := env.pool.GetEntry(env.hashes[0])
	env.pinLease(t, "acct-1", 0)

	fp := env.newForwardProxy(rec, newMockEventEmitter(),
		FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 5 * time.Second},
		OutboundTransportConfig{DialTimeout: 300 * time.Millisecond, ResponseHeaderTimeout: time.Second})

	for i := 0; i < 3; i++ {
		_ = env.forwardRequest(t, fp, http.MethodGet, "http://example.com/dead", "acct-1", "")
		time.Sleep(80 * time.Millisecond)
	}

	stages, slow := rec.waitFor(t, 3, 3*time.Second)
	t.Logf("A3 stages=%v slow=%d failureCount=%d circuitOpen=%v",
		stages, slow, entry.FailureCount.Load(), entry.IsCircuitOpen())

	if slow != 0 {
		t.Fatalf("A3: a refused dial was recorded as %d slow failure(s)", slow)
	}
	if len(stages) < 3 {
		t.Fatalf("A3: expected at least 3 failure records, got %v", stages)
	}
	for _, s := range stages {
		if s != fmt.Sprintf("%s/false", node.PassiveStageConnect) {
			t.Fatalf("A3: record %q, want a connect-stage failure: an unreachable node must be "+
				"attributed to the connect phase, at full weight", s)
		}
	}
	if !entry.IsCircuitOpen() {
		t.Fatal("A3: a node that cannot be dialed at all was never isolated")
	}
}

// --- A2b: same stage, opposite verdicts — the split is the error nature ---

func TestFinal_TransferStageSplitsOnErrorNature(t *testing.T) {
	st := newAttemptState()
	st.dialAttempted.Store(true)
	st.dialSucceeded.Store(true)
	st.addWritten(128)

	timeoutVerdict := classifyEstablishmentFailure(
		&net.OpError{Op: "read", Net: "tcp", Err: &finalTimeoutErr{}}, st)
	resetVerdict := classifyEstablishmentFailure(
		&net.OpError{Op: "read", Net: "tcp", Err: &finalRSTErr{}}, st)

	if got := timeoutVerdict.stage; got != node.PassiveStageTransfer {
		t.Fatalf("timeout stage: got %q, want transfer", got)
	}
	if !timeoutVerdict.slow {
		t.Fatal("a transfer-phase timeout must be marked slow")
	}
	if timeoutVerdict.retryable {
		t.Fatal("a transfer-phase timeout must not be retried: the origin may already hold the request")
	}
	if resetVerdict.stage != node.PassiveStageTransfer {
		t.Fatalf("reset stage: got %q, want transfer", resetVerdict.stage)
	}
	if resetVerdict.slow {
		t.Fatal("an RST must not be marked slow: it is breakage and the breaker must see it")
	}
	if resetVerdict.retryable {
		t.Fatal("an RST after the bytes went out must not be retried")
	}
	// Same stage, opposite verdicts: the split comes from the error, not the phase.
	if timeoutVerdict.slow == resetVerdict.slow {
		t.Fatal("a timeout and an RST in the same stage must land in different channels")
	}
}

type finalTimeoutErr struct{}

func (*finalTimeoutErr) Error() string   { return "i/o timeout" }
func (*finalTimeoutErr) Timeout() bool   { return true }
func (*finalTimeoutErr) Temporary() bool { return true }

type finalRSTErr struct{}

func (*finalRSTErr) Error() string { return "connection reset by peer" }

// --- B: every data-plane entry point reports failover ---

// TestFinal_ForwardHTTPReportsFailover covers entry point 1: handleHTTP.
func TestFinal_ForwardHTTPReportsFailover(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()
	su, _ := url.Parse(upstream.URL)

	env := newFIEnv(t, 2)
	env.outbounds[0].setDial(refuser())
	env.outbounds[1].setDial(realDial(M.ParseSocksaddr(su.Host)))
	emitter := newMockEventEmitter()
	env.pinLease(t, "acct-1", 0)

	fp := env.newForwardProxy(env.pool, emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 2, AttemptBudget: 5 * time.Second},
		OutboundTransportConfig{DialTimeout: 300 * time.Millisecond, ResponseHeaderTimeout: 2 * time.Second})

	w := env.forwardRequest(t, fp, http.MethodGet, upstream.URL+"/x", "acct-1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("forward HTTP: status %d, want 200 (body=%q)", w.Code, w.Body.String())
	}
	logEv := assertFailoverReported(t, emitter, "forward.handleHTTP")
	assertServedByNode(t, "forward.handleHTTP", logEv, env.hashes[1])
}

// TestFinal_ForwardCONNECTReportsFailover covers entry point 2: handleCONNECT.
func TestFinal_ForwardCONNECTReportsFailover(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	su, _ := url.Parse(upstream.URL)

	env := newFIEnv(t, 2)
	env.outbounds[0].setDial(refuser())
	env.outbounds[1].setDial(realDial(M.ParseSocksaddr(su.Host)))
	emitter := newMockEventEmitter()
	env.pinLease(t, "acct-1", 0)

	fp := env.newForwardProxy(env.pool, emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 2, AttemptBudget: 5 * time.Second},
		OutboundTransportConfig{DialTimeout: 300 * time.Millisecond, ResponseHeaderTimeout: 2 * time.Second})

	proxySrv := httptest.NewServer(fp)
	defer proxySrv.Close()

	conn, err := net.Dial("tcp", proxySrv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Authorization: %s\r\n\r\n",
		basicAuth("tok", "plat:acct-1"))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read CONNECT response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status %d, want 200: the retry should have landed on node 1", resp.StatusCode)
	}
	_ = conn.Close()

	logEv := assertFailoverReported(t, emitter, "forward.handleCONNECT")
	assertServedByNode(t, "forward.handleCONNECT", logEv, env.hashes[1])
}

// TestFinal_ReverseReportsFailover covers entry point 3: reverse ServeHTTP.
func TestFinal_ReverseReportsFailover(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("reverse-ok"))
	}))
	defer upstream.Close()
	su, _ := url.Parse(upstream.URL)

	env := newFIEnv(t, 2)
	env.outbounds[0].setDial(refuser())
	env.outbounds[1].setDial(realDial(M.ParseSocksaddr(su.Host)))
	emitter := newMockEventEmitter()

	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		AuthVersion:    string(config.AuthVersionV1),
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         env.pool,
		Events:         emitter,
		OutboundTransport: OutboundTransportConfig{
			DialTimeout:           300 * time.Millisecond,
			ResponseHeaderTimeout: 2 * time.Second,
		},
		Failover: FailoverConfig{Enabled: true, MaxAttempts: 2, AttemptBudget: 5 * time.Second},
	})

	// Pin the lease to the broken node so the retry deterministically lands on
	// the working one. The reverse path derives the account from the identity
	// segment (plat.acct), so the lease must be pinned under that account.
	env.pinLease(t, "acct", 0)

	path := fmt.Sprintf("/tok/plat.acct/http/%s/x", su.Host)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	rp.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("reverse: status %d, want 200 (body=%q, resinErr=%q)",
			w.Code, w.Body.String(), w.Header().Get("X-Resin-Error"))
	}
	logEv := assertFailoverReported(t, emitter, "reverse.ServeHTTP")
	assertServedByNode(t, "reverse.ServeHTTP", logEv, env.hashes[1])
}

// TestFinal_Socks5ReportsFailover covers entry point 4: SOCKS5 ServeConnContext.
func TestFinal_Socks5ReportsFailover(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("socks5-ok"))
	}))
	defer upstream.Close()
	su, _ := url.Parse(upstream.URL)

	env := newFIEnv(t, 2)
	env.outbounds[0].setDial(refuser())
	env.outbounds[1].setDial(realDial(M.ParseSocksaddr(su.Host)))
	emitter := newMockEventEmitter()

	inbound := NewSocks5Inbound(Socks5InboundConfig{
		ProxyToken:  "tok",
		AuthVersion: string(config.AuthVersionV1),
		Router:      env.router,
		Pool:        env.pool,
		Health:      env.pool,
		Events:      emitter,
		OutboundTransport: OutboundTransportConfig{
			DialTimeout:           300 * time.Millisecond,
			ResponseHeaderTimeout: 2 * time.Second,
		},
		Failover: FailoverConfig{Enabled: true, MaxAttempts: 2, AttemptBudget: 5 * time.Second},
	})
	// The account comes from the SOCKS5 username (plat.acct), so the lease must
	// be pinned under that account.
	env.pinLease(t, "acct", 0)

	clientConn, reader, done := startSocks5Session(t, inbound)
	defer clientConn.Close()

	writeAll(t, clientConn, []byte{socks5Version, 1, socks5MethodUserPass})
	if got := readExactly(t, reader, 2); got[1] != socks5MethodUserPass {
		t.Fatalf("selected method: got %d, want %d", got[1], socks5MethodUserPass)
	}
	writeAll(t, clientConn, socks5UserPassPacket("plat.acct", "tok"))
	if got := readExactly(t, reader, 2); got[1] != socks5UserPassStatusSuccess {
		t.Fatalf("auth status: got %d, want %d", got[1], socks5UserPassStatusSuccess)
	}
	writeAll(t, clientConn, socks5ConnectIPv4Packet(su.Host))
	reply := readExactly(t, reader, 10)
	if reply[1] != socks5ReplySucceeded {
		t.Fatalf("connect reply: got %d, want success (the retry should have landed on node 1)", reply[1])
	}

	_ = clientConn.Close()
	<-done

	logEv := assertFailoverReported(t, emitter, "socks5.ServeConnContext")
	assertServedByNode(t, "socks5.ServeConnContext", logEv, env.hashes[1])
}

// assertFailoverReported checks that the request log for one request carries
// the retry. Without it the first-hop success rate looks flawless while every
// request is being rescued by a second node.
func assertFailoverReported(t *testing.T, emitter *mockEventEmitter, entry string) RequestLogEntry {
	t.Helper()
	select {
	case logEv := <-emitter.logCh:
		if logEv.FailoverAttempts < 2 {
			t.Fatalf("%s: FailoverAttempts=%d, want >= 2 — this entry point does not report "+
				"failover, so HTTPS failures stay invisible in the first-hop metric",
				entry, logEv.FailoverAttempts)
		}
		if logEv.FailoverNodes == "" {
			t.Fatalf("%s: FailoverAttempts=%d but FailoverNodes is empty", entry, logEv.FailoverAttempts)
		}
		return logEv
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: no request-log event was emitted", entry)
		return RequestLogEntry{}
	}
}

// assertServedByNode confirms the request really was rescued by the second
// node: a test that never fails over would otherwise pass vacuously.
func assertServedByNode(t *testing.T, entry string, logEv RequestLogEntry, want node.Hash) {
	t.Helper()
	if logEv.NodeHash != want.Hex() {
		t.Fatalf("%s: served by node %s, want %s — the request did not fail over, so the "+
			"FailoverAttempts assertion proves nothing", entry, logEv.NodeHash, want.Hex())
	}
}

// --- C: the per-platform breaker opt-out still holds ---

func TestFinal_PlatformOptOutIsNotIsolatedByTraffic(t *testing.T) {
	run := func(optOut bool) bool {
		ln := newRSTServer(t)
		env := newFIEnv(t, 1)
		env.plat.PassiveCircuitBreakerDisabled = optOut
		env.outbounds[0].setDial(realDial(M.ParseSocksaddr(ln.Addr().String())))
		entry, _ := env.pool.GetEntry(env.hashes[0])

		fp := env.newForwardProxy(env.pool, newMockEventEmitter(),
			FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 5 * time.Second},
			OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 2 * time.Second})
		env.pinLease(t, "acct-1", 0)

		for i := 0; i < 3; i++ {
			_ = env.forwardRequest(t, fp, http.MethodGet, "http://example.com/optout", "acct-1", "")
			time.Sleep(80 * time.Millisecond)
		}
		t.Logf("C optOut=%v failureCount=%d circuitOpen=%v health=%.3f",
			optOut, entry.FailureCount.Load(), entry.IsCircuitOpen(), entry.HealthScore())
		return entry.IsCircuitOpen()
	}

	// Control: without the opt-out the same traffic must isolate the node.
	// Without this half, the assertion below would pass for the wrong reason.
	if !run(false) {
		t.Fatal("C control: the breaker did not isolate a resetting node on a platform that " +
			"has not opted out — the opt-out assertion proves nothing")
	}
	if run(true) {
		t.Fatal("C: a platform with PassiveCircuitBreakerDisabled=true had its node isolated by " +
			"traffic feedback: the platform ID is being lost on the way to the breaker")
	}
}

// --- C2: the opt-out must also hold on the tunnel (CONNECT) path ---

func TestFinal_PlatformOptOutHoldsOnTunnelPath(t *testing.T) {
	ln := newRSTServer(t)
	env := newFIEnv(t, 1)
	env.plat.PassiveCircuitBreakerDisabled = true
	env.outbounds[0].setDial(realDial(M.ParseSocksaddr(ln.Addr().String())))
	entry, _ := env.pool.GetEntry(env.hashes[0])
	env.pinLease(t, "acct-1", 0)

	fp := env.newForwardProxy(env.pool, newMockEventEmitter(),
		FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 5 * time.Second},
		OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 2 * time.Second})

	proxySrv := httptest.NewServer(fp)
	defer proxySrv.Close()

	for i := 0; i < 3; i++ {
		conn, err := net.Dial("tcp", proxySrv.Listener.Addr().String())
		if err != nil {
			t.Fatalf("dial proxy: %v", err)
		}
		_, _ = fmt.Fprintf(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Authorization: %s\r\n\r\n",
			basicAuth("tok", "plat:acct-1"))
		if _, err := http.ReadResponse(bufio.NewReader(conn), nil); err != nil {
			t.Logf("C2 attempt %d: no CONNECT response (%v)", i, err)
		}
		_ = conn.Close()
		time.Sleep(120 * time.Millisecond)
	}

	t.Logf("C2 opt-out tunnel: failureCount=%d circuitOpen=%v",
		entry.FailureCount.Load(), entry.IsCircuitOpen())
	if entry.IsCircuitOpen() {
		t.Fatalf("C2: a platform with PassiveCircuitBreakerDisabled=true had its node isolated "+
			"by CONNECT traffic (failureCount=%d)", entry.FailureCount.Load())
	}
}
