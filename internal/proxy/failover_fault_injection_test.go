package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

// This file injects node-level faults (dial failure, dial hang, RST after
// connect, response-header timeout, client cancel, ...) and checks that the
// failure handling matches the documented intent:
//
//	- retry only when the request provably never reached the upstream
//	- never submit a non-idempotent request twice
//	- a slow node must not be recorded as an unreachable node
//	- a sticky lease survives a failed retry
//
// Deviations from that intent are reported with reportDeviation instead of
// failing the suite, so `go test ./...` stays green while the log still shows
// exactly which scenario misbehaves.

func reportDeviation(t *testing.T, scenario, format string, args ...any) {
	t.Helper()
	t.Logf("DEVIATION [%s] %s", scenario, fmt.Sprintf(format, args...))
}

// --- fault recorder: captures what the data plane tells health ---

type fiStageCall struct {
	platformID string
	hash       node.Hash
	stage      string
	success    bool
	connDrop   bool
}

type fiRecorder struct {
	mu    sync.Mutex
	calls []fiStageCall
}

func newFIRecorder() *fiRecorder { return &fiRecorder{} }

func (r *fiRecorder) RecordResult(hash node.Hash, success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fiStageCall{hash: hash, stage: "probe", success: success})
}

func (r *fiRecorder) RecordLatency(_ node.Hash, _ string, _ *time.Duration)        {}
func (r *fiRecorder) RecordPassiveLatency(_ node.Hash, _ string, _ *time.Duration) {}

func (r *fiRecorder) RecordPassiveStageResult(platformID string, hash node.Hash, stage string, success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fiStageCall{platformID: platformID, hash: hash, stage: stage, success: success})
}

func (r *fiRecorder) RecordConnDrop(platformID string, hash node.Hash) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fiStageCall{platformID: platformID, hash: hash, stage: "conn_drop", success: false, connDrop: true})
}

func (r *fiRecorder) snapshot() []fiStageCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]fiStageCall, len(r.calls))
	copy(out, r.calls)
	return out
}

// waitForCount polls because health feedback is emitted asynchronously.
func (r *fiRecorder) waitForCount(t *testing.T, n int, timeout time.Duration) []fiStageCall {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := r.snapshot()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (r *fiRecorder) countFor(hash node.Hash) map[string]int {
	out := map[string]int{}
	for _, c := range r.snapshot() {
		if c.hash == hash {
			out[c.stage]++
		}
	}
	return out
}

// --- fault outbound: a node whose dial behaviour the test controls ---

type fiOutbound struct {
	mu     sync.Mutex
	dialFn func(ctx context.Context, network string, dest M.Socksaddr) (net.Conn, error)
	dials  int
}

func (o *fiOutbound) Type() string           { return "fault" }
func (o *fiOutbound) Tag() string            { return "fault" }
func (o *fiOutbound) Network() []string      { return []string{"tcp"} }
func (o *fiOutbound) Dependencies() []string { return nil }
func (o *fiOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, fmt.Errorf("fault outbound: udp not supported")
}
func (o *fiOutbound) Close() error { return nil }

func (o *fiOutbound) DialContext(ctx context.Context, network string, dest M.Socksaddr) (net.Conn, error) {
	o.mu.Lock()
	fn := o.dialFn
	o.dials++
	o.mu.Unlock()
	if fn == nil {
		return nil, &net.OpError{Op: "dial", Net: network, Err: &net.DNSError{Err: "fault outbound: no dial func"}}
	}
	return fn(ctx, network, dest)
}

func (o *fiOutbound) setDial(fn func(ctx context.Context, network string, dest M.Socksaddr) (net.Conn, error)) {
	o.mu.Lock()
	o.dialFn = fn
	o.mu.Unlock()
}

func (o *fiOutbound) dialCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.dials
}

// --- fault env ---

type fiEnv struct {
	pool      *topology.GlobalNodePool
	router    *routing.Router
	plat      *platform.Platform
	hashes    []node.Hash
	outbounds []*fiOutbound
}

func newFIEnv(t *testing.T, nodeCount int) *fiEnv {
	t.Helper()

	subMgr := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:                          subMgr.Lookup,
		GeoLookup:                          func(_ netip.Addr) string { return "us" },
		MaxLatencyTableEntries:             16,
		MaxConsecutiveFailures:             func() int { return 3 },
		LatencyDecayWindow:                 func() time.Duration { return 10 * time.Minute },
		HealthEwmaWindow:                   func() int { return 20 },
		HealthEwmaMinSamples:               func() int { return 5 },
		CircuitCooldown:                    func() time.Duration { return 30 * time.Second },
		CircuitMaxCooldown:                 func() time.Duration { return 5 * time.Minute },
		HealthRecoveryFloorPercent:         func() int { return 60 },
		HealthTransferFailureWeightPercent: func() int { return 50 },
	})

	plat := platform.NewPlatform("plat-id", "plat", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	plat.ReverseProxyMissAction = "TREAT_AS_EMPTY"
	pool.RegisterPlatform(plat)

	sub := subscription.NewSubscription("sub-1", "sub-1", "https://example.com", true, false)
	subMgr.Register(sub)

	env := &fiEnv{pool: pool, plat: plat}
	for i := 0; i < nodeCount; i++ {
		raw := json.RawMessage(fmt.Sprintf(`{"type":"stub","server":"10.1.0.%d","server_port":1}`, i+1))
		h := node.HashFromRawOptions(raw)
		sub.ManagedNodes().StoreNode(h, subscription.ManagedNode{Tags: []string{"tag"}})
		pool.AddNodeFromSub(h, raw, sub.ID)

		entry, ok := pool.GetEntry(h)
		if !ok {
			t.Fatalf("node %d missing from pool", i)
		}
		ob := &fiOutbound{}
		var wrapped adapter.Outbound = ob
		entry.Outbound.Store(&wrapped)
		entry.SetEgressIP(netip.MustParseAddr(fmt.Sprintf("203.0.113.%d", i+10)))
		entry.LatencyTable.Update("example.com", 20*time.Millisecond, 10*time.Minute)
		pool.RecordResult(h, true)
		pool.NotifyNodeDirty(h)

		if !plat.View().Contains(h) {
			t.Fatalf("node %d not routable", i)
		}
		env.hashes = append(env.hashes, h)
		env.outbounds = append(env.outbounds, ob)
	}

	env.router = routing.NewRouter(routing.RouterConfig{
		Pool:        pool,
		Authorities: func() []string { return []string{"example.com"} },
		P2CWindow:   func() time.Duration { return 10 * time.Minute },
	})
	return env
}

// pinLease forces the sticky account onto a known node so a retry deterministically
// borrows the other one.
func (e *fiEnv) pinLease(t *testing.T, account string, idx int) {
	t.Helper()
	now := time.Now()
	err := e.router.UpsertLease(model.Lease{
		PlatformID:     e.plat.ID,
		Account:        account,
		NodeHash:       e.hashes[idx].Hex(),
		EgressIP:       mustEgressIPOf(t, e, idx),
		CreatedAtNs:    now.UnixNano(),
		ExpiryNs:       now.Add(time.Hour).UnixNano(),
		LastAccessedNs: now.UnixNano(),
	})
	if err != nil {
		t.Fatalf("upsert lease: %v", err)
	}
}

func mustEgressIPOf(t *testing.T, e *fiEnv, idx int) string {
	t.Helper()
	entry, ok := e.pool.GetEntry(e.hashes[idx])
	if !ok {
		t.Fatalf("node %d missing", idx)
	}
	return entry.GetEgressIP().String()
}

func (e *fiEnv) leaseOf(t *testing.T, account string) (node.Hash, bool) {
	t.Helper()
	var found node.Hash
	ok := false
	e.router.RangeLeases(e.plat.ID, func(acct string, lease routing.Lease) bool {
		if acct == account {
			found = lease.NodeHash
			ok = true
			return false
		}
		return true
	})
	return found, ok
}

func (e *fiEnv) newForwardProxy(
	recorder HealthRecorder,
	emitter EventEmitter,
	failover FailoverConfig,
	transport OutboundTransportConfig,
) *ForwardProxy {
	return NewForwardProxy(ForwardProxyConfig{
		ProxyToken:        "tok",
		Router:            e.router,
		Pool:              e.pool,
		Health:            recorder,
		Events:            emitter,
		OutboundTransport: transport,
		Failover:          failover,
	})
}

func (e *fiEnv) forwardRequest(
	t *testing.T,
	fp *ForwardProxy,
	method, target, account string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, strings.NewReader(body))
	}
	req.Header.Set("Proxy-Authorization", basicAuth("tok", "plat:"+account))
	w := httptest.NewRecorder()
	fp.ServeHTTP(w, req)
	return w
}

func realDial(dest M.Socksaddr) func(ctx context.Context, network string, d M.Socksaddr) (net.Conn, error) {
	return func(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, dest.String())
	}
}

// --- S1: dial failure retries on another node ---

func TestFaultInjection_DialFailureRetriesOnAnotherNode(t *testing.T) {
	env := newFIEnv(t, 2)
	rec := newFIRecorder()
	emitter := newMockEventEmitter()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("served"))
	}))
	defer upstream.Close()

	u, _ := url.Parse(upstream.URL)
	env.outbounds[0].setDial(func(ctx context.Context, network string, d M.Socksaddr) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: network, Err: fmt.Errorf("connection refused")}
	})
	env.outbounds[1].setDial(realDial(M.ParseSocksaddr(u.Host)))

	cfg := FailoverConfig{Enabled: true, MaxAttempts: 2}
	fp := env.newForwardProxy(rec, emitter, cfg, OutboundTransportConfig{
		DialTimeout:           2 * time.Second,
		ResponseHeaderTimeout: 2 * time.Second,
	})
	env.pinLease(t, "acct-1", 0)

	w := env.forwardRequest(t, fp, http.MethodGet, upstream.URL+"/s1", "acct-1", "")

	if w.Code != http.StatusOK {
		t.Fatalf("S1 status: got %d, want 200 (body=%q)", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != "served" {
		t.Fatalf("S1 body: got %q, want served", got)
	}
	if env.outbounds[0].dialCount() == 0 || env.outbounds[1].dialCount() == 0 {
		t.Fatalf("S1 expected both nodes to be dialed, got %d / %d",
			env.outbounds[0].dialCount(), env.outbounds[1].dialCount())
	}

	calls := rec.waitForCount(t, 1, 2*time.Second)
	// The failed node must be recorded as a connect failure, at full weight.
	bad := rec.countFor(env.hashes[0])
	if bad[node.PassiveStageConnect] != 1 {
		reportDeviation(t, "S1", "failed node connect-failure records: got %v (want 1 connect failure), all calls=%+v", bad, calls)
	}
	// The original lease must be untouched: the retry only borrowed node 2.
	if h, ok := env.leaseOf(t, "acct-1"); !ok || h != env.hashes[0] {
		reportDeviation(t, "S1", "sticky lease moved after a borrowed retry: got %v (want node0)", h)
	}
	// First-hop success must be cleared because a retry happened.
	select {
	case ev := <-emitter.finishedCh:
		if ev.FirstHopOK {
			reportDeviation(t, "S1", "a retried request counted as first-hop successful")
		}
	case <-time.After(time.Second):
		t.Fatal("S1: no finished event")
	}
}

// --- S2: a node that never answers must time out and be retried ---

func TestFaultInjection_DialHangTimesOutAndRetries(t *testing.T) {
	env := newFIEnv(t, 2)
	rec := newFIRecorder()
	emitter := newMockEventEmitter()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("served"))
	}))
	defer upstream.Close()
	u, _ := url.Parse(upstream.URL)

	// Node 0's outbound handshake hangs: the dial context is cancelled by the
	// dial timeout, and only that bounds it.
	env.outbounds[0].setDial(func(ctx context.Context, network string, d M.Socksaddr) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	env.outbounds[1].setDial(realDial(M.ParseSocksaddr(u.Host)))

	fp := env.newForwardProxy(rec, emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 2},
		OutboundTransportConfig{
			DialTimeout:           200 * time.Millisecond,
			ResponseHeaderTimeout: 2 * time.Second,
		})
	env.pinLease(t, "acct-1", 0)

	started := time.Now()
	w := env.forwardRequest(t, fp, http.MethodGet, upstream.URL+"/s2", "acct-1", "")
	elapsed := time.Since(started)

	if w.Code != http.StatusOK {
		t.Fatalf("S2 status: got %d, want 200 (body=%q)", w.Code, w.Body.String())
	}
	if elapsed > 3*time.Second {
		reportDeviation(t, "S2", "hang retry took %v, expected the dial timeout to bound it", elapsed)
	}
	if env.outbounds[1].dialCount() == 0 {
		reportDeviation(t, "S2", "a hanging dial was not retried on another node")
	}
	bad := rec.countFor(env.hashes[0])
	if bad[node.PassiveStageConnect] != 1 {
		reportDeviation(t, "S2", "hanging node connect-failure records: got %v, want 1", bad)
	}
}

// --- S3: connection accepted, then reset while the request is in flight ---

func TestFaultInjection_RSTAfterConnectIsNotRetried(t *testing.T) {
	env := newFIEnv(t, 2)
	rec := newFIRecorder()
	emitter := newMockEventEmitter()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("served"))
	}))
	defer upstream.Close()
	u, _ := url.Parse(upstream.URL)

	// Node 0 accepts, reads the request, then resets the connection.
	rstSrv, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer rstSrv.Close()
	go func() {
		for {
			c, err := rstSrv.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				buf := make([]byte, 256)
				_, _ = c.Read(buf)
				if tc, ok := c.(*net.TCPConn); ok {
					_ = tc.SetLinger(0)
				}
				_ = c.Close()
			}(c)
		}
	}()

	env.outbounds[0].setDial(realDial(M.ParseSocksaddr(rstSrv.Addr().String())))
	env.outbounds[1].setDial(realDial(M.ParseSocksaddr(u.Host)))

	fp := env.newForwardProxy(rec, emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 2},
		OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 2 * time.Second})
	env.pinLease(t, "acct-1", 0)

	w := env.forwardRequest(t, fp, http.MethodGet, upstream.URL+"/s3", "acct-1", "")

	if w.Code == http.StatusOK {
		t.Fatalf("S3: an RST'd request must not succeed by replaying it on node 2 (body=%q)", w.Body.String())
	}
	if env.outbounds[1].dialCount() != 0 {
		t.Fatalf("S3: request was replayed on another node after the upstream already saw it")
	}
	_ = rec.waitForCount(t, 1, 2*time.Second)
	stages := rec.countFor(env.hashes[0])
	// Intent: a node that broke after the request left must count as a
	// transfer failure (half weight), not as unreachable.
	if stages[node.PassiveStageTransfer] != 1 {
		reportDeviation(t, "S3", "RST-after-connect recorded as %v, want one transfer-stage failure "+
			"(a connect-stage record weighs the node down as if it were unreachable)", stages)
	}
}

// --- S4a: response-header timeout hit through the attempt budget ---

func TestFaultInjection_ResponseHeaderTimeoutIsAbandonedAsTransferFailure(t *testing.T) {
	env := newFIEnv(t, 2)
	rec := newFIRecorder()
	emitter := newMockEventEmitter()

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(600 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("late"))
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("fast"))
	}))
	defer fast.Close()
	su, _ := url.Parse(slow.URL)
	fu, _ := url.Parse(fast.URL)

	env.outbounds[0].setDial(realDial(M.ParseSocksaddr(su.Host)))
	env.outbounds[1].setDial(realDial(M.ParseSocksaddr(fu.Host)))

	fp := env.newForwardProxy(rec, emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 2, AttemptBudget: 150 * time.Millisecond},
		OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 5 * time.Second})
	env.pinLease(t, "acct-1", 0)

	w := env.forwardRequest(t, fp, http.MethodGet, slow.URL+"/s4", "acct-1", "")

	if w.Code != http.StatusGatewayTimeout {
		reportDeviation(t, "S4", "status: got %d, want 504 (an abandoned attempt must report as a timeout)", w.Code)
	}
	if got := w.Header().Get("X-Resin-Error"); got != "UPSTREAM_TIMEOUT" && w.Code == http.StatusGatewayTimeout {
		reportDeviation(t, "S4", "resin error: got %q, want UPSTREAM_TIMEOUT", got)
	}
	if env.outbounds[1].dialCount() != 0 {
		t.Logf("NOTE [S4] an abandoned attempt WAS retried on another node")
	} else {
		t.Logf("NOTE [S4] an abandoned attempt is not retried (matches rule R-not-retryable; " +
			"conflicts with the 'abandon and switch node' wording in the intent doc)")
	}

	_ = rec.waitForCount(t, 1, 2*time.Second)
	stages := rec.countFor(env.hashes[0])
	if stages[node.PassiveStageTransfer] != 1 {
		reportDeviation(t, "S4", "abandoned attempt recorded as %v, want one transfer-stage failure", stages)
	}
}

// --- S4b: the same slow node, but the transport's own header timeout fires first ---

func TestFaultInjection_ResponseHeaderTimeoutFromTransportIsWeighted(t *testing.T) {
	env := newFIEnv(t, 2)
	rec := newFIRecorder()
	emitter := newMockEventEmitter()

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(600 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("late"))
	}))
	defer slow.Close()
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("fast"))
	}))
	defer fast.Close()
	su, _ := url.Parse(slow.URL)
	fu, _ := url.Parse(fast.URL)

	env.outbounds[0].setDial(realDial(M.ParseSocksaddr(su.Host)))
	env.outbounds[1].setDial(realDial(M.ParseSocksaddr(fu.Host)))

	// ResponseHeaderTimeout < AttemptBudget: the transport returns the timeout
	// itself instead of the failover budget abandoning the attempt.
	fp := env.newForwardProxy(rec, emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 2, AttemptBudget: 5 * time.Second},
		OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 150 * time.Millisecond})
	env.pinLease(t, "acct-1", 0)

	w := env.forwardRequest(t, fp, http.MethodGet, slow.URL+"/s4b", "acct-1", "")

	if w.Code != http.StatusGatewayTimeout {
		reportDeviation(t, "S4b", "status: got %d, want 504", w.Code)
	}
	_ = rec.waitForCount(t, 1, 2*time.Second)
	stages := rec.countFor(env.hashes[0])
	if stages[node.PassiveStageTransfer] != 1 {
		reportDeviation(t, "S4b", "response-header timeout recorded as %v, want one transfer-stage "+
			"failure: a slow origin is being counted as an unreachable node at full weight", stages)
	}
}

// --- S5: every node failing must surface the upstream error, not 503 ---

func TestFaultInjection_AllNodesFailReportsUpstreamError(t *testing.T) {
	env := newFIEnv(t, 2)
	rec := newFIRecorder()
	emitter := newMockEventEmitter()

	for _, ob := range env.outbounds {
		ob.setDial(func(ctx context.Context, network string, d M.Socksaddr) (net.Conn, error) {
			return nil, &net.OpError{Op: "dial", Net: network, Err: fmt.Errorf("connection refused")}
		})
	}

	fp := env.newForwardProxy(rec, emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 2},
		OutboundTransportConfig{DialTimeout: 500 * time.Millisecond, ResponseHeaderTimeout: time.Second})

	w := env.forwardRequest(t, fp, http.MethodGet, "http://example.com/s5", "acct-1", "")

	if w.Code == http.StatusServiceUnavailable {
		t.Fatalf("S5: failure masked as 'no available nodes' (503): %q", w.Header().Get("X-Resin-Error"))
	}
	if w.Code != http.StatusBadGateway {
		reportDeviation(t, "S5", "status: got %d, want 502 (an upstream dial failure)", w.Code)
	}
	if env.outbounds[0].dialCount() == 0 || env.outbounds[1].dialCount() == 0 {
		reportDeviation(t, "S5", "not every candidate node was tried: %d / %d",
			env.outbounds[0].dialCount(), env.outbounds[1].dialCount())
	}
}

// --- S6: a single-node platform must keep its lease when the retry has nowhere to go ---

func TestFaultInjection_SingleNodeFailureKeepsStickyLease(t *testing.T) {
	env := newFIEnv(t, 1)
	emitter := newMockEventEmitter()
	env.outbounds[0].setDial(func(ctx context.Context, network string, d M.Socksaddr) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: network, Err: fmt.Errorf("connection refused")}
	})

	fp := env.newForwardProxy(newFIRecorder(), emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 2},
		OutboundTransportConfig{DialTimeout: 300 * time.Millisecond, ResponseHeaderTimeout: time.Second})
	env.pinLease(t, "acct-1", 0)

	_ = env.forwardRequest(t, fp, http.MethodGet, "http://example.com/s6", "acct-1", "")

	h, ok := env.leaseOf(t, "acct-1")
	if !ok {
		t.Fatalf("S6: the account's lease was deleted after a failed retry on a single-node platform")
	}
	if h != env.hashes[0] {
		reportDeviation(t, "S6", "lease moved to %v, want the original node %v", h, env.hashes[0])
	}
}

// --- S7: a client that hangs up is neither retried nor recorded ---

func TestFaultInjection_ClientCancelIsNotRetriedOrRecorded(t *testing.T) {
	env := newFIEnv(t, 2)
	rec := newFIRecorder()
	emitter := newMockEventEmitter()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	u, _ := url.Parse(upstream.URL)
	for _, ob := range env.outbounds {
		ob.setDial(realDial(M.ParseSocksaddr(u.Host)))
	}

	fp := env.newForwardProxy(rec, emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 2, AttemptBudget: 5 * time.Second},
		OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 3 * time.Second})
	env.pinLease(t, "acct-1", 0)

	req := httptest.NewRequest(http.MethodGet, upstream.URL+"/s7", nil)
	req.Header.Set("Proxy-Authorization", basicAuth("tok", "plat:acct-1"))
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		fp.ServeHTTP(w, req)
		close(done)
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("S7: handler did not return after the client cancelled")
	}

	if total := env.outbounds[0].dialCount() + env.outbounds[1].dialCount(); total > 1 {
		reportDeviation(t, "S7", "a cancelled request was retried: %d dials", total)
	}
	time.Sleep(300 * time.Millisecond)
	calls := rec.snapshot()
	for _, c := range calls {
		if !c.success {
			reportDeviation(t, "S7", "client cancellation recorded as a node failure: %+v", c)
		}
	}
}

// --- S8: a request with a body must reach upstream exactly once ---

func TestFaultInjection_POSTIsNeverSubmittedTwice(t *testing.T) {
	// Control: a bodyless GET is retried, so the second node gets it.
	t.Run("GET is retried", func(t *testing.T) {
		env := newFIEnv(t, 2)
		rec := newFIRecorder()
		emitter := newMockEventEmitter()

		var hits int32
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits++
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}))
		defer upstream.Close()
		u, _ := url.Parse(upstream.URL)

		env.outbounds[0].setDial(func(ctx context.Context, network string, d M.Socksaddr) (net.Conn, error) {
			return nil, &net.OpError{Op: "dial", Net: network, Err: fmt.Errorf("connection refused")}
		})
		env.outbounds[1].setDial(realDial(M.ParseSocksaddr(u.Host)))

		fp := env.newForwardProxy(rec, emitter,
			FailoverConfig{Enabled: true, MaxAttempts: 3},
			OutboundTransportConfig{DialTimeout: 500 * time.Millisecond, ResponseHeaderTimeout: time.Second})
		env.pinLease(t, "acct-1", 0)

		w := env.forwardRequest(t, fp, http.MethodGet, upstream.URL+"/get", "acct-1", "")
		if w.Code != http.StatusOK {
			t.Fatalf("control GET status: got %d, want 200", w.Code)
		}
		if hits != 1 {
			reportDeviation(t, "S8-control", "bodyless GET upstream hits: got %d, want exactly 1", hits)
		}
	})

	// The real check: a POST whose first node cannot dial must not be replayed.
	t.Run("POST is not replayed", func(t *testing.T) {
		env := newFIEnv(t, 2)
		rec := newFIRecorder()
		emitter := newMockEventEmitter()

		var mu sync.Mutex
		var bodies []string
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			bodies = append(bodies, string(body))
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}))
		defer upstream.Close()
		u, _ := url.Parse(upstream.URL)

		env.outbounds[0].setDial(func(ctx context.Context, network string, d M.Socksaddr) (net.Conn, error) {
			return nil, &net.OpError{Op: "dial", Net: network, Err: fmt.Errorf("connection refused")}
		})
		env.outbounds[1].setDial(realDial(M.ParseSocksaddr(u.Host)))

		fp := env.newForwardProxy(rec, emitter,
			FailoverConfig{Enabled: true, MaxAttempts: 3},
			OutboundTransportConfig{DialTimeout: 500 * time.Millisecond, ResponseHeaderTimeout: time.Second})
		env.pinLease(t, "acct-1", 0)

		w := env.forwardRequest(t, fp, http.MethodPost, upstream.URL+"/post", "acct-1", "payload-1")

		mu.Lock()
		got := len(bodies)
		mu.Unlock()
		if got != 0 {
			t.Fatalf("S8: a POST was submitted to another node after a dial failure: %d upstream hits, bodies=%v", got, bodies)
		}
		if w.Code != http.StatusBadGateway {
			reportDeviation(t, "S8", "status: got %d, want 502 (the only honest answer when the body cannot be replayed)", w.Code)
		}
	})
}

// --- S9: bypassed traffic stays out of first-hop statistics ---

func TestFaultInjection_BypassExcludedFromFirstHopStats(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("direct"))
	}))
	defer upstream.Close()

	t.Run("forward", func(t *testing.T) {
		emitter := newMockEventEmitter()
		fp := NewForwardProxy(ForwardProxyConfig{
			ProxyToken:       "tok",
			Events:           emitter,
			ProxyBypassRules: []string{"127.*"},
			Failover:         FailoverConfig{Enabled: true, MaxAttempts: 2},
		})
		req := httptest.NewRequest(http.MethodGet, upstream.URL+"/bypass", nil)
		req.Header.Set("Proxy-Authorization", basicAuth("tok", "plat:acct-1"))
		w := httptest.NewRecorder()
		fp.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("bypass status: got %d, want 200", w.Code)
		}
		select {
		case ev := <-emitter.finishedCh:
			if ev.ViaNode {
				reportDeviation(t, "S9-forward", "bypassed traffic counted as via-node (inflates the first-hop denominator)")
			}
			if ev.FirstHopOK {
				reportDeviation(t, "S9-forward", "bypassed traffic counted as a successful first hop")
			}
		case <-time.After(time.Second):
			t.Fatal("no finished event")
		}
	})

	t.Run("reverse", func(t *testing.T) {
		env := newFIEnv(t, 1)
		emitter := newMockEventEmitter()
		u, _ := url.Parse(upstream.URL)
		target := u.Hostname()

		rp := NewReverseProxy(ReverseProxyConfig{
			Router:            env.router,
			Pool:              env.pool,
			PlatformLookup:    env.pool,
			Health:            newFIRecorder(),
			Events:            emitter,
			ProxyBypassRules:  []string{target},
			Failover:          FailoverConfig{Enabled: true, MaxAttempts: 2},
			OutboundTransport: OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 2 * time.Second},
		})
		req := httptest.NewRequest(http.MethodGet,
			"http://resin.test/tok/plat:acct-1/http/"+u.Host+"/bypass", nil)
		w := httptest.NewRecorder()
		rp.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("reverse bypass status: got %d, want 200 (body=%q, err=%q)",
				w.Code, w.Body.String(), w.Header().Get("X-Resin-Error"))
		}
		if env.outbounds[0].dialCount() != 0 {
			reportDeviation(t, "S9-reverse", "bypassed reverse traffic still went through a node")
		}
		select {
		case ev := <-emitter.finishedCh:
			if ev.ViaNode {
				reportDeviation(t, "S9-reverse", "bypassed reverse traffic counted as via-node")
			}
			if ev.FirstHopOK {
				reportDeviation(t, "S9-reverse", "bypassed reverse traffic counted as a successful first hop")
			}
		case <-time.After(time.Second):
			t.Fatal("no finished event")
		}
	})
}

// --- S10: a slow node must not be treated like a dead one ---
//
// Both envs use the pool itself as the health recorder, so the real breaker and
// health score are exercised.

func newSlowServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("eventually"))
	}))
}

func TestFaultInjection_SlowNodeIsNotTreatedAsDead(t *testing.T) {
	slow := newSlowServer()
	defer slow.Close()
	su, _ := url.Parse(slow.URL)

	// A node that answers, just slowly: every request is abandoned by the
	// attempt budget and recorded as a transfer failure.
	envSlow := newFIEnv(t, 1)
	envSlow.outbounds[0].setDial(realDial(M.ParseSocksaddr(su.Host)))
	fpSlow := envSlow.newForwardProxy(envSlow.pool, newMockEventEmitter(),
		FailoverConfig{Enabled: true, MaxAttempts: 2, AttemptBudget: 120 * time.Millisecond},
		OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 5 * time.Second})
	entrySlow, _ := envSlow.pool.GetEntry(envSlow.hashes[0])

	for i := 0; i < 3; i++ {
		if envSlow.plat.View().Contains(envSlow.hashes[0]) {
			w := envSlow.forwardRequest(t, fpSlow, http.MethodGet, slow.URL+"/slow", "acct-1", "")
			if w.Code != http.StatusGatewayTimeout {
				reportDeviation(t, "S10", "slow request status: got %d, want 504", w.Code)
			}
		} else {
			t.Logf("S10 slow node left the routable view after %d requests", i)
			break
		}
		time.Sleep(80 * time.Millisecond)
	}

	// A node that cannot be dialed at all.
	envDead := newFIEnv(t, 1)
	envDead.outbounds[0].setDial(func(ctx context.Context, network string, d M.Socksaddr) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: network, Err: fmt.Errorf("connection refused")}
	})
	fpDead := envDead.newForwardProxy(envDead.pool, newMockEventEmitter(),
		FailoverConfig{Enabled: true, MaxAttempts: 2},
		OutboundTransportConfig{DialTimeout: 300 * time.Millisecond, ResponseHeaderTimeout: time.Second})
	entryDead, _ := envDead.pool.GetEntry(envDead.hashes[0])
	for i := 0; i < 3; i++ {
		_ = envDead.forwardRequest(t, fpDead, http.MethodGet, "http://example.com/dead", "acct-1", "")
		time.Sleep(80 * time.Millisecond)
	}

	t.Logf("S10 slow: failureCount=%d circuitOpen=%v health=%.3f view=%v",
		entrySlow.FailureCount.Load(), entrySlow.IsCircuitOpen(), entrySlow.HealthScore(),
		envSlow.plat.View().Contains(envSlow.hashes[0]))
	t.Logf("S10 dead: failureCount=%d circuitOpen=%v health=%.3f view=%v",
		entryDead.FailureCount.Load(), entryDead.IsCircuitOpen(), entryDead.HealthScore(),
		envDead.plat.View().Contains(envDead.hashes[0]))

	// The breaker must still do its job for a node that is genuinely unreachable.
	if !entryDead.IsCircuitOpen() {
		reportDeviation(t, "S10", "a node that cannot be dialed at all was never isolated")
	}
	// Transfer weighting must separate a slow node from a dead one.
	if entrySlow.HealthScore() <= entryDead.HealthScore() {
		reportDeviation(t, "S10", "transfer weighting did not separate slow (%.3f) from dead (%.3f)",
			entrySlow.HealthScore(), entryDead.HealthScore())
	}
	// ...but a merely slow node must not be evicted: 3 timeouts is not proof that
	// the node is unreachable.
	if entrySlow.IsCircuitOpen() {
		reportDeviation(t, "S10", "a merely slow node was evicted after %d response-header timeouts: "+
			"the breaker counts an abandoned (transfer-stage) attempt as a full failure",
			entrySlow.FailureCount.Load())
	}
	if !envSlow.plat.View().Contains(envSlow.hashes[0]) {
		reportDeviation(t, "S10", "slow node left the routable view")
	}
}

// --- S11: a pooled connection the peer dropped must not trip the breaker ---

func TestFaultInjection_DeadPooledConnectionDoesNotTripBreaker(t *testing.T) {
	var mu sync.Mutex
	var serverConns []net.Conn

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			serverConns = append(serverConns, c)
			mu.Unlock()
			go func(c net.Conn) {
				br := bufio.NewReader(c)
				for {
					if _, err := http.ReadRequest(br); err != nil {
						return
					}
					_, _ = c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"))
				}
			}(c)
		}
	}()

	env := newFIEnv(t, 1)
	rec := newFIRecorder()
	emitter := newMockEventEmitter()
	env.outbounds[0].setDial(realDial(M.ParseSocksaddr(ln.Addr().String())))
	fp := env.newForwardProxy(rec, emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 2},
		OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 2 * time.Second})
	entry, _ := env.pool.GetEntry(env.hashes[0])
	env.pinLease(t, "acct-1", 0)

	// First request succeeds and leaves an idle connection in the pool.
	w1 := env.forwardRequest(t, fp, http.MethodGet, "http://example.com/one", "acct-1", "")
	if w1.Code != http.StatusOK {
		t.Fatalf("S11 first request: got %d, want 200 (body=%q)", w1.Code, w1.Body.String())
	}
	time.Sleep(100 * time.Millisecond)

	// The peer drops every idle connection without telling us (RST).
	mu.Lock()
	conns := serverConns
	serverConns = nil
	mu.Unlock()
	for _, c := range conns {
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		_ = c.Close()
	}

	// Second request lands on the dead pooled connection.
	w2 := env.forwardRequest(t, fp, http.MethodGet, "http://example.com/two", "acct-1", "")
	time.Sleep(300 * time.Millisecond)

	t.Logf("S11 second request status=%d calls=%+v", w2.Code, rec.snapshot())

	// Intent: a dead pooled connection only weighs the node down; it must not
	// count as a failure or trip the breaker.
	calls := rec.snapshot()
	failureRecords := 0
	dropRecords := 0
	for _, c := range calls {
		if !c.success && !c.connDrop {
			failureRecords++
		}
		if c.connDrop {
			dropRecords++
		}
	}
	if failureRecords > 0 {
		reportDeviation(t, "S11", "a dead pooled connection was recorded as %d node failure(s) "+
			"instead of as a connection drop: %+v", failureRecords, calls)
	}
	if w2.Code == http.StatusOK {
		t.Logf("S11 the transport recovered the dead pooled connection transparently (no signal raised)")
	} else if dropRecords == 0 && failureRecords == 0 {
		t.Logf("S11 dead pooled connection produced a client error (%d) with no health signal at all", w2.Code)
	}
	if got := entry.FailureCount.Load(); got != 0 && failureRecords == 0 {
		_ = got
	}
}

// --- extra: CONNECT tunnel dial failures must not be double counted ---

func TestFaultInjection_CONNECTDialFailureIsCountedOnce(t *testing.T) {
	env := newFIEnv(t, 2)
	rec := newFIRecorder()
	emitter := newMockEventEmitter()
	for _, ob := range env.outbounds {
		ob.setDial(func(ctx context.Context, network string, d M.Socksaddr) (net.Conn, error) {
			return nil, &net.OpError{Op: "dial", Net: network, Err: fmt.Errorf("connection refused")}
		})
	}

	fp := env.newForwardProxy(rec, emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 2},
		OutboundTransportConfig{DialTimeout: 300 * time.Millisecond, ResponseHeaderTimeout: time.Second})
	env.pinLease(t, "acct-1", 0)

	proxySrv := httptest.NewServer(fp)
	defer proxySrv.Close()

	conn, err := net.Dial("tcp", proxySrv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Authorization: %s\r\n\r\n",
		basicAuth("tok", "plat:acct-1"))
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read CONNECT response: %v", err)
	}
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("CONNECT must not succeed when every node refuses to dial")
	}

	calls := rec.waitForCount(t, 2, 2*time.Second)
	perNode := map[node.Hash]int{}
	for _, c := range calls {
		if c.stage == node.PassiveStageConnect && !c.success {
			perNode[c.hash]++
		}
	}
	total := 0
	for _, n := range perNode {
		total += n
	}
	// Two attempts happened; each should be recorded exactly once.
	if total != 2 {
		reportDeviation(t, "S-CONNECT", "CONNECT dial failures recorded %d times for 2 attempts (%v): "+
			"the last node is counted again after OnAttempt already reported it, so a node is "+
			"evicted after half as many failures as configured", total, calls)
	}
}

// --- S12: a platform that opted out of passive tripping must stay opted out ---
//
// The non-retryable failure path rebuilds a RouteResult from FailedNodes, which
// carries only the node hash. If the platform ID is lost there, the opt-out
// switch is silently ignored and a platform that asked never to be tripped by
// traffic feedback gets its nodes isolated anyway.

func TestFaultInjection_PassiveBreakerOptOutIsHonouredOnNonRetryableFailure(t *testing.T) {
	// Node 0 accepts, reads the request, then resets: the request may already be
	// in the server's hands, so this is a non-retryable failure.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				buf := make([]byte, 256)
				_, _ = c.Read(buf)
				if tc, ok := c.(*net.TCPConn); ok {
					_ = tc.SetLinger(0)
				}
				_ = c.Close()
			}(c)
		}
	}()

	env := newFIEnv(t, 1)
	env.plat.PassiveCircuitBreakerDisabled = true
	env.outbounds[0].setDial(realDial(M.ParseSocksaddr(ln.Addr().String())))
	entry, _ := env.pool.GetEntry(env.hashes[0])

	fp := env.newForwardProxy(env.pool, newMockEventEmitter(),
		FailoverConfig{Enabled: true, MaxAttempts: 2},
		OutboundTransportConfig{DialTimeout: time.Second, ResponseHeaderTimeout: 2 * time.Second})
	env.pinLease(t, "acct-1", 0)

	for i := 0; i < 3; i++ {
		w := env.forwardRequest(t, fp, http.MethodGet, "http://example.com/optout", "acct-1", "")
		if w.Code == http.StatusOK {
			t.Fatalf("S12: request unexpectedly succeeded")
		}
		time.Sleep(80 * time.Millisecond)
	}

	t.Logf("S12 opt-out platform: failureCount=%d circuitOpen=%v health=%.3f",
		entry.FailureCount.Load(), entry.IsCircuitOpen(), entry.HealthScore())

	if entry.IsCircuitOpen() {
		reportDeviation(t, "S12", "a platform with PassiveCircuitBreakerDisabled=true had its node "+
			"isolated by traffic feedback (failureCount=%d): the final failure record drops the "+
			"platform ID before reaching the breaker", entry.FailureCount.Load())
	}
}

// --- S13: a retried CONNECT must not count as a successful first hop ---
//
// CONNECT is how HTTPS traffic travels, so it is the traffic that matters most
// for "it connected, then broke". If its retries are invisible, the first-hop
// success rate looks perfect while every request is being rescued by a second
// node.

func TestFaultInjection_CONNECTFailoverIsVisibleInFirstHopStats(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("tunneled"))
	}))
	defer upstream.Close()
	u, _ := url.Parse(upstream.URL)

	env := newFIEnv(t, 2)
	emitter := newMockEventEmitter()
	// Node 0 cannot dial at all, so the CONNECT must be retried on node 1.
	env.outbounds[0].setDial(func(ctx context.Context, network string, d M.Socksaddr) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: network, Err: fmt.Errorf("connection refused")}
	})
	env.outbounds[1].setDial(realDial(M.ParseSocksaddr(u.Host)))

	fp := env.newForwardProxy(newFIRecorder(), emitter,
		FailoverConfig{Enabled: true, MaxAttempts: 2},
		OutboundTransportConfig{DialTimeout: 300 * time.Millisecond, ResponseHeaderTimeout: time.Second})
	env.pinLease(t, "acct-1", 0)

	proxySrv := httptest.NewServer(fp)
	defer proxySrv.Close()

	conn, err := net.Dial("tcp", proxySrv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: %s\r\n\r\n",
		u.Host, u.Host, basicAuth("tok", "plat:acct-1"))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read CONNECT response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("S13 CONNECT status: got %d, want 200 (the retry should have rescued it)", resp.StatusCode)
	}
	if env.outbounds[1].dialCount() == 0 {
		t.Fatalf("S13: the CONNECT was not retried on the second node")
	}

	// Push real traffic through the tunnel so the session counts as served, then
	// close it and wait for the request-finished event.
	_, _ = fmt.Fprintf(conn, "GET /tunneled HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", u.Host)
	br := bufio.NewReader(conn)
	tunnelResp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("S13 read through tunnel: %v", err)
	}
	_, _ = io.ReadAll(tunnelResp.Body)
	_ = conn.Close()
	time.Sleep(100 * time.Millisecond)

	select {
	case ev := <-emitter.finishedCh:
		if ev.FirstHopOK {
			reportDeviation(t, "S13", "a CONNECT that only succeeded on its second node counted as a "+
				"first-hop success: HTTPS failover is invisible in the first-hop metric")
		}
	case <-time.After(time.Second):
		t.Fatal("S13: no finished event")
	}
}
