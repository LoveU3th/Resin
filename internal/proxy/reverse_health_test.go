package proxy

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// stagedHealthRecorder is a HealthRecorder that records which request phase a
// result came from, so tests can tell a connect failure from a transfer one.
type stagedHealthRecorder struct {
	mockHealthRecorder

	mu      sync.Mutex
	calls   []stagedCall
	notify  chan struct{}
	wantN   int
	doneErr chan struct{}
}

type stagedCall struct {
	platformID string
	hash       node.Hash
	stage      string
	success    bool
}

func newStagedHealthRecorder() *stagedHealthRecorder {
	return &stagedHealthRecorder{
		notify:  make(chan struct{}, 8),
		doneErr: make(chan struct{}, 1),
	}
}

func (m *stagedHealthRecorder) RecordPassiveStageResult(
	platformID string,
	hash node.Hash,
	stage string,
	success bool,
) {
	m.mu.Lock()
	m.calls = append(m.calls, stagedCall{platformID, hash, stage, success})
	m.mu.Unlock()

	select {
	case m.notify <- struct{}{}:
	default:
	}
}

func (m *stagedHealthRecorder) RecordResult(hash node.Hash, success bool) {
	m.mockHealthRecorder.RecordResult(hash, success)
}

func (m *stagedHealthRecorder) RecordPassiveResult(platformID string, hash node.Hash, success bool) {
	m.RecordPassiveStageResult(platformID, hash, node.PassiveStageConnect, success)
}

// waitFor blocks until at least n results have been recorded, or the deadline
// expires. Recording is asynchronous, so tests must not assert immediately.
func (m *stagedHealthRecorder) waitFor(t *testing.T, n int) []stagedCall {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		got := len(m.calls)
		m.mu.Unlock()
		if got >= n {
			return m.snapshot()
		}
		select {
		case <-m.notify:
		case <-time.After(10 * time.Millisecond):
		}
	}
	return m.snapshot()
}

func (m *stagedHealthRecorder) snapshot() []stagedCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]stagedCall, len(m.calls))
	copy(out, m.calls)
	return out
}

func hasStage(calls []stagedCall, stage string, success bool) bool {
	for _, c := range calls {
		if c.stage == stage && c.success == success {
			return true
		}
	}
	return false
}

// A response that breaks partway through the body must be attributed to the
// node. Recording it when the headers arrive would let a node that hands back
// a few bytes and then dies look perfectly healthy.
func TestReverseProxy_BodyTransferFailureIsAttributed(t *testing.T) {
	env := newProxyE2EEnv(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Promise more than we deliver, then drop the connection: the client
		// sees an unexpected EOF partway through the body.
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("first chunk"))
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, err := hj.Hijack()
			if err == nil {
				_ = conn.Close()
			}
		}
	}))
	defer upstream.Close()

	health := newStagedHealthRecorder()
	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         health,
		Events:         NoOpEventEmitter{},
	})

	host := strings.TrimPrefix(upstream.URL, "http://")
	req := httptest.NewRequest(http.MethodGet, "/tok/plat:acct/http/"+host+"/stream", nil)
	w := httptest.NewRecorder()

	func() {
		// Under a real server net/http recovers this; prevents an unhelpful
		// panic from failing the test for the wrong reason.
		defer func() { _ = recover() }()
		rp.ServeHTTP(w, req)
	}()

	calls := health.waitFor(t, 1)
	if !hasStage(calls, node.PassiveStageTransfer, false) {
		t.Fatalf("expected a failed transfer result, got %+v", calls)
	}
	// The failure must not also be reported as a successful connect.
	if hasStage(calls, node.PassiveStageConnect, true) {
		t.Fatalf("headers arriving must not count as success when the body breaks: %+v", calls)
	}
}

// A body delivered in full must still be counted as a success, and only once.
func TestReverseProxy_CompleteBodyIsOneSuccess(t *testing.T) {
	env := newProxyE2EEnv(t)

	body := []byte("reverse-e2e")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Content-Length must match exactly: an over-long promise makes the
		// client see an unexpected EOF even though all bytes arrived.
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer upstream.Close()

	health := newStagedHealthRecorder()
	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         health,
		Events:         NoOpEventEmitter{},
	})

	host := strings.TrimPrefix(upstream.URL, "http://")
	req := httptest.NewRequest(http.MethodGet, "/tok/plat:acct/http/"+host+"/ok", nil)
	w := httptest.NewRecorder()
	rp.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%q)", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != "reverse-e2e" {
		t.Fatalf("body: got %q", got)
	}

	calls := health.waitFor(t, 1)
	var successes int
	for _, c := range calls {
		if c.success {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly 1 success, got %d (%+v)", successes, calls)
	}
}

// A response with no body has nothing left to fail, so it is settled as soon as
// the headers arrive.
//
// What this locks down is that a bodyless response settles exactly once: with
// no body there is no transfer phase, so it must not also settle after
// ServeHTTP. It does not distinguish settling at the headers from settling
// later — for a bodyless response those are the same instant.
func TestReverseProxy_NoBodySettlesAtHeaders(t *testing.T) {
	env := newProxyE2EEnv(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	health := newStagedHealthRecorder()
	rp := NewReverseProxy(ReverseProxyConfig{
		ProxyToken:     "tok",
		Router:         env.router,
		Pool:           env.pool,
		PlatformLookup: env.pool,
		Health:         health,
		Events:         NoOpEventEmitter{},
	})

	host := strings.TrimPrefix(upstream.URL, "http://")
	req := httptest.NewRequest(http.MethodGet, "/tok/plat:acct/http/"+host+"/empty", nil)
	w := httptest.NewRecorder()
	rp.ServeHTTP(w, req)

	calls := health.waitFor(t, 1)
	var successes int
	for _, c := range calls {
		if c.success {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("bodyless response should settle once, got %d (%+v)", successes, calls)
	}
}

// The recorder must satisfy the interfaces the proxy relies on, otherwise the
// staged path is silently skipped.
var (
	_ stagedPassiveHealthRecorder = (*stagedHealthRecorder)(nil)
	_ passiveHealthRecorder       = (*stagedHealthRecorder)(nil)
)

func TestRecordPassiveStageResultAsync_UsesStagedRecorder(t *testing.T) {
	rec := newStagedHealthRecorder()
	route := routing.RouteResult{PlatformID: "p", NodeHash: node.Hash{9}}

	recordPassiveStageResultAsync(rec, route, node.PassiveStageTransfer, false)

	calls := rec.waitFor(t, 1)
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if calls[0].stage != node.PassiveStageTransfer || calls[0].success {
		t.Fatalf("got %+v, want a failed transfer", calls[0])
	}
}
