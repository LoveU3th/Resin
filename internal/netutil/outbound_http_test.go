package netutil

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/testutil"
)

func TestHTTPGetViaOutbound_RequireStatus2xx_RejectsNonSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	_, _, err = HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatus2xx: true,
	})
	if err == nil {
		t.Fatal("expected non-2xx status to return error")
	}
	if !strings.Contains(err.Error(), "unexpected status 404") {
		t.Fatalf("expected status error, got: %v", err)
	}
}

// The latency probe target is gstatic.com/generate_204, which answers 204.
// Accepting only exactly 200 would make every latency probe fail.
func TestHTTPGetViaOutbound_RequireStatus2xx_Accepts204(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	body, latency, err := HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatus2xx: true,
	})
	if err != nil {
		t.Fatalf("expected 204 to count as success, got: %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("expected empty body, got %q", string(body))
	}
	if latency <= 0 {
		t.Fatalf("expected positive TTFB, got %v", latency)
	}
}

// A 3xx/5xx must not count as healthy: a node that completes handshakes but
// cannot serve traffic is exactly the failure mode probing has to catch.
func TestHTTPGetViaOutbound_RequireStatus2xx_Rejects502(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("bad gateway"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	_, _, err = HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatus2xx: true,
	})
	if err == nil {
		t.Fatal("expected 502 to return error")
	}
}

func TestHTTPGetViaOutbound_AllowAnyStatusWhenNotRequired(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("probe-body"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	body, _, err := HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{})
	if err != nil {
		t.Fatalf("expected non-2xx response to pass through, got: %v", err)
	}
	if string(body) != "probe-body" {
		t.Fatalf("unexpected body %q", string(body))
	}
}

// TTFB must cover the whole request, not just the TLS handshake: a node that
// shakes hands fast and then stalls used to look healthy.
//
// The handler sleeps before responding so the assertion has a lower bound.
// Asserting only "latency > 0" would still pass if the implementation regressed
// to measuring connection setup.
func TestHTTPGetViaOutbound_ReportsTimeToFirstByte(t *testing.T) {
	const serverDelay = 150 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(serverDelay)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	_, latency, err := HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatus2xx: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if latency < serverDelay {
		t.Fatalf("TTFB = %v, want at least the %v server delay", latency, serverDelay)
	}
}

func TestConnCloseHook_CloseIsIdempotentAndConcurrentSafe(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	var onCloseCount atomic.Int32
	hook := &connCloseHook{
		Conn: client,
		onClose: func() {
			onCloseCount.Add(1)
		},
	}

	const closers = 32
	var wg sync.WaitGroup
	wg.Add(closers)
	for i := 0; i < closers; i++ {
		go func() {
			defer wg.Done()
			_ = hook.Close()
		}()
	}
	wg.Wait()

	if got := onCloseCount.Load(); got != 1 {
		t.Fatalf("onClose called %d times, want 1", got)
	}
}
