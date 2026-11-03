package proxy

import (
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

type noIngressStub struct {
	mu     sync.Mutex
	calls  []noIngressCall
	notify chan struct{}
}

type noIngressCall struct {
	platformID string
	hash       node.Hash
}

func newNoIngressStub() *noIngressStub {
	return &noIngressStub{notify: make(chan struct{}, 1)}
}

func (s *noIngressStub) RecordResult(node.Hash, bool)                           {}
func (s *noIngressStub) RecordLatency(node.Hash, string, *time.Duration)        {}
func (s *noIngressStub) RecordPassiveLatency(node.Hash, string, *time.Duration) {}

func (s *noIngressStub) RecordNoIngressFailure(platformID string, hash node.Hash) {
	s.mu.Lock()
	s.calls = append(s.calls, noIngressCall{platformID: platformID, hash: hash})
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *noIngressStub) waitFor(t *testing.T) noIngressCall {
	t.Helper()
	select {
	case <-s.notify:
	case <-time.After(time.Second):
		t.Fatal("expected a zero-ingress report")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[len(s.calls)-1]
}

// The zero-ingress signal has to reach the recorder with the route it belongs
// to, otherwise the quarantine would be applied to the wrong node.
func TestRecordNoIngressFailureAsync_ReportsRoute(t *testing.T) {
	stub := newNoIngressStub()
	route := routing.RouteResult{
		PlatformID: "plat-1",
		NodeHash:   node.Hash{0x0a, 0x0b},
	}

	recordNoIngressFailureAsync(stub, route)

	call := stub.waitFor(t)
	if call.platformID != route.PlatformID || call.hash != route.NodeHash {
		t.Fatalf("reported (%q,%v), want (%q,%v)",
			call.platformID, call.hash, route.PlatformID, route.NodeHash)
	}
}

// Recorders that do not implement the optional interface must be left alone:
// the ordinary failure has already been recorded for them.
type plainRecorder struct{}

func (plainRecorder) RecordResult(node.Hash, bool)                    {}
func (plainRecorder) RecordLatency(node.Hash, string, *time.Duration) {}
func (plainRecorder) RecordPassiveLatency(node.Hash, string, *time.Duration) {
}

func TestRecordNoIngressFailureAsync_IgnoresRecordersWithoutSupport(t *testing.T) {
	// Must not panic and must not fall back to an ordinary failure: that one is
	// already reported by the caller.
	recordNoIngressFailureAsync(plainRecorder{}, routing.RouteResult{NodeHash: node.Hash{0x01}})
	recordNoIngressFailureAsync(nil, routing.RouteResult{NodeHash: node.Hash{0x02}})
}
