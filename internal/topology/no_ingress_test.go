package topology

import (
	"testing"
	"time"
)

// One zero-ingress tunnel is not enough evidence: a target that is simply down
// makes any node look like this. Two in a row park the node.
func TestRecordNoIngressFailure_ParksOnlyAfterConsecutiveFailures(t *testing.T) {
	pool, subMgr := newHealthTestPool(3)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"no-ingress"}`)
	entry, _ := pool.GetEntry(h)

	// New nodes start circuit-open; a success closes the breaker so the test
	// can tell quarantine apart from the breaker.
	pool.RecordResult(h, true)
	if entry.IsCircuitOpen() {
		t.Fatal("precondition: node should be routable after a success")
	}

	pool.RecordNoIngressFailure("plat", h)
	if entry.NoIngressQuarantined(time.Now()) {
		t.Fatal("a single zero-ingress tunnel must not park the node")
	}

	pool.RecordNoIngressFailure("plat", h)
	if !entry.NoIngressQuarantined(time.Now()) {
		t.Fatalf("expected the node to be parked after %d consecutive failures",
			noIngressQuarantineThreshold)
	}
	// The breaker is a separate mechanism with its own persistence; parking a
	// node must not pretend it was evicted.
	if entry.IsCircuitOpen() {
		t.Fatal("zero-ingress quarantine must not open the circuit breaker")
	}
	if entry.FailureCount.Load() != 0 {
		t.Fatalf("quarantine must not touch the failure count, got %d", entry.FailureCount.Load())
	}
}

// A single success clears both the streak and the quarantine: the node just
// served traffic, which is stronger evidence than two tunnels that returned
// nothing.
func TestRecordNoIngressFailure_AnySuccessClearsQuarantine(t *testing.T) {
	pool, subMgr := newHealthTestPool(3)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"no-ingress-clear"}`)
	entry, _ := pool.GetEntry(h)

	pool.RecordResult(h, true)
	for i := 0; i < noIngressQuarantineThreshold; i++ {
		pool.RecordNoIngressFailure("plat", h)
	}
	if !entry.NoIngressQuarantined(time.Now()) {
		t.Fatal("precondition: node should be parked")
	}

	pool.RecordResult(h, true)
	if entry.NoIngressQuarantined(time.Now()) {
		t.Fatal("a success must clear the quarantine")
	}
	if entry.NoIngressStreak.Load() != 0 {
		t.Fatalf("a success must reset the streak, got %d", entry.NoIngressStreak.Load())
	}
}

// The quarantine expires on its own; it is a pause, not an eviction.
func TestNoIngressQuarantine_Expires(t *testing.T) {
	pool, subMgr := newHealthTestPool(3)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"no-ingress-expire"}`)
	entry, _ := pool.GetEntry(h)

	entry.NoIngressQuarantineUntil.Store(time.Now().Add(-time.Second).UnixNano())
	if entry.NoIngressQuarantined(time.Now()) {
		t.Fatal("an elapsed quarantine must no longer apply")
	}
	entry.NoIngressQuarantineUntil.Store(time.Now().Add(time.Minute).UnixNano())
	if !entry.NoIngressQuarantined(time.Now()) {
		t.Fatal("an active quarantine must apply")
	}
}
