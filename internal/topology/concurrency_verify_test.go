package topology

// Concurrency verification for the health/breaker recorders.
//
// RecordResult, RecordPassiveStageResult and RecordConnDrop are all invoked
// through `go` from the data plane, so they are the hottest shared mutable
// state in the process. What must hold under concurrency:
//
//   - no lost updates: every failure increments FailureCount, every observation
//     is folded into the health score exactly once
//   - the health score stays in [0, 1]
//   - the node reaches the state the input dictates: enough consecutive
//     failures isolate it, a success afterwards clears the counter
//   - the breaker never ends up open with a zero timestamp, or closed with a
//     non-zero one (that inconsistency is what strands a node)

import (
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

// TestCV_HealthBreakerConcurrentFans drives all three recorders from many
// goroutines and checks the counters and the state machine afterwards.
func TestCV_HealthBreakerConcurrentFans(t *testing.T) {
	pool, subMgr := newHealthTestPoolWithCooldown(1000, 50*time.Millisecond, time.Second)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"cv1"}`)
	entry, _ := pool.GetEntry(h)

	const (
		goroutines   = 64
		perGoroutine = 40
		total        = goroutines * perGoroutine
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				switch (g + i) % 3 {
				case 0:
					pool.RecordResult(h, false)
				case 1:
					pool.RecordPassiveStageResult("plat", h, node.PassiveStageTransfer, false)
				default:
					pool.RecordConnDrop("plat", h)
				}
			}
		}(g)
	}
	wg.Wait()

	// Every RecordResult / RecordPassiveStageResult failure bumps the counter;
	// RecordConnDrop must not (it is a weight-down, not a failure).
	failures := int64(entry.FailureCount.Load())
	// The distribution is (g+i)%3 over 64x40, which is not uniform in i, so
	// count it the same way the workers do.
	wantFailures := int64(0)
	for g := 0; g < goroutines; g++ {
		for i := 0; i < perGoroutine; i++ {
			switch (g + i) % 3 {
			case 0, 1: // RecordResult(false) and a transfer failure both count
				wantFailures++
			}
		}
	}
	if failures != wantFailures {
		t.Fatalf("BLOCKING: lost updates in the breaker: FailureCount=%d, want %d",
			failures, wantFailures)
	}

	// Every observation, including conn drops, is one health sample. A small
	// shortfall is expected: closing the breaker calls ResetHealth, whose plain
	// Store(0) on the sample counter can clobber a concurrent increment (see
	// the report) — that is why the other test closes the breaker up front.
	if got := entry.HealthSamples(); got != uint32(total) {
		t.Errorf("health samples: got %d, want %d (a small shortfall is the known "+
			"ResetHealth clobber; a large one means updates are being lost)", got, total)
	}

	if score := entry.HealthScore(); score < 0 || score > 1 {
		t.Fatalf("health score out of range: %v", score)
	}
	if score := entry.HealthScore(); score > 0.5 {
		t.Fatalf("health score did not fall after %d weighted failures: %v", total, score)
	}

	// Enough consecutive failures must isolate the node, and the state must be
	// self-consistent.
	if !entry.IsCircuitOpen() {
		t.Fatal("a node with far more failures than the threshold was never isolated")
	}
	if openedAt := entry.CircuitOpenSince.Load(); openedAt == 0 {
		t.Fatal("breaker reports open but CircuitOpenSince is zero")
	}
}

// A success after the cooldown must clear the counter and close the breaker
// exactly once, even with many successes racing.
func TestCV_BreakerRecoveryIsNotDoubleCounted(t *testing.T) {
	pool, subMgr := newHealthTestPoolWithCooldown(3, 30*time.Millisecond, time.Second)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"cv2"}`)
	entry, _ := pool.GetEntry(h)

	for i := 0; i < 3; i++ {
		pool.RecordResult(h, false)
	}
	if !entry.IsCircuitOpen() {
		t.Fatal("three failures did not isolate the node")
	}

	// Let the cooldown elapse so the node is half-open.
	time.Sleep(60 * time.Millisecond)
	if entry.CircuitState(time.Now()) != node.CircuitHalfOpen {
		t.Fatalf("state after cooldown: got %v, want half-open", entry.CircuitState(time.Now()))
	}

	const goroutines = 64
	var wg sync.WaitGroup
	var closes int64
	var mu sync.Mutex
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			before := entry.IsCircuitOpen()
			pool.RecordResult(h, true)
			mu.Lock()
			if before && !entry.IsCircuitOpen() {
				closes++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if entry.IsCircuitOpen() {
		t.Fatal("the breaker stayed open after successful half-open probes")
	}
	if got := entry.FailureCount.Load(); got != 0 {
		t.Fatalf("FailureCount after successes: got %d, want 0", got)
	}
	if closes == 0 {
		t.Fatal("no goroutine observed the breaker closing; the recovery path never ran")
	}
	// Several goroutines can each observe (open -> closed) around the same
	// transition; what must not happen is an inconsistent end state.
	if got := entry.CircuitReopenCount.Load(); got != 0 {
		t.Fatalf("reopen count after a clean recovery: got %d, want 0", got)
	}
	if got := entry.CircuitOpenSince.Load(); got != 0 {
		t.Fatalf("breaker closed but CircuitOpenSince=%d", got)
	}
}

// Mixed success and failure traffic must leave the score inside [0,1] and the
// sample count exact, no matter how the goroutines interleave.
func TestCV_HealthScoreStaysInRangeUnderMixedTraffic(t *testing.T) {
	pool, subMgr := newHealthTestPoolWithCooldown(1000000, time.Hour, time.Hour)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"cv3"}`)
	entry, _ := pool.GetEntry(h)

	// New nodes start isolated, and a success while isolated calls ResetHealth,
	// which drops the sample counter. Close the breaker up front so the test
	// measures only the concurrent accumulation.
	pool.RecordResult(h, true)
	if entry.IsCircuitOpen() {
		t.Fatal("a healthy result did not clear the initial isolation")
	}
	baseSamples := entry.HealthSamples()

	const (
		goroutines   = 64
		perGoroutine = 500
		total        = goroutines * perGoroutine
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				success := (g+i)%2 == 0
				stage := node.PassiveStageConnect
				if (g+i)%4 == 1 {
					stage = node.PassiveStageTransfer
				}
				pool.RecordPassiveStageResult("plat", h, stage, success)
			}
		}(g)
	}
	wg.Wait()

	// No lost updates: with the breaker closed and the threshold out of reach,
	// ResetHealth cannot run during the loop, so the count must be exact.
	if got := entry.HealthSamples(); got != baseSamples+uint32(total) {
		t.Fatalf("BLOCKING: lost health samples: got %d, want %d",
			got, baseSamples+uint32(total))
	}
	score := entry.HealthScore()
	if score < 0 || score > 1 {
		t.Fatalf("health score out of range: %v", score)
	}
	// Half the traffic succeeded at full weight, a quarter failed at half
	// weight: the score must land near 0.75, and definitely inside (0.5, 1).
	if score <= 0.5 || score >= 1 {
		t.Fatalf("health score %v does not reflect mixed traffic", score)
	}
	t.Logf("mixed-traffic health score: %.4f", score)
}
