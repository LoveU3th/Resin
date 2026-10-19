package probe

import (
	"errors"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/topology"
)

// newEgressSchedulePool builds a pool whose subLookup fallback treats every
// node as enabled, so the scheduling tests do not need subscription wiring.
func newEgressSchedulePool(maxConsecutiveFailures int) *topology.GlobalNodePool {
	return topology.NewGlobalNodePool(topology.PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return maxConsecutiveFailures },
	})
}

func addEgressScheduleNode(t *testing.T, pool *topology.GlobalNodePool, raw string) (node.Hash, *node.NodeEntry) {
	t.Helper()
	hash := node.HashFromRawOptions([]byte(raw))
	pool.AddNodeFromSub(hash, []byte(raw), "sub1")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)
	return hash, entry
}

// A node that has never been probed is due immediately, so a freshly added node
// is measured at the first scan instead of waiting out an interval.
func TestEgressProbeDue_NeverAttemptedIsDue(t *testing.T) {
	pool := newEgressSchedulePool(3)
	_, entry := addEgressScheduleNode(t, pool, `{"type":"egress-never"}`)

	mgr := NewProbeManager(ProbeConfig{Pool: pool})
	if !mgr.egressProbeDue(entry, time.Now(), 24*time.Hour, 15*time.Second) {
		t.Fatal("a node with no probe attempt on record must be due")
	}
}

// The regression this guards: a node whose first egress probe failed used to
// wait MaxEgressTestInterval — a day by default — for its next attempt, even
// though without an egress IP it cannot be routed at all. It must instead be
// retried on the short backoff.
func TestEgressProbeDue_UnknownIPRetriesOnShortBackoff(t *testing.T) {
	pool := newEgressSchedulePool(3)
	_, entry := addEgressScheduleNode(t, pool, `{"type":"egress-unknown-ip"}`)
	// Closed breaker: the half-open bypass must not be what makes this due.
	entry.CircuitOpenSince.Store(0)

	now := time.Now()
	entry.LastEgressUpdateAttempt.Store(now.Add(-4 * time.Minute).UnixNano())
	entry.RecordEgressProbeResult(false) // streak 1 -> 3m backoff

	mgr := NewProbeManager(ProbeConfig{Pool: pool})
	if !mgr.egressProbeDue(entry, now, 24*time.Hour, 15*time.Second) {
		t.Fatal("a node with no egress sample must be retried after the short backoff, not after a day")
	}

	// Still inside the backoff window: not due yet.
	fresh := time.Now()
	entry.LastEgressUpdateAttempt.Store(fresh.Add(-2 * time.Minute).UnixNano())
	if mgr.egressProbeDue(entry, fresh, 24*time.Hour, 15*time.Second) {
		t.Fatal("a node inside its backoff window must not be probed")
	}
}

// The backoff must grow with the failure streak, so a node that can never
// sample its egress decays to the refresh interval instead of being probed
// forever.
func TestEgressProbeDue_UnknownIPBackoffGrowsWithStreak(t *testing.T) {
	pool := newEgressSchedulePool(3)
	_, entry := addEgressScheduleNode(t, pool, `{"type":"egress-backoff"}`)
	entry.CircuitOpenSince.Store(0)

	mgr := NewProbeManager(ProbeConfig{Pool: pool})
	now := time.Now()

	// streak 3 -> 12m backoff.
	entry.RecordEgressProbeResult(false)
	entry.RecordEgressProbeResult(false)
	entry.RecordEgressProbeResult(false)

	entry.LastEgressUpdateAttempt.Store(now.Add(-11 * time.Minute).UnixNano())
	if mgr.egressProbeDue(entry, now, 24*time.Hour, 15*time.Second) {
		t.Fatal("streak 3 must not be due at 11m (backoff is 12m)")
	}
	entry.LastEgressUpdateAttempt.Store(now.Add(-13 * time.Minute).UnixNano())
	if !mgr.egressProbeDue(entry, now, 24*time.Hour, 15*time.Second) {
		t.Fatal("streak 3 must be due at 13m (backoff is 12m)")
	}

	// A large streak saturates at the configured refresh interval.
	for i := 0; i < 40; i++ {
		entry.RecordEgressProbeResult(false)
	}
	entry.LastEgressUpdateAttempt.Store(now.Add(-23 * time.Hour).UnixNano())
	if mgr.egressProbeDue(entry, now, 24*time.Hour, 15*time.Second) {
		t.Fatal("a saturated streak must not be due before the refresh interval")
	}
}

// A node that already has an egress sample stays on the refresh interval: the
// aggressive retry exists for nodes that cannot be routed, not for every node.
func TestEgressProbeDue_KnownIPStaysOnRefreshInterval(t *testing.T) {
	pool := newEgressSchedulePool(3)
	_, entry := addEgressScheduleNode(t, pool, `{"type":"egress-known-ip"}`)
	entry.CircuitOpenSince.Store(0)
	entry.LastEgressUpdate.Store(time.Now().Add(-time.Hour).UnixNano())

	now := time.Now()
	entry.LastEgressUpdateAttempt.Store(now.Add(-4 * time.Minute).UnixNano())

	mgr := NewProbeManager(ProbeConfig{Pool: pool})
	if mgr.egressProbeDue(entry, now, 24*time.Hour, 15*time.Second) {
		t.Fatal("a node with an egress sample must not be retried after minutes")
	}

	entry.LastEgressUpdateAttempt.Store(now.Add(-25 * time.Hour).UnixNano())
	if !mgr.egressProbeDue(entry, now, 24*time.Hour, 15*time.Second) {
		t.Fatal("a node with an egress sample must be due after the refresh interval")
	}
}

// A half-open node is waiting for a probe to decide whether it can rejoin
// routing, and rejoining needs an egress IP. Without the bypass the recovery
// probe would be gated behind MaxEgressTestInterval and the cooldown would be
// unobservable — the same reasoning the latency scan already applies.
func TestEgressProbeDue_HalfOpenNodeBypassesIntervalGate(t *testing.T) {
	pool := newEgressSchedulePool(3)
	_, entry := addEgressScheduleNode(t, pool, `{"type":"egress-half-open"}`)
	// Cooldown 5m, opened 10m ago: elapsed, so the node is half-open.
	entry.CircuitOpenSince.Store(time.Now().Add(-10 * time.Minute).UnixNano())
	entry.CircuitCooldownNs.Store(int64(5 * time.Minute))

	now := time.Now()
	entry.LastEgressUpdateAttempt.Store(now.Add(-time.Minute).UnixNano())

	mgr := NewProbeManager(ProbeConfig{Pool: pool})
	// Spacing is cooldown/4 = 75s, so one minute is still too soon.
	if mgr.egressProbeDue(entry, now, 24*time.Hour, 15*time.Second) {
		t.Fatal("a half-open node inside its probe spacing must not be probed")
	}
	entry.LastEgressUpdateAttempt.Store(now.Add(-2 * time.Minute).UnixNano())
	if !mgr.egressProbeDue(entry, now, 24*time.Hour, 15*time.Second) {
		t.Fatal("a half-open node past its probe spacing must be probed despite the refresh interval")
	}
}

func TestEgressUnknownRetryInterval(t *testing.T) {
	const maxInterval = 24 * time.Hour
	cases := []struct {
		streak int32
		want   time.Duration
	}{
		{0, 3 * time.Minute},
		{1, 3 * time.Minute},
		{2, 6 * time.Minute},
		{3, 12 * time.Minute},
		{4, 24 * time.Minute},
		{10, maxInterval}, // 3m << 9 = 1536m, capped
	}
	for _, tc := range cases {
		if got := egressUnknownRetryInterval(tc.streak, maxInterval); got != tc.want {
			t.Fatalf("streak %d: got %v, want %v", tc.streak, got, tc.want)
		}
	}
	// A configured interval below the base clamps the backoff to it.
	if got := egressUnknownRetryInterval(1, time.Minute); got != time.Minute {
		t.Fatalf("with maxInterval=1m: got %v, want 1m", got)
	}
}

// End to end through the scan: a node whose egress probe failed four minutes
// ago must be probed again even though MaxEgressTestInterval is a day.
func TestScanEgress_RetriesNodeWithoutEgressIP(t *testing.T) {
	pool := newEgressSchedulePool(3)
	_, entry := addEgressScheduleNode(t, pool, `{"type":"egress-scan-retry"}`)
	entry.CircuitOpenSince.Store(0)

	now := time.Now()
	entry.LastEgressUpdateAttempt.Store(now.Add(-4 * time.Minute).UnixNano())
	entry.RecordEgressProbeResult(false)

	var calls atomic.Int32
	mgr := NewProbeManager(ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		MaxEgressTestInterval: func() time.Duration {
			return 24 * time.Hour
		},
		Fetcher: func(_ node.Hash, _ string) ([]byte, time.Duration, error) {
			calls.Add(1)
			return []byte("ip=203.0.113.7\nloc=US"), 10 * time.Millisecond, nil
		},
	})
	mgr.Start()
	defer mgr.Stop()

	mgr.scanEgress()

	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() == 0 {
		t.Fatal("scanEgress did not retry a node with no egress sample")
	}
	if got := entry.GetEgressIP(); got != netip.MustParseAddr("203.0.113.7") {
		t.Fatalf("egress IP: got %v, want 203.0.113.7", got)
	}
}

// The counterpart: a node with a fresh attempt on record and no sample is still
// inside its backoff, so the scan must leave it alone.
func TestScanEgress_DoesNotRetryInsideBackoff(t *testing.T) {
	pool := newEgressSchedulePool(3)
	_, entry := addEgressScheduleNode(t, pool, `{"type":"egress-scan-wait"}`)
	entry.CircuitOpenSince.Store(0)

	entry.LastEgressUpdateAttempt.Store(time.Now().Add(-time.Minute).UnixNano())
	entry.RecordEgressProbeResult(false)

	var calls atomic.Int32
	mgr := NewProbeManager(ProbeConfig{
		Pool:        pool,
		Concurrency: 1,
		MaxEgressTestInterval: func() time.Duration {
			return 24 * time.Hour
		},
		Fetcher: func(_ node.Hash, _ string) ([]byte, time.Duration, error) {
			calls.Add(1)
			return []byte("ip=203.0.113.8"), 10 * time.Millisecond, nil
		},
	})
	mgr.Start()
	defer mgr.Stop()

	mgr.scanEgress()
	time.Sleep(100 * time.Millisecond)

	if got := calls.Load(); got != 0 {
		t.Fatalf("a node inside its backoff must not be probed, calls=%d", got)
	}
}

// A probe that got as far as seeing the target answer says the node proxied a
// connection. That is evidence about the target, not the node: it must not feed
// the breaker, only the health score.
//
// The node starts circuit-open (that is how the pool admits new nodes), so a
// successful probe runs first to close it — otherwise "the breaker stayed
// closed" would be vacuous.
func TestProbeEgress_TransferFailureSkipsBreaker(t *testing.T) {
	pool := newEgressSchedulePool(1) // any single breaker-fed failure would open it
	hash, entry := addEgressScheduleNode(t, pool, `{"type":"egress-transfer"}`)

	var fetchErr error
	var fetchLatency time.Duration
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ node.Hash, _ string) ([]byte, time.Duration, error) {
			if fetchErr != nil {
				return nil, fetchLatency, fetchErr
			}
			return []byte("ip=203.0.113.1\nloc=US"), 10 * time.Millisecond, nil
		},
	})

	mgr.probeEgress(hash, entry)
	if entry.CircuitOpenSince.Load() != 0 {
		t.Fatal("precondition: the warm-up probe should have closed the breaker")
	}

	// The target answered through the node, then the exchange failed.
	fetchErr = errors.New("outbound fetch: unexpected status 502")
	fetchLatency = 42 * time.Millisecond
	mgr.probeEgress(hash, entry)

	if entry.FailureCount.Load() != 0 {
		t.Fatalf("a transfer-phase probe failure must not feed the breaker, got %d failures",
			entry.FailureCount.Load())
	}
	if entry.CircuitOpenSince.Load() != 0 {
		t.Fatal("a transfer-phase probe failure must not open the breaker")
	}
	if entry.HealthScore() >= 1 {
		t.Fatalf("a transfer-phase probe failure must still lower the health score, got %v",
			entry.HealthScore())
	}
}

// The contrast: a fetch that never saw the target answer is a connect-phase
// failure and must feed the breaker at full weight.
func TestProbeEgress_ConnectFailureFeedsBreaker(t *testing.T) {
	pool := newEgressSchedulePool(1)
	hash, entry := addEgressScheduleNode(t, pool, `{"type":"egress-connect"}`)

	var fetchErr error
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ node.Hash, _ string) ([]byte, time.Duration, error) {
			if fetchErr != nil {
				return nil, 0, fetchErr
			}
			return []byte("ip=203.0.113.1\nloc=US"), 10 * time.Millisecond, nil
		},
	})

	mgr.probeEgress(hash, entry)
	if entry.CircuitOpenSince.Load() != 0 {
		t.Fatal("precondition: the warm-up probe should have closed the breaker")
	}

	fetchErr = errors.New("connection refused")
	mgr.probeEgress(hash, entry)

	if entry.FailureCount.Load() != 1 {
		t.Fatalf("a connect-phase probe failure must feed the breaker, got %d failures",
			entry.FailureCount.Load())
	}
	if entry.CircuitOpenSince.Load() == 0 {
		t.Fatal("a connect-phase probe failure must open the breaker at threshold 1")
	}
}

// The latency probe attributes failures the same way.
func TestProbeLatency_TransferFailureSkipsBreaker(t *testing.T) {
	pool := newEgressSchedulePool(1)
	hash, entry := addEgressScheduleNode(t, pool, `{"type":"latency-transfer"}`)

	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ node.Hash, _ string) ([]byte, time.Duration, error) {
			return nil, 30 * time.Millisecond, errors.New("read body: unexpected EOF")
		},
	})

	mgr.probeLatency(hash, entry, "https://www.gstatic.com/generate_204")

	if entry.FailureCount.Load() != 0 {
		t.Fatalf("a transfer-phase latency failure must not feed the breaker, got %d",
			entry.FailureCount.Load())
	}
	if entry.HealthScore() >= 1 {
		t.Fatalf("a transfer-phase latency failure must lower the health score, got %v",
			entry.HealthScore())
	}
}

// A success resets the backoff streak, so a node that recovers starts from the
// base interval again instead of staying pinned at a long one.
func TestProbeEgress_SuccessResetsFailStreak(t *testing.T) {
	pool := newEgressSchedulePool(3)
	hash, entry := addEgressScheduleNode(t, pool, `{"type":"egress-streak-reset"}`)

	fail := true
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ node.Hash, _ string) ([]byte, time.Duration, error) {
			if fail {
				return nil, 0, errors.New("connection refused")
			}
			return []byte("ip=203.0.113.9\nloc=US"), 10 * time.Millisecond, nil
		},
	})

	mgr.probeEgress(hash, entry)
	mgr.probeEgress(hash, entry)
	if streak := entry.EgressFailStreak(); streak != 2 {
		t.Fatalf("streak after two failures: got %d, want 2", streak)
	}

	fail = false
	mgr.probeEgress(hash, entry)
	if streak := entry.EgressFailStreak(); streak != 0 {
		t.Fatalf("streak after a success: got %d, want 0", streak)
	}
}

// probeFailureStage is the classifier both probes use, so pin its boundary:
// only a fetch that observed the target answering counts as transfer.
func TestProbeFailureStage(t *testing.T) {
	if got := probeFailureStage(0); got != node.PassiveStageConnect {
		t.Fatalf("zero latency: got %q, want connect", got)
	}
	if got := probeFailureStage(time.Nanosecond); got != node.PassiveStageTransfer {
		t.Fatalf("positive latency: got %q, want transfer", got)
	}
}
