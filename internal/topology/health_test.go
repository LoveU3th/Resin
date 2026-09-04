package topology

import (
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
)

func newHealthTestPool(maxFailures int) (*GlobalNodePool, *SubscriptionManager) {
	return newHealthTestPoolWithCooldown(maxFailures, 0, 0)
}

// newHealthTestPoolWithCooldown builds a pool whose breaker cooldown can be
// controlled. A cooldown of 0 means "no cooldown", which keeps tests that
// assert immediate recovery fast and deterministic; tests about the cooldown
// itself pass explicit short durations.
func newHealthTestPoolWithCooldown(
	maxFailures int,
	cooldown time.Duration,
	maxCooldown time.Duration,
) (*GlobalNodePool, *SubscriptionManager) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "url", true, false)
	subMgr.Register(sub)

	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(addr netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return maxFailures },
		CircuitCooldown:        func() time.Duration { return cooldown },
		CircuitMaxCooldown:     func() time.Duration { return maxCooldown },
	})
	return pool, subMgr
}

func addTestNode(pool *GlobalNodePool, sub *subscription.Subscription, raw string) node.Hash {
	h := node.HashFromRawOptions([]byte(raw))
	mn := subscription.NewManagedNodes()
	mn.StoreNode(h, subscription.ManagedNode{Tags: []string{"node"}})
	sub.SwapManagedNodes(mn)
	pool.AddNodeFromSub(h, []byte(raw), "s1")
	return h
}

// --- RecordResult tests ---

func TestRecordResult_CircuitBreak(t *testing.T) {
	pool, subMgr := newHealthTestPool(3) // break after 3 failures
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"1"}`)
	entry, _ := pool.GetEntry(h)

	// New node starts circuit-open; mark one success to bring it to healthy state.
	pool.RecordResult(h, true)
	if entry.IsCircuitOpen() {
		t.Fatal("node should recover after first success")
	}

	// 2 failures — not yet broken.
	pool.RecordResult(h, false)
	pool.RecordResult(h, false)
	if entry.IsCircuitOpen() {
		t.Fatal("should not be circuit-open after 2 failures")
	}

	// 3rd failure → circuit opens.
	pool.RecordResult(h, false)
	if !entry.IsCircuitOpen() {
		t.Fatal("should be circuit-open after 3 failures")
	}
	if entry.FailureCount.Load() != 3 {
		t.Fatalf("expected FailureCount=3, got %d", entry.FailureCount.Load())
	}
}

func TestRecordResult_Recovery(t *testing.T) {
	pool, subMgr := newHealthTestPool(2)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"2"}`)

	pool.RecordResult(h, false)
	pool.RecordResult(h, false)
	entry, _ := pool.GetEntry(h)
	if !entry.IsCircuitOpen() {
		t.Fatal("should be circuit open")
	}

	// Success → resets.
	pool.RecordResult(h, true)
	if entry.IsCircuitOpen() {
		t.Fatal("should not be circuit-open after success")
	}
	if entry.FailureCount.Load() != 0 {
		t.Fatalf("expected FailureCount=0, got %d", entry.FailureCount.Load())
	}
}

func TestRecordResult_MaxConsecutiveFailuresPulled(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "url", true, false)
	subMgr.Register(sub)

	var maxFailures atomic.Int64
	maxFailures.Store(3)

	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(addr netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return int(maxFailures.Load()) },
	})

	h := addTestNode(pool, sub, `{"type":"ss","n":"pull-threshold"}`)
	entry, _ := pool.GetEntry(h)
	pool.RecordResult(h, true)
	if entry.IsCircuitOpen() {
		t.Fatal("node should recover after first success")
	}

	pool.RecordResult(h, false)
	if entry.IsCircuitOpen() {
		t.Fatal("should not be circuit-open after first failure")
	}

	// Lower threshold dynamically. Next failure should open circuit.
	maxFailures.Store(2)
	pool.RecordResult(h, false)
	if !entry.IsCircuitOpen() {
		t.Fatal("should be circuit-open after threshold shrinks")
	}
}

func TestRecordResult_DynamicCallback_OnActualChange(t *testing.T) {
	var count atomic.Int32
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              NewSubscriptionManager().Lookup,
		GeoLookup:              func(addr netip.Addr) string { return "" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 5 },
		OnNodeDynamicChanged:   func(hash node.Hash) { count.Add(1) },
	})

	raw := `{"type":"ss","n":"cb"}`
	h := node.HashFromRawOptions([]byte(raw))
	pool.AddNodeFromSub(h, []byte(raw), "s1")

	pool.RecordResult(h, true)
	pool.RecordResult(h, false)
	pool.RecordResult(h, true)

	// New node starts circuit-open: first success recovers, then failure and success mutate again.
	if count.Load() != 3 {
		t.Fatalf("expected 3 callbacks, got %d", count.Load())
	}
}

func TestRecordResult_CircuitBreak_RemovesFromView(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "url", true, false)
	subMgr.Register(sub)

	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(addr netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 2 },
		// No cooldown: this test asserts that the platform view follows the
		// breaker state. Cooldown behaviour is covered separately.
		CircuitCooldown: func() time.Duration { return 0 },
	})
	plat := platform.NewPlatform("p1", "Test", nil, nil)
	pool.RegisterPlatform(plat)

	h := addTestNode(pool, sub, `{"type":"ss","n":"view"}`)

	// Make entry fully routable.
	entry, _ := pool.GetEntry(h)
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	entry.SetEgressIP(netip.MustParseAddr("1.2.3.4"))
	pool.RecordResult(h, true)
	pool.RebuildAllPlatforms()

	if plat.View().Size() != 1 {
		t.Fatal("node should be in view initially")
	}

	// Circuit break → remove from view.
	pool.RecordResult(h, false)
	pool.RecordResult(h, false)
	if plat.View().Size() != 0 {
		t.Fatal("circuit-broken node should be removed from view")
	}

	// Recover → back in view.
	pool.RecordResult(h, true)
	if plat.View().Size() != 1 {
		t.Fatal("recovered node should be back in view")
	}
}

func TestRecordPassiveResult_DisabledPlatformSkipsFailures(t *testing.T) {
	pool, subMgr := newHealthTestPool(2)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"passive-disabled"}`)
	entry, _ := pool.GetEntry(h)
	pool.RecordResult(h, true)

	plat := platform.NewPlatform("p1", "NoPassiveBreaker", nil, nil)
	plat.PassiveCircuitBreakerDisabled = true
	pool.RegisterPlatform(plat)

	pool.RecordPassiveResult(plat.ID, h, false)
	pool.RecordPassiveResult(plat.ID, h, false)
	if got := entry.FailureCount.Load(); got != 0 {
		t.Fatalf("passive failures should be ignored, failure count=%d", got)
	}
	if entry.IsCircuitOpen() {
		t.Fatal("passive failures should not open circuit when disabled")
	}

	pool.RecordResult(h, false)
	pool.RecordResult(h, false)
	if !entry.IsCircuitOpen() {
		t.Fatal("active health feedback should still open circuit")
	}
}

// A platform that opts out of passive circuit breaking must still have its
// traffic failures reflected in the health score. Otherwise such a platform
// only ever sees probe results — and probes report "reachable" for exactly the
// nodes that break on real traffic, so the score would sit at 1.0 forever.
func TestRecordPassiveResult_DisabledPlatformStillFeedsHealthScore(t *testing.T) {
	pool, subMgr := newHealthTestPool(2)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"passive-disabled-health"}`)
	entry, _ := pool.GetEntry(h)
	pool.RecordResult(h, true)

	plat := platform.NewPlatform("p1", "NoPassiveBreaker", nil, nil)
	plat.PassiveCircuitBreakerDisabled = true
	pool.RegisterPlatform(plat)

	before := entry.HealthScore()
	for i := 0; i < 5; i++ {
		pool.RecordPassiveResult(plat.ID, h, false)
	}

	// The breaker must stay untouched...
	if got := entry.FailureCount.Load(); got != 0 {
		t.Fatalf("passive failures must not reach the breaker, failure count=%d", got)
	}
	if entry.IsCircuitOpen() {
		t.Fatal("passive failures must not open circuit when disabled")
	}
	// ...but the health score must have seen them.
	after := entry.HealthScore()
	if after >= before {
		t.Fatalf("health score must drop on passive failures: before=%v after=%v", before, after)
	}
	// One sample from the initial RecordResult above, plus the five failures.
	if got := entry.HealthSamples(); got != 6 {
		t.Fatalf("health samples: got %d, want 6", got)
	}
}

func TestRecordPassiveResult_EnabledPlatformCountsFailures(t *testing.T) {
	pool, subMgr := newHealthTestPool(2)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"passive-enabled"}`)
	entry, _ := pool.GetEntry(h)
	pool.RecordResult(h, true)

	plat := platform.NewPlatform("p1", "PassiveBreaker", nil, nil)
	plat.PassiveCircuitBreakerDisabled = false
	pool.RegisterPlatform(plat)

	pool.RecordPassiveResult(plat.ID, h, false)
	pool.RecordPassiveResult(plat.ID, h, false)
	if got := entry.FailureCount.Load(); got != 2 {
		t.Fatalf("passive failures should be counted, failure count=%d", got)
	}
	if !entry.IsCircuitOpen() {
		t.Fatal("passive failures should open circuit when enabled")
	}
}

// --- RecordLatency tests ---

func TestRecordLatency_NormalizesDomain(t *testing.T) {
	pool, subMgr := newHealthTestPool(5)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"lat"}`)

	// Pass raw target with subdomain+port — should normalize to eTLD+1.
	latency := 100 * time.Millisecond
	pool.RecordLatency(h, "www.example.com:443", &latency)

	entry, _ := pool.GetEntry(h)
	stats, ok := entry.LatencyTable.GetDomainStats("example.com")
	if !ok {
		t.Fatal("should find stats for normalized domain 'example.com'")
	}
	if stats.Ewma != 100*time.Millisecond {
		t.Fatalf("expected 100ms, got %v", stats.Ewma)
	}
}

func TestRecordLatency_FirstRecord_PlatformDirty(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "url", true, false)
	subMgr.Register(sub)

	var latencyCBCount atomic.Int32
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(addr netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnNodeLatencyChanged:   func(hash node.Hash, domain string) { latencyCBCount.Add(1) },
	})

	h := addTestNode(pool, sub, `{"type":"ss","n":"first"}`)

	// First record → wasEmpty=true → platform dirty.
	latency := 50 * time.Millisecond
	pool.RecordLatency(h, "example.com", &latency)
	if latencyCBCount.Load() != 1 {
		t.Fatalf("expected 1 latency callback, got %d", latencyCBCount.Load())
	}
}

func TestRecordLatency_AuthorityResident_RegularBounded(t *testing.T) {
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              NewSubscriptionManager().Lookup,
		GeoLookup:              func(netip.Addr) string { return "" },
		MaxLatencyTableEntries: 1,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyAuthorities:     func() []string { return []string{"gstatic.com"} },
	})

	raw := `{"type":"ss","n":"authority-resident"}`
	h := node.HashFromRawOptions([]byte(raw))
	pool.AddNodeFromSub(h, []byte(raw), "s1")

	l1 := 10 * time.Millisecond
	l2 := 20 * time.Millisecond
	l3 := 30 * time.Millisecond
	pool.RecordLatency(h, "gstatic.com", &l1)
	pool.RecordLatency(h, "a.com", &l2)
	pool.RecordLatency(h, "b.com", &l3)

	entry, _ := pool.GetEntry(h)
	if _, ok := entry.LatencyTable.GetDomainStats("gstatic.com"); !ok {
		t.Fatal("authority domain should remain resident")
	}
	if _, ok := entry.LatencyTable.GetDomainStats("a.com"); ok {
		t.Fatal("oldest regular entry should be evicted at capacity 1")
	}
	if _, ok := entry.LatencyTable.GetDomainStats("b.com"); !ok {
		t.Fatal("latest regular entry should remain")
	}
}

func TestRecordLatency_RegularEviction_EmitsEvictedDomainCallback(t *testing.T) {
	subMgr := NewSubscriptionManager()
	domainCounts := map[string]int{}
	var countsMu sync.Mutex

	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "" },
		MaxLatencyTableEntries: 1,
		MaxConsecutiveFailures: func() int { return 3 },
		OnNodeLatencyChanged: func(_ node.Hash, domain string) {
			countsMu.Lock()
			domainCounts[domain]++
			countsMu.Unlock()
		},
	})

	raw := `{"type":"ss","n":"eviction-callback"}`
	h := node.HashFromRawOptions([]byte(raw))
	pool.AddNodeFromSub(h, []byte(raw), "s1")

	l1 := 10 * time.Millisecond
	l2 := 20 * time.Millisecond
	pool.RecordLatency(h, "a.com", &l1)
	pool.RecordLatency(h, "b.com", &l2)

	countsMu.Lock()
	defer countsMu.Unlock()
	if domainCounts["a.com"] != 2 {
		t.Fatalf("expected a.com callback twice (upsert + eviction), got %d", domainCounts["a.com"])
	}
	if domainCounts["b.com"] != 1 {
		t.Fatalf("expected b.com callback once, got %d", domainCounts["b.com"])
	}
}

func TestRecordLatency_AttemptOnly_UpdatesAttemptTimestamps(t *testing.T) {
	var dynamicCBCount atomic.Int32
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              NewSubscriptionManager().Lookup,
		GeoLookup:              func(netip.Addr) string { return "" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyAuthorities:     func() []string { return []string{"example.com"} },
		OnNodeDynamicChanged:   func(hash node.Hash) { dynamicCBCount.Add(1) },
	})

	raw := `{"type":"ss","n":"attempt-only"}`
	h := node.HashFromRawOptions([]byte(raw))
	pool.AddNodeFromSub(h, []byte(raw), "s1")

	entry, _ := pool.GetEntry(h)
	if entry.LastLatencyProbeAttempt.Load() != 0 || entry.LastAuthorityLatencyProbeAttempt.Load() != 0 {
		t.Fatalf("attempt timestamps should start at 0: %+v", entry)
	}

	pool.RecordLatency(h, "www.example.com:443", nil)

	if entry.LastLatencyProbeAttempt.Load() == 0 {
		t.Fatal("LastLatencyProbeAttempt should be updated")
	}
	if entry.LastAuthorityLatencyProbeAttempt.Load() == 0 {
		t.Fatal("LastAuthorityLatencyProbeAttempt should be updated for authority domain")
	}
	if entry.HasLatency() {
		t.Fatal("attempt-only RecordLatency(nil) must not write latency sample")
	}
	if dynamicCBCount.Load() != 1 {
		t.Fatalf("expected 1 dynamic callback, got %d", dynamicCBCount.Load())
	}
}

// Traffic-path samples must not advance the probe-attempt timestamps.
// ProbeManager decides whether a node is due for a proactive probe from those
// timestamps, so letting user traffic refresh them meant a busy node was never
// due again: the latency shown in the UI went stale and a degrading node kept
// looking healthy.
func TestRecordPassiveLatency_DoesNotUpdateAttemptTimestamps(t *testing.T) {
	var dynamicCBCount atomic.Int32
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              NewSubscriptionManager().Lookup,
		GeoLookup:              func(netip.Addr) string { return "" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyAuthorities:     func() []string { return []string{"example.com"} },
		OnNodeDynamicChanged:   func(hash node.Hash) { dynamicCBCount.Add(1) },
	})

	raw := `{"type":"ss","n":"passive-latency"}`
	h := node.HashFromRawOptions([]byte(raw))
	pool.AddNodeFromSub(h, []byte(raw), "s1")

	entry, _ := pool.GetEntry(h)

	// Nil sample: the common data-path call.
	pool.RecordPassiveLatency(h, "www.example.com:443", nil)

	if entry.LastLatencyProbeAttempt.Load() != 0 {
		t.Fatal("passive sample must not advance LastLatencyProbeAttempt")
	}
	if entry.LastAuthorityLatencyProbeAttempt.Load() != 0 {
		t.Fatal("passive sample must not advance LastAuthorityLatencyProbeAttempt")
	}
	if entry.HasLatency() {
		t.Fatal("nil passive sample must not write a latency sample")
	}
	if dynamicCBCount.Load() != 1 {
		t.Fatalf("expected 1 dynamic callback, got %d", dynamicCBCount.Load())
	}

	// A real sample must still land in the latency table.
	latency := 25 * time.Millisecond
	pool.RecordPassiveLatency(h, "www.example.com:443", &latency)

	if !entry.HasLatency() {
		t.Fatal("passive sample with latency must write a latency sample")
	}
	if entry.LastLatencyProbeAttempt.Load() != 0 {
		t.Fatal("passive sample with latency must not advance LastLatencyProbeAttempt")
	}
}

// --- UpdateNodeEgressIP tests ---

func TestUpdateNodeEgressIP_Change(t *testing.T) {
	var dynamicCount atomic.Int32
	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              NewSubscriptionManager().Lookup,
		GeoLookup:              func(addr netip.Addr) string { return "" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		OnNodeDynamicChanged:   func(hash node.Hash) { dynamicCount.Add(1) },
	})

	raw := `{"type":"ss","n":"egress"}`
	h := node.HashFromRawOptions([]byte(raw))
	pool.AddNodeFromSub(h, []byte(raw), "s1")

	ip1 := netip.MustParseAddr("1.2.3.4")
	pool.UpdateNodeEgressIP(h, &ip1, nil)
	if dynamicCount.Load() != 1 {
		t.Fatalf("expected 1 callback on first IP set, got %d", dynamicCount.Load())
	}

	entry, _ := pool.GetEntry(h)
	if entry.GetEgressIP() != ip1 {
		t.Fatalf("expected %v, got %v", ip1, entry.GetEgressIP())
	}

	// Same IP still updates probe-attempt timestamp.
	pool.UpdateNodeEgressIP(h, &ip1, nil)
	if dynamicCount.Load() != 2 {
		t.Fatalf("expected callback on same IP attempt, got %d", dynamicCount.Load())
	}

	// Different IP → callback.
	ip2 := netip.MustParseAddr("5.6.7.8")
	pool.UpdateNodeEgressIP(h, &ip2, nil)
	if dynamicCount.Load() != 3 {
		t.Fatalf("expected 3 callbacks after IP change, got %d", dynamicCount.Load())
	}
}

func TestUpdateNodeEgressIP_LocStateMachine(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "url", true, false)
	subMgr.Register(sub)

	pool := NewGlobalNodePool(PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(_ netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
	})

	plat := platform.NewPlatform("p1", "JP-Only", nil, []string{"jp"})
	pool.RegisterPlatform(plat)

	h := addTestNode(pool, sub, `{"type":"ss","n":"egress-loc"}`)
	entry, _ := pool.GetEntry(h)
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        30 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	pool.RecordResult(h, true)

	ip := netip.MustParseAddr("1.2.3.4")
	locJP := "jp"
	pool.UpdateNodeEgressIP(h, &ip, &locJP)
	if got := entry.GetEgressRegion(); got != "jp" {
		t.Fatalf("egress region: got %q, want %q", got, "jp")
	}
	if plat.View().Size() != 1 {
		t.Fatal("node should be routable with explicit jp region")
	}

	locUS := "us"
	pool.UpdateNodeEgressIP(h, &ip, &locUS)
	if got := entry.GetEgressRegion(); got != "us" {
		t.Fatalf("egress region: got %q, want %q", got, "us")
	}
	if plat.View().Size() != 0 {
		t.Fatal("same IP but changed region should trigger platform re-evaluation")
	}

	// ip unchanged + loc=nil => keep region.
	pool.UpdateNodeEgressIP(h, &ip, nil)
	if got := entry.GetEgressRegion(); got != "us" {
		t.Fatalf("egress region should keep when ip unchanged and loc=nil: got %q", got)
	}

	// ip=nil + loc=nil => keep both ip and region.
	pool.UpdateNodeEgressIP(h, nil, nil)
	if got := entry.GetEgressRegion(); got != "us" {
		t.Fatalf("egress region should remain unchanged on nil/nil attempt: got %q", got)
	}
	if got := entry.GetEgressIP(); got != ip {
		t.Fatalf("egress IP should remain on attempt-only update: got %v, want %v", got, ip)
	}

	// ip changed + loc=nil => clear region.
	ip2 := netip.MustParseAddr("5.6.7.8")
	pool.UpdateNodeEgressIP(h, &ip2, nil)
	if got := entry.GetEgressRegion(); got != "" {
		t.Fatalf("egress region should clear when ip changed and loc=nil: got %q", got)
	}
	if got := entry.GetEgressIP(); got != ip2 {
		t.Fatalf("egress IP should update on ip change: got %v, want %v", got, ip2)
	}
}

// --- Circuit breaker cooldown ---

// The whole point of the cooldown: a success arriving before it elapses must
// not rejoin routing, otherwise a flapping node oscillates between removed and
// routable many times a minute.
func TestCircuitCooldown_SuccessBeforeCooldownDoesNotCloseBreaker(t *testing.T) {
	pool, subMgr := newHealthTestPoolWithCooldown(2, time.Hour, 0)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"cooldown-open"}`)
	entry, _ := pool.GetEntry(h)

	pool.RecordResult(h, false)
	pool.RecordResult(h, false)
	if !entry.IsCircuitOpen() {
		t.Fatal("precondition: breaker should be open")
	}
	if entry.CircuitState(time.Now()) != node.CircuitOpen {
		t.Fatal("precondition: breaker should still be inside cooldown")
	}

	scoreBeforeSuccess := entry.HealthScore()
	pool.RecordResult(h, true)

	// The cooldown has not elapsed, so the breaker must stay open.
	if !entry.IsCircuitOpen() {
		t.Fatal("breaker must not close before the cooldown elapses")
	}
	// The success is still recorded as health feedback, not thrown away.
	if got := entry.HealthScore(); got <= scoreBeforeSuccess {
		t.Fatalf("success should still feed the health score: before=%v after=%v",
			scoreBeforeSuccess, got)
	}
}

// Once the cooldown has elapsed a success closes the breaker, and the node is
// lifted to a health floor above the filter threshold so it does not get
// filtered straight back out on its stale score.
func TestCircuitCooldown_SuccessAfterCooldownClosesBreaker(t *testing.T) {
	const cooldown = 60 * time.Millisecond
	pool, subMgr := newHealthTestPoolWithCooldown(2, cooldown, 0)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"cooldown-close"}`)
	entry, _ := pool.GetEntry(h)

	// Drive the score down so the recovery floor is observable.
	for i := 0; i < 10; i++ {
		pool.RecordResult(h, false)
	}
	time.Sleep(cooldown * 2)

	if entry.CircuitState(time.Now()) != node.CircuitHalfOpen {
		t.Fatalf("state: got %v, want half-open after the cooldown", entry.CircuitState(time.Now()))
	}

	pool.RecordResult(h, true)

	if entry.IsCircuitOpen() {
		t.Fatal("breaker should close once the cooldown has elapsed")
	}
	// Recovery floor is 60% by default; the closing success counts as one more
	// healthy observation on top of it, so the score sits just above the floor.
	if got := entry.HealthScore(); got < 0.6 || got >= 1 {
		t.Fatalf("health after recovery: got %v, want >= 0.6 and < 1", got)
	}
	if got := entry.CircuitReopenCount.Load(); got != 0 {
		t.Fatalf("reopen count after closing: got %d, want 0", got)
	}
}

// Each failed half-open probe doubles the cooldown, measured from the original
// isolation time rather than the last failure.
func TestCircuitCooldown_HalfOpenFailureBacksOffExponentially(t *testing.T) {
	const cooldown = 50 * time.Millisecond
	pool, subMgr := newHealthTestPoolWithCooldown(2, cooldown, time.Hour)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"backoff"}`)
	entry, _ := pool.GetEntry(h)

	pool.RecordResult(h, false)
	pool.RecordResult(h, false)
	openedAt := entry.CircuitOpenSince.Load()
	if got := time.Duration(entry.CircuitCooldownNs.Load()); got != cooldown {
		t.Fatalf("initial cooldown: got %v, want %v", got, cooldown)
	}

	// Fail one half-open probe: 50ms -> 100ms.
	time.Sleep(cooldown * 2)
	pool.RecordResult(h, false)
	if got := time.Duration(entry.CircuitCooldownNs.Load()); got != 2*cooldown {
		t.Fatalf("cooldown after 1st half-open failure: got %v, want %v", got, 2*cooldown)
	}
	if entry.CircuitOpenSince.Load() != openedAt {
		t.Fatal("open timestamp must stay at the original isolation time")
	}

	// And again: 100ms -> 200ms.
	time.Sleep(2 * cooldown * 2)
	pool.RecordResult(h, false)
	if got := time.Duration(entry.CircuitCooldownNs.Load()); got != 4*cooldown {
		t.Fatalf("cooldown after 2nd half-open failure: got %v, want %v", got, 4*cooldown)
	}
	if got := entry.CircuitReopenCount.Load(); got != 2 {
		t.Fatalf("reopen count: got %d, want 2", got)
	}
}

// The backoff must stop growing at the configured maximum.
func TestCircuitCooldown_BackoffIsCapped(t *testing.T) {
	const (
		cooldown = 20 * time.Millisecond
		max      = 60 * time.Millisecond
	)
	pool, subMgr := newHealthTestPoolWithCooldown(2, cooldown, max)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"backoff-cap"}`)
	entry, _ := pool.GetEntry(h)

	pool.RecordResult(h, false)
	pool.RecordResult(h, false)

	for i := 0; i < 4; i++ {
		time.Sleep(max * 2)
		pool.RecordResult(h, false)
		if got := time.Duration(entry.CircuitCooldownNs.Load()); got > max {
			t.Fatalf("iteration %d: cooldown %v exceeds the max %v", i, got, max)
		}
	}
	if got := time.Duration(entry.CircuitCooldownNs.Load()); got != max {
		t.Fatalf("cooldown should be pinned at the max: got %v, want %v", got, max)
	}
}

// A disabled cooldown must stay disabled and never back off into a positive
// value, no matter how many half-open probes fail.
func TestCircuitCooldown_ZeroCooldownNeverBacksOff(t *testing.T) {
	pool, subMgr := newHealthTestPoolWithCooldown(2, 0, 0)
	sub := subMgr.Lookup("s1")
	h := addTestNode(pool, sub, `{"type":"ss","n":"no-cooldown"}`)
	entry, _ := pool.GetEntry(h)

	pool.RecordResult(h, false)
	pool.RecordResult(h, false)
	for i := 0; i < 3; i++ {
		pool.RecordResult(h, false)
		if got := entry.CircuitCooldownNs.Load(); got != 0 {
			t.Fatalf("iteration %d: cooldown should stay 0, got %v", i, got)
		}
	}

	pool.RecordResult(h, true)
	if entry.IsCircuitOpen() {
		t.Fatal("with no cooldown a success must close the breaker")
	}
}
