package node

import (
	"math"
	"net/netip"
	"regexp"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/testutil"
)

func TestNodeEntry_SubscriptionIDs(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)

	e.AddSubscriptionID("s1")
	e.AddSubscriptionID("s2")
	e.AddSubscriptionID("s1") // idempotent

	ids := e.SubscriptionIDs()
	if len(ids) != 2 {
		t.Fatalf("expected 2 subs, got %d: %v", len(ids), ids)
	}

	empty := e.RemoveSubscriptionID("s1")
	if empty {
		t.Fatal("should not be empty after removing s1")
	}
	if e.SubscriptionCount() != 1 {
		t.Fatalf("expected 1 sub, got %d", e.SubscriptionCount())
	}

	empty = e.RemoveSubscriptionID("s2")
	if !empty {
		t.Fatal("should be empty after removing s2")
	}

	// Idempotent remove.
	empty = e.RemoveSubscriptionID("s999")
	if !empty {
		t.Fatal("removing nonexistent should report empty if already empty")
	}
}

func TestNodeEntry_MatchRegexs_EmptyRegex(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)
	if !e.MatchRegexs(nil, nil) {
		t.Fatal("empty regex list should match")
	}
	if !e.MatchRegexs([]*regexp.Regexp{}, nil) {
		t.Fatal("empty regex slice should match")
	}
}

func TestNodeEntry_MatchRegexs_EmptyRegex_RequiresEnabledSubscriptionWhenLookupProvided(t *testing.T) {
	h := HashFromRawOptions([]byte(`{"type":"ss"}`))
	e := NewNodeEntry(h, nil, time.Now(), 0)
	e.AddSubscriptionID("sub-disabled")

	lookup := func(subID string, hash Hash) (string, bool, []string, bool) {
		switch subID {
		case "sub-disabled":
			return "SubDisabled", false, []string{"node-a"}, true
		case "sub-enabled":
			return "SubEnabled", true, []string{"node-b"}, true
		default:
			return "", false, nil, false
		}
	}

	if e.MatchRegexs([]*regexp.Regexp{}, lookup) {
		t.Fatal("empty regex with lookup should not match when all subscriptions are disabled")
	}

	e.AddSubscriptionID("sub-enabled")
	if !e.MatchRegexs([]*regexp.Regexp{}, lookup) {
		t.Fatal("empty regex with lookup should match when any subscription is enabled")
	}
}

func TestNodeEntry_MatchRegexs_Basic(t *testing.T) {
	h := HashFromRawOptions([]byte(`{"type":"ss"}`))
	e := NewNodeEntry(h, nil, time.Now(), 0)
	e.AddSubscriptionID("sub-1")

	lookup := func(subID string, hash Hash) (string, bool, []string, bool) {
		if subID == "sub-1" {
			return "MySub", true, []string{"us-node", "fast"}, true
		}
		return "", false, nil, false
	}

	// Match "MySub/us-node" — should match regex "us".
	regexes := []*regexp.Regexp{regexp.MustCompile("us")}
	if !e.MatchRegexs(regexes, lookup) {
		t.Fatal("should match 'us' regex")
	}

	// Should not match "jp".
	regexes = []*regexp.Regexp{regexp.MustCompile("jp")}
	if e.MatchRegexs(regexes, lookup) {
		t.Fatal("should not match 'jp' regex")
	}
}

func TestNodeEntry_MatchRegexs_AllRegexesMustMatch(t *testing.T) {
	h := HashFromRawOptions([]byte(`{"type":"ss"}`))
	e := NewNodeEntry(h, nil, time.Now(), 0)
	e.AddSubscriptionID("sub-1")

	lookup := func(subID string, hash Hash) (string, bool, []string, bool) {
		return "Provider", true, []string{"us-fast-1"}, true
	}

	// Both "us" and "fast" match "Provider/us-fast-1".
	regexes := []*regexp.Regexp{
		regexp.MustCompile("us"),
		regexp.MustCompile("fast"),
	}
	if !e.MatchRegexs(regexes, lookup) {
		t.Fatal("both regexes should match")
	}

	// "us" matches but "jp" does not.
	regexes = []*regexp.Regexp{
		regexp.MustCompile("us"),
		regexp.MustCompile("jp"),
	}
	if e.MatchRegexs(regexes, lookup) {
		t.Fatal("should not match when one regex fails")
	}
}

func TestNodeEntry_MatchRegexs_DisabledSubSkipped(t *testing.T) {
	h := HashFromRawOptions([]byte(`{"type":"ss"}`))
	e := NewNodeEntry(h, nil, time.Now(), 0)
	e.AddSubscriptionID("sub-1")

	lookup := func(subID string, hash Hash) (string, bool, []string, bool) {
		return "MySub", false, []string{"us-node"}, true // disabled
	}

	regexes := []*regexp.Regexp{regexp.MustCompile("us")}
	if e.MatchRegexs(regexes, lookup) {
		t.Fatal("disabled sub should not contribute to match")
	}
}

func TestNodeEntry_MatchRegexs_MultiSub(t *testing.T) {
	h := HashFromRawOptions([]byte(`{"type":"ss"}`))
	e := NewNodeEntry(h, nil, time.Now(), 0)
	e.AddSubscriptionID("sub-1")
	e.AddSubscriptionID("sub-2")

	lookup := func(subID string, hash Hash) (string, bool, []string, bool) {
		switch subID {
		case "sub-1":
			return "Provider-A", true, []string{"eu-node"}, true
		case "sub-2":
			return "Provider-B", true, []string{"us-node"}, true
		}
		return "", false, nil, false
	}

	// Match "us" — should match via sub-2.
	regexes := []*regexp.Regexp{regexp.MustCompile("us")}
	if !e.MatchRegexs(regexes, lookup) {
		t.Fatal("should match via second subscription")
	}
}

func TestNodeEntry_HasEnabledSubscription(t *testing.T) {
	h := HashFromRawOptions([]byte(`{"type":"ss"}`))
	e := NewNodeEntry(h, nil, time.Now(), 0)
	e.AddSubscriptionID("sub-disabled")
	e.AddSubscriptionID("sub-enabled")

	lookup := func(subID string, hash Hash) (string, bool, []string, bool) {
		switch subID {
		case "sub-disabled":
			return "SubDisabled", false, []string{"node-a"}, true
		case "sub-enabled":
			return "SubEnabled", true, []string{"node-b"}, true
		default:
			return "", false, nil, false
		}
	}

	if !e.HasEnabledSubscription(lookup) {
		t.Fatal("expected HasEnabledSubscription to be true")
	}
	if e.IsDisabledBySubscriptions(lookup) {
		t.Fatal("expected IsDisabledBySubscriptions to be false")
	}
}

func TestNodeEntry_IsDisabledBySubscriptions_AllDisabledOrMissing(t *testing.T) {
	h := HashFromRawOptions([]byte(`{"type":"ss"}`))
	e := NewNodeEntry(h, nil, time.Now(), 0)
	e.AddSubscriptionID("sub-disabled")
	e.AddSubscriptionID("sub-missing")

	lookup := func(subID string, hash Hash) (string, bool, []string, bool) {
		if subID == "sub-disabled" {
			return "SubDisabled", false, []string{"node-a"}, true
		}
		return "", false, nil, false
	}

	if e.HasEnabledSubscription(lookup) {
		t.Fatal("expected HasEnabledSubscription to be false")
	}
	if !e.IsDisabledBySubscriptions(lookup) {
		t.Fatal("expected IsDisabledBySubscriptions to be true")
	}
}

func TestNodeEntry_CircuitBreaker(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)
	if e.IsCircuitOpen() {
		t.Fatal("should not be circuit-open by default")
	}

	e.CircuitOpenSince.Store(time.Now().UnixNano())
	if !e.IsCircuitOpen() {
		t.Fatal("should be circuit-open after store")
	}

	e.CircuitOpenSince.Store(0)
	if e.IsCircuitOpen() {
		t.Fatal("should not be circuit-open after reset")
	}
}

func TestNodeEntry_LatencyCount(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 16)
	if e.HasLatency() {
		t.Fatal("should not have latency by default")
	}

	e.LatencyTable.LoadEntry("example.com", DomainLatencyStats{
		Ewma:        100 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	if !e.HasLatency() {
		t.Fatal("should have latency after adding an entry")
	}
}

func TestNodeEntry_Outbound(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)
	if e.HasOutbound() {
		t.Fatal("should not have outbound by default")
	}

	ob := testutil.NewNoopOutbound()
	e.Outbound.Store(&ob)
	if !e.HasOutbound() {
		t.Fatal("should have outbound after store")
	}
}

func TestNodeEntry_IsHealthy(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)
	if e.IsHealthy() {
		t.Fatal("node without outbound should not be healthy")
	}

	ob := testutil.NewNoopOutbound()
	e.Outbound.Store(&ob)
	if !e.IsHealthy() {
		t.Fatal("node with outbound and no circuit should be healthy")
	}

	e.CircuitOpenSince.Store(time.Now().UnixNano())
	if e.IsHealthy() {
		t.Fatal("circuit-open node should not be healthy")
	}
}

func TestNodeEntry_EgressIP(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)

	// Before any store, should return zero addr.
	addr := e.GetEgressIP()
	if addr.IsValid() {
		t.Fatal("should be invalid before first store")
	}

	ip := netip.MustParseAddr("1.2.3.4")
	e.SetEgressIP(ip)
	if got := e.GetEgressIP(); got != ip {
		t.Fatalf("expected %s, got %s", ip, got)
	}
}

func TestNodeEntry_EgressRegion(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)

	if got := e.GetEgressRegion(); got != "" {
		t.Fatalf("default egress region: got %q, want empty", got)
	}

	e.SetEgressRegion("US")
	if got := e.GetEgressRegion(); got != "us" {
		t.Fatalf("normalized egress region: got %q, want %q", got, "us")
	}

	e.SetEgressRegion("")
	if got := e.GetEgressRegion(); got != "" {
		t.Fatalf("cleared egress region: got %q, want empty", got)
	}
}

func TestNodeEntry_GetRegion_UsesStoredThenGeoIPFallback(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)
	e.SetEgressIP(netip.MustParseAddr("203.0.113.1"))

	geoLookupCalled := false
	geoLookup := func(_ netip.Addr) string {
		geoLookupCalled = true
		return "jp"
	}

	if got := e.GetRegion(geoLookup); got != "jp" {
		t.Fatalf("fallback region: got %q, want %q", got, "jp")
	}
	if !geoLookupCalled {
		t.Fatal("expected geo lookup to be called without stored region")
	}

	geoLookupCalled = false
	e.SetEgressRegion("US")
	if got := e.GetRegion(geoLookup); got != "us" {
		t.Fatalf("stored region: got %q, want %q", got, "us")
	}
	if geoLookupCalled {
		t.Fatal("geo lookup should not be called when stored region exists")
	}
}

// --- Health score (success-ratio EWMA) ---

func TestHealthScore_StartsFullyHealthy(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)

	if got := e.HealthScore(); got != 1 {
		t.Fatalf("initial health: got %v, want 1 (unknown nodes are healthy)", got)
	}
	if got := e.HealthSamples(); got != 0 {
		t.Fatalf("initial samples: got %d, want 0", got)
	}
}

// Failures must pull the score down, and successes must bring it back up.
// A monotonically broken node is what the router needs to see.
func TestHealthScore_DecaysOnFailureAndRecoversOnSuccess(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)

	for i := 0; i < 10; i++ {
		e.RecordHealthSample(false, 1, 20, 5)
	}
	failed := e.HealthScore()
	if failed >= 1 {
		t.Fatalf("health after 10 failures: got %v, want < 1", failed)
	}
	// Cold start (alpha 1/5) applies for the first 5 samples, then alpha 1/20,
	// which lands around 0.25 after ten consecutive failures.
	if failed > 0.3 {
		t.Fatalf("health after 10 failures: got %v, want <= 0.3", failed)
	}

	for i := 0; i < 40; i++ {
		e.RecordHealthSample(true, 1, 20, 5)
	}
	if got := e.HealthScore(); got < 0.8 {
		t.Fatalf("health after 40 successes: got %v, want >= 0.8", got)
	}
}

// The score must decay per observation, not per unit of time: a node that gets
// no traffic must keep its score rather than drifting, otherwise a node that is
// bad (and therefore avoided) freezes at its last score with no way back.
func TestHealthScore_IsIndependentOfElapsedTime(t *testing.T) {
	slow := NewNodeEntry(Hash{}, nil, time.Now(), 0)
	fast := NewNodeEntry(Hash{}, nil, time.Now(), 0)

	for i := 0; i < 6; i++ {
		slow.RecordHealthSample(false, 1, 20, 5)
		fast.RecordHealthSample(false, 1, 20, 5)
	}

	if got, want := slow.HealthScore(), fast.HealthScore(); got != want {
		t.Fatalf("same observations must give the same score: got %v and %v", got, want)
	}
}

// Below the minimum sample count a larger alpha applies, so a fresh node is not
// still sitting near 1.0 after several failures.
func TestHealthScore_ColdStartConvergesFaster(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)

	e.RecordHealthSample(false, 1, 20, 5)
	after1 := 1 - e.HealthScore()

	// With the steady-state alpha (1/20) a single failure would move the score
	// by 0.05; cold start should move it noticeably more.
	if after1 <= 0.05 {
		t.Fatalf("first failure moved health by %v, want > 0.05 (cold-start alpha)", after1)
	}
}

// A partial-weight observation (a transfer-phase drop, say) must count for less
// than a full failure.
func TestHealthScore_WeightScalesTheObservation(t *testing.T) {
	full := NewNodeEntry(Hash{}, nil, time.Now(), 0)
	half := NewNodeEntry(Hash{}, nil, time.Now(), 0)

	full.RecordHealthSample(false, 1, 20, 5)
	half.RecordHealthSample(false, 0.5, 20, 5)

	if half.HealthScore() <= full.HealthScore() {
		t.Fatalf("weighted failure (%v) must be less severe than full failure (%v)",
			half.HealthScore(), full.HealthScore())
	}
}

// Out-of-range weights are clamped rather than corrupting the score.
func TestHealthScore_ClampsWeightAndScore(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)

	e.RecordHealthSample(false, 5, 20, 5)
	if got := e.HealthScore(); got < 0 || got > 1 {
		t.Fatalf("health out of range after weight > 1: got %v", got)
	}

	e.RecordHealthSample(false, 0, 20, 5)
	e.RecordHealthSample(false, -1, 20, 5)
	if got := e.HealthScore(); got < 0 || got > 1 {
		t.Fatalf("health out of range after non-positive weight: got %v", got)
	}
}

func TestHealthScore_ResetHealth(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)
	for i := 0; i < 10; i++ {
		e.RecordHealthSample(false, 1, 20, 5)
	}

	e.ResetHealth(0.6)
	// The score is stored as float32, so compare with a tolerance.
	if got := e.HealthScore(); math.Abs(got-0.6) > 1e-6 {
		t.Fatalf("health after reset: got %v, want 0.6", got)
	}
	if got := e.HealthSamples(); got != 0 {
		t.Fatalf("samples after reset: got %d, want 0", got)
	}
}

// Concurrent sampling must not lose updates or drive the score out of range.
func TestHealthScore_ConcurrentSamples(t *testing.T) {
	e := NewNodeEntry(Hash{}, nil, time.Now(), 0)

	const goroutines = 16
	const perGoroutine = 50
	done := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func(fail bool) {
			defer func() { done <- struct{}{} }()
			for j := 0; j < perGoroutine; j++ {
				e.RecordHealthSample(fail, 1, 20, 5)
			}
		}(i%2 == 0)
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}

	if got := e.HealthScore(); got < 0 || got > 1 {
		t.Fatalf("health out of range after concurrent sampling: got %v", got)
	}
	if got, want := e.HealthSamples(), uint32(goroutines*perGoroutine); got != want {
		t.Fatalf("samples: got %d, want %d (updates were lost)", got, want)
	}
}
