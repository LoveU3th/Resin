package routing

import (
	"errors"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

// newUnhealthyEntry returns a routable node whose health score has been driven
// down by the given number of consecutive failures.
func newUnhealthyEntry(t *testing.T, raw, ip string, failures int) (node.Hash, *node.NodeEntry) {
	t.Helper()
	h, e := newRoutableEntry(t, raw, ip)
	for i := 0; i < failures; i++ {
		e.RecordHealthSample(false, 1, 20, 5)
	}
	return h, e
}

func healthTestWeights() HealthWeights {
	return HealthWeights{
		PenaltyNs:              2000 * float64(time.Millisecond),
		FilterThresholdPercent: 40,
		MinSamplesForFilter:    8,
	}
}

// A healthy node must score exactly as it did before health existed, so an
// all-healthy fleet sees no behaviour change at all.
func TestCalculateScore_HealthyNodeIsNotPenalized(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat", "Plat", nil, nil)
	pool.addPlatform(plat)

	h, entry := newRoutableEntry(t, `{"id":"healthy"}`, "198.51.100.1")
	pool.addEntry(h, entry)

	stats := NewIPLoadStats()
	latency := 100 * time.Millisecond

	without := calculateScore(h, latency, plat, stats, pool, HealthWeights{})
	with := calculateScore(h, latency, plat, stats, pool, healthTestWeights())

	if without != with {
		t.Fatalf("healthy node penalized: got %v without health, %v with", without, with)
	}
	if with != float64(latency) {
		t.Fatalf("score: got %v, want %v", with, float64(latency))
	}
}

// An unhealthy node must be penalized by exactly (1 - health) * penalty.
func TestCalculateScore_UnhealthyNodeIsPenalizedProportionally(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat", "Plat", nil, nil)
	pool.addPlatform(plat)

	h, entry := newUnhealthyEntry(t, `{"id":"unhealthy"}`, "198.51.100.2", 10)
	pool.addEntry(h, entry)

	stats := NewIPLoadStats()
	latency := 100 * time.Millisecond
	penaltyNs := 2000 * float64(time.Millisecond)

	base := calculateScore(h, latency, plat, stats, pool, HealthWeights{})
	got := calculateScore(h, latency, plat, stats, pool, HealthWeights{PenaltyNs: penaltyNs})

	if entry.HealthScore() >= 1 {
		t.Fatalf("precondition: node should be unhealthy, got %v", entry.HealthScore())
	}
	want := base + (1-entry.HealthScore())*penaltyNs
	if diff := got - want; diff > 1e-6 || diff < -1e-6 {
		t.Fatalf("penalty: got %v, want %v", got, want)
	}
}

// The penalty must work under every allocation policy, including
// PreferIdleIP where the score is a lease count rather than a duration.
func TestCalculateScore_PenaltyAppliesUnderEveryPolicy(t *testing.T) {
	pool := newRouterTestPool()
	stats := NewIPLoadStats()
	penaltyNs := 2000 * float64(time.Millisecond)

	policies := []platform.AllocationPolicy{
		platform.AllocationPolicyPreferLowLatency,
		platform.AllocationPolicyPreferIdleIP,
		platform.AllocationPolicyBalanced,
	}
	for _, policy := range policies {
		t.Run(string(policy), func(t *testing.T) {
			plat := platform.NewPlatform("plat", "Plat", nil, nil)
			plat.AllocationPolicy = policy
			pool.addPlatform(plat)

			h, entry := newUnhealthyEntry(t, `{"id":"unhealthy-`+string(policy)+`"}`, "198.51.100.3", 10)
			pool.addEntry(h, entry)

			base := calculateScore(h, 100*time.Millisecond, plat, stats, pool, HealthWeights{})
			got := calculateScore(h, 100*time.Millisecond, plat, stats, pool, HealthWeights{PenaltyNs: penaltyNs})
			if got <= base {
				t.Fatalf("policy %s: unhealthy node not penalized (%v vs %v)", policy, got, base)
			}
		})
	}
}

// The filter's decision itself is deterministic, so assert it directly rather
// than inferring it from random picks.
func TestHealthWeights_Allows(t *testing.T) {
	pool := newRouterTestPool()

	badH, badEntry := newUnhealthyEntry(t, `{"id":"bad"}`, "198.51.100.10", 20)
	pool.addEntry(badH, badEntry)
	fewH, fewEntry := newUnhealthyEntry(t, `{"id":"few"}`, "198.51.100.11", 3)
	pool.addEntry(fewH, fewEntry)
	goodH, goodEntry := newRoutableEntry(t, `{"id":"good"}`, "198.51.100.12")
	pool.addEntry(goodH, goodEntry)

	weights := healthTestWeights()

	if badEntry.HealthScore()*100 >= float64(weights.FilterThresholdPercent) {
		t.Fatalf("precondition: bad node health %v should be below %d%%",
			badEntry.HealthScore(), weights.FilterThresholdPercent)
	}
	if weights.allows(pool, badH) {
		t.Fatal("a node below the health threshold with enough samples must be filtered")
	}
	if !weights.allows(pool, goodH) {
		t.Fatal("a healthy node must always be allowed")
	}
	// Too few observations to trust the score.
	if fewEntry.HealthSamples() >= uint32(weights.MinSamplesForFilter) {
		t.Fatalf("precondition: node should have < %d samples", weights.MinSamplesForFilter)
	}
	if !weights.allows(pool, fewH) {
		t.Fatalf("a node with %d samples must not be filtered (min %d)",
			fewEntry.HealthSamples(), weights.MinSamplesForFilter)
	}
	// Unknown nodes are not filtered.
	if !weights.allows(pool, node.Hash{0xde, 0xad}) {
		t.Fatal("an unknown node must not be filtered")
	}
}

// End to end, an unhealthy node should lose traffic rather than keep getting
// picked. P2C is randomized, so this asserts a strong bias rather than
// excluding the bad node outright.
func TestRandomRoute_PrefersHealthyNodes(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat", "Plat", nil, nil)
	pool.addPlatform(plat)

	badH, badEntry := newUnhealthyEntry(t, `{"id":"bad"}`, "198.51.100.20", 20)
	pool.addEntry(badH, badEntry)
	for i := 0; i < 5; i++ {
		h, e := newRoutableEntry(t, `{"id":"good-`+string(rune('a'+i))+`"}`, "198.51.100.3"+string(rune('0'+i)))
		pool.addEntry(h, e)
	}
	pool.rebuildPlatformView(plat)

	stats := NewIPLoadStats()
	const iterations = 200
	var badPicks int
	for i := 0; i < iterations; i++ {
		got, err := randomRoute(plat, stats, pool, "example.com", nil, 10*time.Minute, healthTestWeights(), nil)
		if err != nil {
			t.Fatalf("randomRoute: %v", err)
		}
		if got == badH {
			badPicks++
		}
	}
	// With 5 healthy nodes, filtering plus the score penalty should keep the
	// bad node well under its 1-in-6 random share.
	if badPicks > iterations/20 {
		t.Fatalf("unhealthy node picked %d/%d times, want <= %d", badPicks, iterations, iterations/20)
	}
}

// Filtering is best-effort: when every candidate is unhealthy, routing must
// still return a node rather than failing the request.
func TestRandomRoute_FallsBackWhenAllCandidatesUnhealthy(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat", "Plat", nil, nil)
	pool.addPlatform(plat)

	for i := 0; i < 2; i++ {
		h, e := newUnhealthyEntry(t, `{"id":"allbad-`+string(rune('a'+i))+`"}`, "198.51.100.4"+string(rune('0'+i)), 20)
		pool.addEntry(h, e)
	}
	pool.rebuildPlatformView(plat)

	stats := NewIPLoadStats()
	got, err := randomRoute(plat, stats, pool, "example.com", nil, 10*time.Minute, healthTestWeights(), nil)
	if err != nil {
		t.Fatalf("routing must not fail when all nodes are unhealthy: %v", err)
	}
	if got == node.Zero {
		t.Fatal("routing returned the zero hash")
	}
}

// A zero HealthWeights must disable health entirely, preserving the previous
// behaviour for anyone who does not configure it.
func TestHealthWeights_ZeroValueDisablesHealth(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat", "Plat", nil, nil)
	pool.addPlatform(plat)

	h, entry := newUnhealthyEntry(t, `{"id":"zero-weights"}`, "198.51.100.50", 20)
	pool.addEntry(h, entry)
	pool.rebuildPlatformView(plat)

	var zero HealthWeights
	if !zero.allows(pool, h) {
		t.Fatal("zero HealthWeights must not filter anything")
	}

	stats := NewIPLoadStats()
	base := calculateScore(h, 100*time.Millisecond, plat, stats, pool, HealthWeights{})
	withZero := calculateScore(h, 100*time.Millisecond, plat, stats, pool, zero)
	if base != withZero {
		t.Fatalf("zero HealthWeights must not change scores: %v vs %v", base, withZero)
	}
}

// Scores are built from time.Duration and therefore in nanoseconds, while the
// configuration is in milliseconds. Getting this conversion wrong would make
// the penalty three orders of magnitude too small to matter.
func TestRouter_HealthWeightsConvertsMillisecondsToNanoseconds(t *testing.T) {
	r := NewRouter(RouterConfig{
		HealthPenaltyMs:              func() int { return 2000 },
		HealthFilterThresholdPercent: func() int { return 40 },
		HealthMinSamplesForFilter:    func() int { return 8 },
	})

	w := r.healthWeights()
	if want := 2000 * float64(time.Millisecond); w.PenaltyNs != want {
		t.Fatalf("PenaltyNs: got %v, want %v", w.PenaltyNs, want)
	}
	if w.FilterThresholdPercent != 40 {
		t.Fatalf("FilterThresholdPercent: got %d, want 40", w.FilterThresholdPercent)
	}
	if w.MinSamplesForFilter != 8 {
		t.Fatalf("MinSamplesForFilter: got %d, want 8", w.MinSamplesForFilter)
	}
}

// Missing accessors must yield a zero-value HealthWeights, which disables
// health instead of panicking.
func TestRouter_HealthWeightsNilAccessorsDisableHealth(t *testing.T) {
	r := NewRouter(RouterConfig{})

	w := r.healthWeights()
	if w != (HealthWeights{}) {
		t.Fatalf("expected zero HealthWeights, got %+v", w)
	}
}

// Excluded nodes must never be handed out, so a request that already failed on a
// node gets a different one.
func TestRandomRoute_ExcludesGivenNodes(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat", "Plat", nil, nil)
	pool.addPlatform(plat)

	var hashes []node.Hash
	for i := 0; i < 4; i++ {
		h, e := newRoutableEntry(t, `{"id":"excl-`+string(rune('a'+i))+`"}`, "198.51.100.2"+string(rune('0'+i)))
		pool.addEntry(h, e)
		hashes = append(hashes, h)
	}
	pool.rebuildPlatformView(plat)

	stats := NewIPLoadStats()
	exclude := hashes[:2]
	for i := 0; i < 50; i++ {
		got, err := randomRoute(plat, stats, pool, "example.com", nil, 10*time.Minute, HealthWeights{}, exclude)
		if err != nil {
			t.Fatalf("randomRoute: %v", err)
		}
		if got == exclude[0] || got == exclude[1] {
			t.Fatalf("iteration %d: returned excluded node %v", i, got)
		}
	}
}

// When every node is excluded the request has nowhere left to go, and that must
// be reported as such rather than by silently reusing an excluded node.
func TestRandomRoute_AllExcludedReportsNoNodes(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat", "Plat", nil, nil)
	pool.addPlatform(plat)

	h, e := newRoutableEntry(t, `{"id":"only-node"}`, "198.51.100.90")
	pool.addEntry(h, e)
	pool.rebuildPlatformView(plat)

	stats := NewIPLoadStats()
	got, err := randomRoute(plat, stats, pool, "example.com", nil, 10*time.Minute, HealthWeights{}, []node.Hash{h})
	if !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("err: got %v, want ErrNoAvailableNodes", err)
	}
	if got != node.Zero {
		t.Fatalf("node: got %v, want the zero hash", got)
	}
}

// A sticky request that cannot use its own node borrows another one for that
// request only. The lease must come back untouched, so one failure does not
// relocate the account's egress IP.
func TestRouteRequestExcluding_BorrowsWithoutTouchingLease(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-borrow", "plat-borrow", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var events []LeaseEvent
	router := newTestRouter(pool, func(e LeaseEvent) {
		events = append(events, e)
	})

	for i := 0; i < 3; i++ {
		h, e := newRoutableEntry(t, `{"id":"borrow-`+string(rune('a'+i))+`"}`, "198.51.100.3"+string(rune('0'+i)))
		pool.addEntry(h, e)
	}
	pool.rebuildPlatformView(plat)

	key := model.LeaseKey{PlatformID: plat.ID, Account: "acct"}
	first, err := router.RouteRequest("plat-borrow", "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}

	before := router.ReadLease(key)
	if before == nil {
		t.Fatal("expected a lease to have been created")
	}
	if before.NodeHash != first.NodeHash.Hex() {
		t.Fatalf("lease node %q does not match the routed node %v", before.NodeHash, first.NodeHash)
	}
	eventsBefore := len(events)

	borrowed, err := router.RouteRequestExcluding(
		"plat-borrow", "acct", "example.com",
		RouteOptions{Exclude: []node.Hash{first.NodeHash}})
	if err != nil {
		t.Fatalf("RouteRequestExcluding: %v", err)
	}
	if borrowed.NodeHash == first.NodeHash {
		t.Fatalf("borrowed the excluded node %v", borrowed.NodeHash)
	}
	if !borrowed.Borrowed {
		t.Fatal("expected the result to be marked borrowed")
	}

	after := router.ReadLease(key)
	if after == nil {
		t.Fatal("the lease should survive a borrowed request")
	}
	if after.NodeHash != before.NodeHash {
		t.Fatalf("lease moved from %q to %q on a borrow", before.NodeHash, after.NodeHash)
	}
	if after.LastAccessedNs != before.LastAccessedNs {
		t.Fatal("a borrowed request must not touch the lease's access time")
	}
	if len(events) != eventsBefore {
		t.Fatalf("a borrow must not emit lease events, got %d new", len(events)-eventsBefore)
	}
}

// Without an exclude list the ordinary sticky path is used and the result is not
// a borrow.
func TestRouteRequestExcluding_EmptyOptionsBehavesNormally(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-normal", "plat-normal", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	router := newTestRouter(pool, func(LeaseEvent) {})
	h, e := newRoutableEntry(t, `{"id":"normal-node"}`, "198.51.100.95")
	pool.addEntry(h, e)
	pool.rebuildPlatformView(plat)

	got, err := router.RouteRequestExcluding("plat-normal", "acct", "example.com", RouteOptions{})
	if err != nil {
		t.Fatalf("RouteRequestExcluding: %v", err)
	}
	if got.Borrowed {
		t.Fatal("an ordinary request must not be a borrow")
	}
	if got.NodeHash == node.Zero {
		t.Fatal("expected a node")
	}
}

// On a single-node platform, excluding that node leaves nowhere to go. The
// account's lease must survive that: deleting it would relocate the account on
// the strength of one failed request, which is the exact thing borrow-only
// routing exists to prevent.
func TestRouteRequestExcluding_SingleNodeExcludedKeepsLease(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-single", "plat-single", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var events []LeaseEvent
	router := newTestRouter(pool, func(e LeaseEvent) {
		events = append(events, e)
	})

	h, e := newRoutableEntry(t, `{"id":"only"}`, "198.51.100.99")
	pool.addEntry(h, e)
	pool.rebuildPlatformView(plat)

	key := model.LeaseKey{PlatformID: plat.ID, Account: "acct"}
	first, err := router.RouteRequest("plat-single", "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	before := router.ReadLease(key)
	if before == nil {
		t.Fatal("expected a lease")
	}
	eventsBefore := len(events)

	_, err = router.RouteRequestExcluding(
		"plat-single", "acct", "example.com",
		RouteOptions{Exclude: []node.Hash{first.NodeHash}})
	if !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("err: got %v, want ErrNoAvailableNodes", err)
	}

	after := router.ReadLease(key)
	if after == nil {
		t.Fatal("the lease must survive exhausted exclusions")
	}
	if after.NodeHash != before.NodeHash {
		t.Fatalf("lease moved from %q to %q", before.NodeHash, after.NodeHash)
	}
	for _, ev := range events[eventsBefore:] {
		if ev.Type == LeaseRemove || ev.Type == LeaseExpire {
			t.Fatalf("exhausted exclusions must not emit %v", ev.Type)
		}
	}
}
