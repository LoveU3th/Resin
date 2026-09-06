package routing

import (
	"errors"
	"sync"
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

// newProvenEntry returns a routable node with the given number of successful
// observations behind its score, so it counts as measured rather than merely
// healthy. Zero successes yields a node nobody has seen fail: score 1, no
// evidence.
func newProvenEntry(t *testing.T, raw, ip string, successes int) (node.Hash, *node.NodeEntry) {
	t.Helper()
	h, e := newRoutableEntry(t, raw, ip)
	for i := 0; i < successes; i++ {
		e.RecordHealthSample(true, 1, 20, 5)
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
	// Seed the observations the score needs, so this node means "proven
	// healthy" rather than "not yet measured" — an unmeasured node is charged
	// for the doubt, and that is a different case with a different assertion.
	for i := 0; i < healthTestWeights().MinSamplesForFilter; i++ {
		entry.RecordHealthSample(true, 1, 20, 5)
	}
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
	// Too few observations to trust the score. Six failures score ~31%, well
	// below the threshold, so this node only passes on the sample count: drop
	// that gate and it would be filtered.
	fewH, fewEntry := newUnhealthyEntry(t, `{"id":"few"}`, "198.51.100.11", 6)
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
	// Without this the case proves nothing: the node has to be failing *and*
	// short of the bar, so that only the sample count can save it.
	if fewEntry.HealthScore()*100 >= float64(weights.FilterThresholdPercent) {
		t.Fatalf("precondition: the unmeasured node must score below %d%%, got %.1f%%",
			weights.FilterThresholdPercent, fewEntry.HealthScore()*100)
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

// An unmeasured node starts at a perfect score, so on score alone it ties with
// a node that has a long track record behind it. It has to pay for the doubt
// instead, and a proven node has to pay nothing.
func TestHealthPenalty_UnmeasuredNodePaysForTheDoubt(t *testing.T) {
	_, fresh := newProvenEntry(t, `{"id":"fresh"}`, "198.51.100.40", 0)
	_, proven := newProvenEntry(t, `{"id":"proven"}`, "198.51.100.41", 8)

	penaltyNs := 2000 * float64(time.Millisecond)
	weights := HealthWeights{PenaltyNs: penaltyNs, MinSamplesForFilter: 8}

	if fresh.HealthScore() != 1 {
		t.Fatalf("precondition: an unmeasured node scores 1, got %v", fresh.HealthScore())
	}
	if got := healthPenalty(proven, weights); got != 0 {
		t.Fatalf("a proven healthy node must not be penalized, got %v", got)
	}
	if got := healthPenalty(fresh, weights); got != penaltyNs {
		t.Fatalf("unmeasured penalty: got %v, want %v", got, penaltyNs)
	}
}

// The taper has to fall monotonically to nothing as observations accumulate:
// a node is charged for the doubt it still carries, not for doubt it has
// already retired.
func TestHealthPenalty_UnknownPenaltyTapersWithSamples(t *testing.T) {
	penaltyNs := 2000 * float64(time.Millisecond)
	const bar = 8
	weights := HealthWeights{PenaltyNs: penaltyNs, MinSamplesForFilter: bar}

	previous := -1.0
	for samples := 0; samples <= bar; samples++ {
		_, entry := newProvenEntry(t, `{"id":"taper"}`, "198.51.100.42", samples)
		got := healthPenalty(entry, weights)
		// Assert the taper itself, not just its direction: a penalty that is
		// always zero is monotonic too, and would pass on shape alone.
		want := (1 - float64(samples)/float64(bar)) * penaltyNs
		if diff := got - want; diff > 1e-6 || diff < -1e-6 {
			t.Fatalf("penalty at %d samples: got %v, want %v", samples, got, want)
		}
		if previous >= 0 && got > previous {
			t.Fatalf("penalty rose at %d samples: %v > %v", samples, got, previous)
		}
		previous = got
	}
	if previous != 0 {
		t.Fatalf("penalty once the sample bar is reached: got %v, want 0", previous)
	}
}

// health_penalty_ms is the whole subsystem's off switch, so zero must disable
// the unknown charge along with everything else.
func TestHealthPenalty_ZeroPenaltyDisablesTheUnknownChargeToo(t *testing.T) {
	_, fresh := newProvenEntry(t, `{"id":"fresh-off"}`, "198.51.100.43", 0)

	weights := HealthWeights{MinSamplesForFilter: 8}
	if got := healthPenalty(fresh, weights); got != 0 {
		t.Fatalf("with no penalty configured an unmeasured node must not be charged, got %v", got)
	}
}

// Without a sample bar there is no line between "measured" and "not yet", so
// the unknown charge has nothing to taper against.
func TestHealthPenalty_NoSampleBarMeansNoUnknownCharge(t *testing.T) {
	_, fresh := newProvenEntry(t, `{"id":"fresh-nobar"}`, "198.51.100.44", 0)

	weights := HealthWeights{PenaltyNs: 2000 * float64(time.Millisecond)}
	if got := healthPenalty(fresh, weights); got != 0 {
		t.Fatalf("with no sample bar an unmeasured node must not be charged, got %v", got)
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

// The symptom this fixes: a node nobody has measured yet used to tie with
// proven nodes on score, so it took a full share of the traffic and a request
// could land on one that never worked. Filtering cannot help here — an
// unmeasured node is deliberately never filtered — so this is the scoring
// penalty doing its job.
func TestRandomRoute_PrefersProvenNodesOverUnmeasuredOnes(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat", "Plat", nil, nil)
	pool.addPlatform(plat)

	freshH, freshEntry := newProvenEntry(t, `{"id":"fresh"}`, "198.51.100.50", 0)
	pool.addEntry(freshH, freshEntry)
	for i := 0; i < 5; i++ {
		h, e := newProvenEntry(t, `{"id":"proven-`+string(rune('a'+i))+`"}`, "198.51.100.6"+string(rune('0'+i)), 8)
		pool.addEntry(h, e)
	}
	pool.rebuildPlatformView(plat)

	stats := NewIPLoadStats()
	const iterations = 200
	var freshPicks int
	for i := 0; i < iterations; i++ {
		got, err := randomRoute(plat, stats, pool, "example.com", nil, 10*time.Minute, healthTestWeights(), nil)
		if err != nil {
			t.Fatalf("randomRoute: %v", err)
		}
		if got == freshH {
			freshPicks++
		}
	}
	if freshPicks > iterations/20 {
		t.Fatalf("unmeasured node picked %d/%d times, want <= %d", freshPicks, iterations, iterations/20)
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

// newHealthTestRouter builds a router with the health tuning the sticky-path
// decisions below assume.
func newHealthTestRouter(pool PoolAccessor, onEvent LeaseEventFunc) *Router {
	return NewRouter(RouterConfig{
		Pool:                         pool,
		Authorities:                  func() []string { return []string{"cloudflare.com"} },
		P2CWindow:                    func() time.Duration { return 10 * time.Minute },
		HealthPenaltyMs:              func() int { return 2000 },
		HealthFilterThresholdPercent: func() int { return 40 },
		HealthMinSamplesForFilter:    func() int { return 8 },
		OnLeaseEvent:                 onEvent,
	})
}

// rejects is the lever the sticky path uses, so pin down its boundaries: a node
// is rejected only on the strength of its own track record, never on the absence
// of one.
func TestHealthWeights_Rejects(t *testing.T) {
	pool := newRouterTestPool()

	badH, badEntry := newUnhealthyEntry(t, `{"id":"reject-bad"}`, "198.51.100.60", 20)
	pool.addEntry(badH, badEntry)
	// Too few observations to trust the score: six failures score ~31%, well
	// below the threshold, so the only thing keeping this node out of the
	// rejected set is its sample count.
	fewH, fewEntry := newUnhealthyEntry(t, `{"id":"reject-few"}`, "198.51.100.61", 6)
	pool.addEntry(fewH, fewEntry)
	goodH, goodEntry := newProvenEntry(t, `{"id":"reject-good"}`, "198.51.100.62", 8)
	pool.addEntry(goodH, goodEntry)

	weights := healthTestWeights()

	if badEntry.HealthScore()*100 >= float64(weights.FilterThresholdPercent) {
		t.Fatalf("precondition: bad node health %v should be below %d%%",
			badEntry.HealthScore(), weights.FilterThresholdPercent)
	}
	if !weights.rejects(pool, badH) {
		t.Fatal("a node below the threshold with enough samples must be rejected")
	}
	if weights.rejects(pool, goodH) {
		t.Fatal("a healthy node must never be rejected")
	}
	// Too few observations to trust the score: unmeasured, not bad.
	if fewEntry.HealthSamples() >= uint32(weights.MinSamplesForFilter) {
		t.Fatalf("precondition: node should have < %d samples", weights.MinSamplesForFilter)
	}
	// The node is failing on score too, so only its sample count keeps it out
	// of the rejected set — otherwise this case proves nothing.
	if fewEntry.HealthScore()*100 >= float64(weights.FilterThresholdPercent) {
		t.Fatalf("precondition: the unmeasured node must score below %d%%, got %.1f%%",
			weights.FilterThresholdPercent, fewEntry.HealthScore()*100)
	}
	if weights.rejects(pool, fewH) {
		t.Fatalf("a node with %d samples must not be rejected (min %d) — the scoring "+
			"penalty holds it back, a rejection would misread it as failing",
			fewEntry.HealthSamples(), weights.MinSamplesForFilter)
	}
	// Unknown nodes are not rejected either.
	if weights.rejects(pool, node.Hash{0xde, 0xad}) {
		t.Fatal("an unknown node must not be rejected")
	}
	// With no threshold or no sample bar there is nothing to reject on.
	if (HealthWeights{PenaltyNs: weights.PenaltyNs}).rejects(pool, badH) {
		t.Fatal("with no threshold configured nothing may be rejected")
	}
	if (HealthWeights{PenaltyNs: weights.PenaltyNs, FilterThresholdPercent: weights.FilterThresholdPercent}).rejects(pool, badH) {
		t.Fatal("with no sample bar nothing may be rejected")
	}
}

// A sticky account parked on a failing node must stop being served by it, but
// must not be relocated: this request borrows another node, and the lease — and
// with it the account's egress IP — is left exactly as it was.
func TestRouteRequest_DegradedStickyNodeIsBorrowedAround(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-degraded", "plat-degraded", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var events []LeaseEvent
	router := newHealthTestRouter(pool, func(e LeaseEvent) { events = append(events, e) })

	// The lease can only land on the degraded node while it is the sole
	// candidate; filtering would otherwise never hand it out.
	badH, badEntry := newUnhealthyEntry(t, `{"id":"degraded"}`, "198.51.100.70", 20)
	pool.addEntry(badH, badEntry)
	pool.rebuildPlatformView(plat)

	first, err := router.RouteRequest(plat.Name, "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	if first.NodeHash != badH {
		t.Fatalf("precondition: the lease should sit on the degraded node, got %v", first.NodeHash)
	}

	for i := 0; i < 2; i++ {
		h, e := newProvenEntry(t, `{"id":"healthy-`+string(rune('a'+i))+`"}`, "198.51.100.8"+string(rune('0'+i)), 8)
		pool.addEntry(h, e)
	}
	pool.rebuildPlatformView(plat)

	key := model.LeaseKey{PlatformID: plat.ID, Account: "acct"}
	before := router.ReadLease(key)
	if before == nil {
		t.Fatal("expected a lease to have been created")
	}
	eventsBefore := len(events)

	second, err := router.RouteRequest(plat.Name, "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	if second.NodeHash == badH {
		t.Fatal("the degraded node still served the account")
	}
	if !second.Borrowed {
		t.Fatal("expected the result to be marked borrowed")
	}

	after := router.ReadLease(key)
	if after == nil {
		t.Fatal("the lease must survive a borrow")
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

// With nowhere to borrow, a degraded sticky node keeps serving: health must not
// turn a routable lease into a failed request, nor into lease churn.
func TestRouteRequest_SingleNodePlatformKeepsLeaseWhenDegraded(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-lonely", "plat-lonely", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var events []LeaseEvent
	router := newHealthTestRouter(pool, func(e LeaseEvent) { events = append(events, e) })

	badH, badEntry := newUnhealthyEntry(t, `{"id":"lonely"}`, "198.51.100.75", 20)
	pool.addEntry(badH, badEntry)
	pool.rebuildPlatformView(plat)

	if _, err := router.RouteRequest(plat.Name, "acct", "example.com"); err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	key := model.LeaseKey{PlatformID: plat.ID, Account: "acct"}
	before := router.ReadLease(key)
	if before == nil {
		t.Fatal("expected a lease to have been created")
	}
	eventsBefore := len(events)

	second, err := router.RouteRequest(plat.Name, "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	if second.NodeHash != badH {
		t.Fatalf("node: got %v, want the only node %v", second.NodeHash, badH)
	}
	if second.Borrowed {
		t.Fatal("with nothing to borrow the lease hit must be returned as-is")
	}

	after := router.ReadLease(key)
	if after == nil {
		t.Fatal("the lease must not be dropped")
	}
	for _, ev := range events[eventsBefore:] {
		if ev.Type == LeaseRemove || ev.Type == LeaseExpire {
			t.Fatalf("a degraded single node must not emit %v", ev.Type)
		}
	}
}

// A borrow prefers another node behind the same egress IP, so the account keeps
// its address even while its own node is out of favour.
func TestRouteRequest_BorrowPrefersTheSameEgressIP(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-sameip", "plat-sameip", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	router := newHealthTestRouter(pool, func(LeaseEvent) {})

	badH, badEntry := newUnhealthyEntry(t, `{"id":"sameip-bad"}`, "198.51.100.90", 20)
	pool.addEntry(badH, badEntry)
	pool.rebuildPlatformView(plat)

	if _, err := router.RouteRequest(plat.Name, "acct", "example.com"); err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}

	sameH, sameEntry := newProvenEntry(t, `{"id":"sameip-healthy"}`, "198.51.100.90", 8)
	pool.addEntry(sameH, sameEntry)
	otherH, otherEntry := newProvenEntry(t, `{"id":"otherip-healthy"}`, "198.51.100.91", 8)
	pool.addEntry(otherH, otherEntry)
	pool.rebuildPlatformView(plat)

	second, err := router.RouteRequest(plat.Name, "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	if second.NodeHash == badH {
		t.Fatal("the degraded node still served the account")
	}
	if second.EgressIP != badEntry.GetEgressIP() {
		t.Fatalf("egress IP: got %v, want the lease's %v", second.EgressIP, badEntry.GetEgressIP())
	}
	if second.NodeHash != sameH {
		t.Fatalf("borrowed %v, want the same-IP node %v", second.NodeHash, sameH)
	}
}

// The sample bar is the only thing separating "unmeasured" from "failing", so
// pin it down from both sides: one observation short keeps a node in play, one
// at the bar hands it over to the score.
func TestHealthWeights_RejectsAtExactlyTheSampleBar(t *testing.T) {
	pool := newRouterTestPool()
	weights := healthTestWeights()
	bar := weights.MinSamplesForFilter

	belowH, belowEntry := newUnhealthyEntry(t, `{"id":"bar-below"}`, "198.51.100.64", bar-1)
	pool.addEntry(belowH, belowEntry)
	atH, atEntry := newUnhealthyEntry(t, `{"id":"bar-at"}`, "198.51.100.65", bar)
	pool.addEntry(atH, atEntry)

	if belowEntry.HealthSamples() != uint32(bar-1) {
		t.Fatalf("precondition: samples got %d, want %d", belowEntry.HealthSamples(), bar-1)
	}
	if atEntry.HealthSamples() != uint32(bar) {
		t.Fatalf("precondition: samples got %d, want %d", atEntry.HealthSamples(), bar)
	}
	if weights.rejects(pool, belowH) {
		t.Fatal("one observation short of the bar must not be rejected")
	}
	if !weights.rejects(pool, atH) {
		t.Fatal("a failing node at the sample bar must be rejected")
	}
	if !weights.allows(pool, belowH) {
		t.Fatal("one observation short of the bar must still be a candidate")
	}
	if weights.allows(pool, atH) {
		t.Fatal("a failing node at the sample bar must be filtered")
	}
}

// The threshold is inclusive on the way in: scoring exactly the threshold still
// counts as healthy enough.
func TestHealthWeights_ThresholdBoundaryIsInclusive(t *testing.T) {
	pool := newRouterTestPool()
	h, entry := newUnhealthyEntry(t, `{"id":"boundary"}`, "198.51.100.66", 8)
	pool.addEntry(h, entry)

	base := healthTestWeights()
	score := entry.HealthScore() * 100
	at := int(score) // at or below the score: still healthy enough
	over := at + 1   // above the score: rejected

	withThreshold := func(percent int) HealthWeights {
		return HealthWeights{
			PenaltyNs:              base.PenaltyNs,
			FilterThresholdPercent: percent,
			MinSamplesForFilter:    base.MinSamplesForFilter,
		}
	}

	if withThreshold(at).rejects(pool, h) {
		t.Fatalf("a node at %.2f%% must not be rejected by a %d%% threshold", score, at)
	}
	if !withThreshold(over).rejects(pool, h) {
		t.Fatalf("a node at %.2f%% must be rejected by a %d%% threshold", score, over)
	}
}

// Borrowing runs inside the leases.Compute callback, so concurrent requests for
// one degraded account must neither corrupt nor relocate the lease.
func TestRouteRequest_ConcurrentBorrowOnDegradedSticky(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-concurrent", "plat-concurrent", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	router := newHealthTestRouter(pool, func(LeaseEvent) {})

	badH, badEntry := newUnhealthyEntry(t, `{"id":"concurrent-bad"}`, "198.51.100.95", 20)
	pool.addEntry(badH, badEntry)
	pool.rebuildPlatformView(plat)
	if _, err := router.RouteRequest(plat.Name, "acct", "example.com"); err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	for i := 0; i < 3; i++ {
		h, e := newProvenEntry(t, `{"id":"concurrent-good-`+string(rune('a'+i))+`"}`, "198.51.100.9"+string(rune('6'+i)), 8)
		pool.addEntry(h, e)
	}
	pool.rebuildPlatformView(plat)

	key := model.LeaseKey{PlatformID: plat.ID, Account: "acct"}
	before := router.ReadLease(key)
	if before == nil {
		t.Fatal("expected a lease to have been created")
	}

	const goroutines = 16
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			res, err := router.RouteRequest(plat.Name, "acct", "example.com")
			if err != nil {
				t.Errorf("RouteRequest: %v", err)
				return
			}
			if res.NodeHash == badH {
				t.Errorf("the degraded node served a request")
			}
		}()
	}
	wg.Wait()

	after := router.ReadLease(key)
	if after == nil {
		t.Fatal("the lease must survive concurrent borrows")
	}
	if after.NodeHash != before.NodeHash {
		t.Fatalf("lease moved from %q to %q", before.NodeHash, after.NodeHash)
	}
}
