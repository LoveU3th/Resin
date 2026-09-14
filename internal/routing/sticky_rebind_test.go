package routing

import (
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

// eventLog collects lease events. The callback runs inside a lease table
// Compute, so concurrent requests can invoke it from several goroutines.
type eventLog struct {
	mu     sync.Mutex
	events []LeaseEvent
}

func (l *eventLog) record(e LeaseEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) snapshot() []LeaseEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]LeaseEvent(nil), l.events...)
}

func (l *eventLog) countOf(kind LeaseEventType) int {
	n := 0
	for _, e := range l.snapshot() {
		if e.Type == kind {
			n++
		}
	}
	return n
}

func (l *eventLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = nil
}

// newStrictRebindRouter builds a router with the strict-rebind tuning these
// tests assume: health scoring on, rebind on, tiers at 85% and 70% over eight
// observations, five minute cooldown.
func newStrictRebindRouter(pool PoolAccessor, onEvent LeaseEventFunc) *Router {
	return NewRouter(RouterConfig{
		Pool:                         pool,
		Authorities:                  func() []string { return []string{"cloudflare.com"} },
		P2CWindow:                    func() time.Duration { return 10 * time.Minute },
		HealthPenaltyMs:              func() int { return 2000 },
		HealthFilterThresholdPercent: func() int { return 40 },
		HealthMinSamplesForFilter:    func() int { return 8 },
		StickyStrictRebindEnabled:    func() bool { return true },
		StickyStrictThresholdPercent: func() int { return 85 },
		StickyStrictFallbackPercent:  func() int { return 70 },
		StickyStrictCooldown:         func() time.Duration { return 5 * time.Minute },
		OnLeaseEvent:                 onEvent,
	})
}

// newMidEntry returns a node that has been measured long enough to be trusted
// and then failed a few times, so its score lands between the fallback tier and
// the primary tier.
func newMidEntry(t *testing.T, raw, ip string, failures int) (node.Hash, *node.NodeEntry) {
	t.Helper()
	h, e := newRoutableEntry(t, raw, ip)
	for i := 0; i < 8; i++ {
		e.RecordHealthSample(true, 1, 20, 5)
	}
	for i := 0; i < failures; i++ {
		e.RecordHealthSample(false, 1, 20, 5)
	}
	return h, e
}

// leaseOrFail reads the live lease, marker fields included. ReadLease
// deliberately drops the rebind marker, so tests that care about it come
// through here.
func leaseOrFail(t *testing.T, r *Router, platformID, account string) Lease {
	t.Helper()
	state, ok := r.states.Load(platformID)
	if !ok {
		t.Fatalf("no routing state for platform %q", platformID)
	}
	lease, ok := state.Leases.GetLease(account)
	if !ok {
		t.Fatalf("no lease for account %q", account)
	}
	return lease
}

// placeLease writes a lease directly, which is how the tests pin an account to
// a node without depending on which node a fresh pick would choose.
func placeLease(t *testing.T, r *Router, plat *platform.Platform, account string, h node.Hash, entry *node.NodeEntry) {
	t.Helper()
	state := r.ensurePlatformState(plat.ID)
	now := time.Now()
	state.Leases.CreateLease(account, Lease{
		NodeHash:       h,
		EgressIP:       entry.GetEgressIP(),
		CreatedAtNs:    now.UnixNano(),
		ExpiryNs:       now.Add(time.Hour).UnixNano(),
		LastAccessedNs: now.UnixNano(),
	})
}

// The strict pool's eligibility answers the opposite question from
// HealthWeights.allows: there, too few observations means "let it through";
// here it means "no evidence, not eligible".
func TestStrictEligible(t *testing.T) {
	_, provenEntry := newProvenEntry(t, `{"id":"strict-proven"}`, "198.51.100.90", 8)
	_, lowEntry := newUnhealthyEntry(t, `{"id":"strict-low"}`, "198.51.100.91", 20)
	_, unmeasuredEntry := newRoutableEntry(t, `{"id":"strict-unmeasured"}`, "198.51.100.92")

	if !strictEligible(provenEntry, 85, 8) {
		t.Fatalf("a measured healthy node must be eligible (score %.2f, samples %d)",
			provenEntry.HealthScore(), provenEntry.HealthSamples())
	}
	if strictEligible(lowEntry, 85, 8) {
		t.Fatalf("a measured failing node must not be eligible (score %.2f)", lowEntry.HealthScore())
	}
	// The whole point of the type: a perfect score with no observations is not
	// evidence of health.
	if unmeasuredEntry.HealthSamples() != 0 {
		t.Fatalf("precondition: expected an unmeasured node, got %d samples", unmeasuredEntry.HealthSamples())
	}
	if strictEligible(unmeasuredEntry, 0, 8) {
		t.Fatal("an unmeasured node must not be eligible at any threshold once a sample bar is set")
	}
	// With no bar at all the last-resort tier accepts anything, which is what
	// lets a restart still move a lease off a node that just failed.
	if !strictEligible(unmeasuredEntry, 0, 0) {
		t.Fatal("with no sample bar and no threshold every node must be eligible")
	}
	if !strictEligible(lowEntry, 0, 0) {
		t.Fatal("the last-resort tier must not judge a node by its score")
	}
}

func TestMarkStickyNodeFailure_MarksOnlyLeaseNode(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-mark", "plat-mark", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var log eventLog
	router := newStrictRebindRouter(pool, log.record)

	failingH, failingEntry := newUnhealthyEntry(t, `{"id":"mark-lease"}`, "198.51.100.93", 20)
	otherH, otherEntry := newProvenEntry(t, `{"id":"mark-other"}`, "198.51.100.94", 8)
	pool.addEntry(failingH, failingEntry)
	pool.addEntry(otherH, otherEntry)
	pool.rebuildPlatformView(plat)

	placeLease(t, router, plat, "acct", failingH, failingEntry)

	// A failure on some other node says nothing about this account's lease.
	if router.MarkStickyNodeFailure(plat.ID, "acct", otherH) {
		t.Fatal("a failure on a different node must not mark the lease")
	}
	if leaseOrFail(t, router, plat.ID, "acct").NeedsRebind {
		t.Fatal("the lease must not be marked by an unrelated node's failure")
	}

	if !router.MarkStickyNodeFailure(plat.ID, "acct", failingH) {
		t.Fatal("a failure on the leased node must mark the lease")
	}
	marked := leaseOrFail(t, router, plat.ID, "acct")
	if !marked.NeedsRebind {
		t.Fatal("expected the lease to be marked for a rebind")
	}
	if !marked.NeedsStrictRebind(time.Now().UnixNano()) {
		t.Fatal("the mark must still be live inside the cooldown")
	}
}

func TestMarkStickyNodeFailure_NoLeaseIsNoop(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-nolease", "plat-nolease", nil, nil)
	pool.addPlatform(plat)

	var log eventLog
	router := newStrictRebindRouter(pool, log.record)

	h, entry := newProvenEntry(t, `{"id":"nolease-node"}`, "198.51.100.95", 8)
	pool.addEntry(h, entry)
	pool.rebuildPlatformView(plat)

	if router.MarkStickyNodeFailure(plat.ID, "acct", h) {
		t.Fatal("an account with no lease must not be marked")
	}
	if router.MarkStickyNodeFailure("", "acct", h) {
		t.Fatal("a missing platform must not be marked")
	}
	if router.MarkStickyNodeFailure(plat.ID, "", h) {
		t.Fatal("an empty account must not be marked")
	}
}

// With the feature unconfigured the router behaves exactly as before, which is
// what keeps a deployment that never sets the new fields unchanged.
func TestMarkStickyNodeFailure_DisabledIsNoop(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-off", "plat-off", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var log eventLog
	router := newHealthTestRouter(pool, log.record)

	h, entry := newUnhealthyEntry(t, `{"id":"off-node"}`, "198.51.100.96", 20)
	pool.addEntry(h, entry)
	pool.rebuildPlatformView(plat)

	if _, err := router.RouteRequest(plat.Name, "acct", "example.com"); err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	if router.MarkStickyNodeFailure(plat.ID, "acct", h) {
		t.Fatal("a router without the tuning must not mark leases")
	}
}

// The core behaviour: a connect failure on the lease node moves the lease into
// the strict pool on the next request, instead of borrowing another node for
// one request and going back to the failed one afterwards.
func TestRouteRequest_RebindOnConnectFailure(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-rebind", "plat-rebind", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var log eventLog
	router := newStrictRebindRouter(pool, log.record)

	badH, badEntry := newUnhealthyEntry(t, `{"id":"rebind-bad"}`, "198.51.100.97", 20)
	pool.addEntry(badH, badEntry)
	pool.rebuildPlatformView(plat)

	placeLease(t, router, plat, "acct", badH, badEntry)
	before := leaseOrFail(t, router, plat.ID, "acct")

	goodH, goodEntry := newProvenEntry(t, `{"id":"rebind-good"}`, "198.51.100.98", 8)
	pool.addEntry(goodH, goodEntry)
	pool.rebuildPlatformView(plat)

	if !router.MarkStickyNodeFailure(plat.ID, "acct", badH) {
		t.Fatal("expected the lease to be marked")
	}
	replaces := log.countOf(LeaseReplace)

	next, err := router.RouteRequest(plat.Name, "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	if next.NodeHash != goodH {
		t.Fatalf("node: got %v, want the healthy node %v", next.NodeHash, goodH)
	}
	// Borrowed would mean the lease stayed put and this request merely worked
	// around it — the behaviour this feature exists to replace.
	if next.Borrowed {
		t.Fatal("a rebind must write the lease, not borrow around it")
	}
	if got := log.countOf(LeaseReplace); got != replaces+1 {
		t.Fatalf("expected exactly one LeaseReplace, got %d", got-replaces)
	}

	after := leaseOrFail(t, router, plat.ID, "acct")
	if after.NodeHash != goodH {
		t.Fatalf("lease node: got %v, want %v", after.NodeHash, goodH)
	}
	if after.EgressIP != goodEntry.GetEgressIP() {
		t.Fatalf("lease egress IP: got %v, want %v", after.EgressIP, goodEntry.GetEgressIP())
	}
	if after.NeedsRebind {
		t.Fatal("a completed rebind must clear the mark")
	}
	// Moving a lease must not extend how long the account stays sticky.
	if after.CreatedAtNs != before.CreatedAtNs || after.ExpiryNs != before.ExpiryNs {
		t.Fatalf("the lease window moved: created %d->%d, expiry %d->%d",
			before.CreatedAtNs, after.CreatedAtNs, before.ExpiryNs, after.ExpiryNs)
	}

	// The per-IP lease count follows the address.
	snapshot := router.SnapshotIPLoad(plat.ID)
	if snapshot[badEntry.GetEgressIP()] != 0 {
		t.Fatalf("the old address still holds %d leases", snapshot[badEntry.GetEgressIP()])
	}
	if snapshot[goodEntry.GetEgressIP()] != 1 {
		t.Fatalf("the new address holds %d leases, want 1", snapshot[goodEntry.GetEgressIP()])
	}

	// And the mark must not come back: the next request is served normally.
	third, err := router.RouteRequest(plat.Name, "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	if third.NodeHash != goodH || third.Borrowed {
		t.Fatalf("third request: got %v (borrowed %v), want the same leased node", third.NodeHash, third.Borrowed)
	}
}

// With no eligible node at all the lease must stay put: a rebind that cannot
// find a target must never turn a working lease into a failed request.
func TestRouteRequest_RebindNeverPicksFailedNode(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-noreb", "plat-noreb", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var log eventLog
	router := newStrictRebindRouter(pool, log.record)

	onlyH, onlyEntry := newUnhealthyEntry(t, `{"id":"noreb-only"}`, "198.51.100.99", 20)
	pool.addEntry(onlyH, onlyEntry)
	pool.rebuildPlatformView(plat)

	placeLease(t, router, plat, "acct", onlyH, onlyEntry)
	if !router.MarkStickyNodeFailure(plat.ID, "acct", onlyH) {
		t.Fatal("expected the lease to be marked")
	}
	replaces := log.countOf(LeaseReplace)

	next, err := router.RouteRequest(plat.Name, "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	if next.NodeHash != onlyH {
		t.Fatalf("node: got %v, want the only node %v", next.NodeHash, onlyH)
	}
	if got := log.countOf(LeaseReplace); got != replaces {
		t.Fatal("an impossible rebind must not emit a replacement")
	}
	// The mark stays armed, so the account keeps trying to move while the pool
	// offers nothing; the deadline is what stops that from being a scan on
	// every request.
	if !leaseOrFail(t, router, plat.ID, "acct").NeedsRebind {
		t.Fatal("a failed rebind must leave the mark armed")
	}
}

// The tiers: a pool with nothing above the primary threshold still moves the
// lease off the failed node, using the fallback tier.
func TestRouteRequest_RebindLayeredFallback(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-tier", "plat-tier", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var log eventLog
	router := newStrictRebindRouter(pool, log.record)

	badH, badEntry := newUnhealthyEntry(t, `{"id":"tier-bad"}`, "198.51.100.100", 20)
	pool.addEntry(badH, badEntry)
	pool.rebuildPlatformView(plat)

	placeLease(t, router, plat, "acct", badH, badEntry)

	midH, midEntry := newMidEntry(t, `{"id":"tier-mid"}`, "198.51.100.101", 5)
	pool.addEntry(midH, midEntry)
	pool.rebuildPlatformView(plat)

	score := midEntry.HealthScore() * 100
	if score >= 85 || score < 70 {
		t.Fatalf("precondition: the middle node must sit between the tiers, got %.1f%%", score)
	}
	if !router.MarkStickyNodeFailure(plat.ID, "acct", badH) {
		t.Fatal("expected the lease to be marked")
	}

	next, err := router.RouteRequest(plat.Name, "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	if next.NodeHash != midH {
		t.Fatalf("node: got %v, want the fallback-tier node %v", next.NodeHash, midH)
	}
	if leaseOrFail(t, router, plat.ID, "acct").NodeHash != midH {
		t.Fatal("the lease must follow the fallback-tier pick")
	}
}

// A pool where nothing has been measured yet still has to be able to move a
// lease off a node that failed: right after a restart every node is unmeasured,
// and refusing to move would pin the account to a broken node indefinitely.
func TestRouteRequest_RebindLastResortWhenNothingMeasured(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-resort", "plat-resort", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var log eventLog
	router := newStrictRebindRouter(pool, log.record)

	badH, badEntry := newUnhealthyEntry(t, `{"id":"resort-bad"}`, "198.51.100.102", 20)
	pool.addEntry(badH, badEntry)
	pool.rebuildPlatformView(plat)

	placeLease(t, router, plat, "acct", badH, badEntry)

	freshH, freshEntry := newRoutableEntry(t, `{"id":"resort-fresh"}`, "198.51.100.103")
	pool.addEntry(freshH, freshEntry)
	pool.rebuildPlatformView(plat)

	if strictEligible(freshEntry, 85, 8) || strictEligible(freshEntry, 70, 8) {
		t.Fatal("precondition: an unmeasured node must not clear either health tier")
	}
	if !router.MarkStickyNodeFailure(plat.ID, "acct", badH) {
		t.Fatal("expected the lease to be marked")
	}

	next, err := router.RouteRequest(plat.Name, "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	if next.NodeHash != freshH {
		t.Fatalf("node: got %v, want the unmeasured node %v", next.NodeHash, freshH)
	}
	if leaseOrFail(t, router, plat.ID, "acct").NodeHash != freshH {
		t.Fatal("the lease must follow the last-resort pick")
	}
}

// A rebind prefers a healthy node that shares the lease's address, so a move
// does not have to change the address a site already knows.
func TestRouteRequest_RebindPrefersSameEgressIP(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-rebind-ip", "plat-rebind-ip", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var log eventLog
	router := newStrictRebindRouter(pool, log.record)

	const sharedIP = "198.51.100.104"
	badH, badEntry := newUnhealthyEntry(t, `{"id":"rebind-ip-bad"}`, sharedIP, 20)
	pool.addEntry(badH, badEntry)
	pool.rebuildPlatformView(plat)

	placeLease(t, router, plat, "acct", badH, badEntry)

	sameIPH, sameIPEntry := newProvenEntry(t, `{"id":"rebind-ip-same"}`, sharedIP, 8)
	otherH, otherEntry := newProvenEntry(t, `{"id":"rebind-ip-other"}`, "198.51.100.105", 12)
	pool.addEntry(sameIPH, sameIPEntry)
	pool.addEntry(otherH, otherEntry)
	pool.rebuildPlatformView(plat)

	if sameIPEntry.GetEgressIP() != badEntry.GetEgressIP() {
		t.Fatal("precondition: the preferred node must share the lease's address")
	}
	if otherEntry.GetEgressIP() == badEntry.GetEgressIP() {
		t.Fatal("precondition: the other node must be a different address")
	}
	if !router.MarkStickyNodeFailure(plat.ID, "acct", badH) {
		t.Fatal("expected the lease to be marked")
	}

	next, err := router.RouteRequest(plat.Name, "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	if next.NodeHash != sameIPH {
		t.Fatalf("node: got %v, want the same-address node %v", next.NodeHash, sameIPH)
	}
	if next.EgressIP != badEntry.GetEgressIP() {
		t.Fatalf("egress IP changed to %v, want it kept at %v", next.EgressIP, badEntry.GetEgressIP())
	}
}

// Once the cooldown has elapsed the mark stops applying: the account is served
// by its lease again rather than being moved on every request.
func TestRouteRequest_RebindCooldownExpiry(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-cooldown", "plat-cooldown", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var log eventLog
	router := newStrictRebindRouter(pool, log.record)

	// The lease node is mediocre rather than failing, so the ordinary health
	// borrow does not intervene and the assertion is about the deadline alone.
	midH, midEntry := newMidEntry(t, `{"id":"cooldown-mid"}`, "198.51.100.106", 5)
	pool.addEntry(midH, midEntry)
	pool.rebuildPlatformView(plat)

	placeLease(t, router, plat, "acct", midH, midEntry)
	goodH, goodEntry := newProvenEntry(t, `{"id":"cooldown-good"}`, "198.51.100.107", 8)
	pool.addEntry(goodH, goodEntry)
	pool.rebuildPlatformView(plat)

	if router.healthWeights().rejects(pool, midH) {
		t.Fatal("precondition: the lease node must not be rejected, or the borrow path answers instead")
	}
	state := router.ensurePlatformState(plat.ID)
	expired := leaseOrFail(t, router, plat.ID, "acct")
	expired.NeedsRebind = true
	expired.StrictUntilNs = time.Now().Add(-time.Minute).UnixNano()
	state.Leases.CreateLease("acct", expired)
	replaces := log.countOf(LeaseReplace)

	next, err := router.RouteRequest(plat.Name, "acct", "example.com")
	if err != nil {
		t.Fatalf("RouteRequest: %v", err)
	}
	if next.NodeHash != midH {
		t.Fatalf("node: got %v, want the leased node %v", next.NodeHash, midH)
	}
	if got := log.countOf(LeaseReplace); got != replaces {
		t.Fatal("an expired cooldown must not rebind")
	}
}

// Concurrent requests for one account must move the lease once, not once each.
func TestRouteRequest_ConcurrentRebindMovesOnce(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-conc", "plat-conc", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var log eventLog
	router := newStrictRebindRouter(pool, log.record)

	badH, badEntry := newUnhealthyEntry(t, `{"id":"conc-bad"}`, "198.51.100.108", 20)
	goodH, goodEntry := newProvenEntry(t, `{"id":"conc-good"}`, "198.51.100.109", 8)
	pool.addEntry(badH, badEntry)
	pool.addEntry(goodH, goodEntry)
	pool.rebuildPlatformView(plat)

	placeLease(t, router, plat, "acct", badH, badEntry)
	if !router.MarkStickyNodeFailure(plat.ID, "acct", badH) {
		t.Fatal("expected the lease to be marked")
	}
	log.reset()

	const requesters = 8
	var wg sync.WaitGroup
	results := make([]node.Hash, requesters)
	for i := 0; i < requesters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := router.RouteRequest(plat.Name, "acct", "example.com")
			if err != nil {
				t.Errorf("RouteRequest: %v", err)
				return
			}
			results[i] = res.NodeHash
		}(i)
	}
	wg.Wait()

	for i, h := range results {
		if h != goodH {
			t.Fatalf("requester %d got %v, want %v", i, h, goodH)
		}
	}
	if got := log.countOf(LeaseReplace); got != 1 {
		t.Fatalf("expected exactly one rebind, got %d replacements", got)
	}
	if got := leaseOrFail(t, router, plat.ID, "acct"); got.NodeHash != goodH {
		t.Fatalf("the lease must end up on the healthy node, got %v", got.NodeHash)
	}
	if got := router.SnapshotIPLoad(plat.ID)[goodEntry.GetEgressIP()]; got != 1 {
		t.Fatalf("the new address holds %d leases, want 1", got)
	}
}

// A rebind that lands on the same address must not disturb the per-address
// lease count: the count is about addresses, not nodes.
func TestLandRebind_SameAddressKeepsLoadCount(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-same-addr", "plat-same-addr", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	var log eventLog
	router := newStrictRebindRouter(pool, log.record)

	const sharedIP = "198.51.100.110"
	oldH, oldEntry := newUnhealthyEntry(t, `{"id":"same-addr-old"}`, sharedIP, 20)
	newH, newEntry := newProvenEntry(t, `{"id":"same-addr-new"}`, sharedIP, 8)
	pool.addEntry(oldH, oldEntry)
	pool.addEntry(newH, newEntry)
	pool.rebuildPlatformView(plat)

	placeLease(t, router, plat, "acct", oldH, oldEntry)
	if got := router.SnapshotIPLoad(plat.ID)[oldEntry.GetEgressIP()]; got != 1 {
		t.Fatalf("precondition: the shared address holds %d leases, want 1", got)
	}

	state := router.ensurePlatformState(plat.ID)
	next, _, ok := router.rebindLease(
		plat, state, "acct", "example.com", time.Now().UnixNano(),
		leaseOrFail(t, router, plat.ID, "acct"), nil,
	)
	if !ok {
		t.Fatal("expected the rebind to find the same-address node")
	}
	if next.NodeHash != newH || next.EgressIP != newEntry.GetEgressIP() {
		t.Fatalf("rebind landed on %v/%v, want %v/%v",
			next.NodeHash, next.EgressIP, newH, newEntry.GetEgressIP())
	}
	if got := router.SnapshotIPLoad(plat.ID)[newEntry.GetEgressIP()]; got != 1 {
		t.Fatalf("the shared address holds %d leases, want 1", got)
	}
}
