package routing

import (
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

var ErrNoAvailableNodes = errors.New("no available nodes")

// healthFilterAttempts bounds how many extra candidates are drawn when the
// picked one is unhealthy, so a fleet of bad nodes cannot turn one pick into an
// unbounded scan.
const healthFilterAttempts = 3

// healthyScanCap bounds the view scan that looks for an acceptable candidate
// when the random draws all landed on unhealthy nodes.
//
// The quick filter above draws a handful of extra candidates, which is enough
// while the pool is mostly healthy. In a pool where most nodes are failing,
// those draws keep landing on rejected nodes even though acceptable ones exist
// — the request then gets a node the filter already said no to. A bounded scan
// of the view fixes that without turning selection into a full sort: the cap
// keeps the cost constant, and the scan only runs when the quick filter came
// back empty-handed, so a healthy fleet never pays for it.
const healthyScanCap = 64

// HealthWeights controls how strongly node health affects routing.
// A zero value disables health entirely, leaving scoring as it was before.
type HealthWeights struct {
	// PenaltyNs is added to a candidate's score per unit of unhealthiness, in
	// nanoseconds — scores are built from time.Duration, so the configured
	// milliseconds must be converted before being stored here. A node at 0.5
	// health pays half of this.
	//
	// It is additive rather than multiplicative on purpose: an all-healthy
	// fleet then scores exactly as it did before health existed, and the
	// penalty stays independent of the platform's allocation policy (which may
	// return 0, making a multiplier ineffective).
	PenaltyNs float64
	// FilterThresholdPercent rejects candidates at or below this health, as a
	// percentage. 0 disables filtering.
	FilterThresholdPercent int
	// MinSamplesForFilter gates filtering: a node is never filtered before it
	// has this many observations, so a fresh node is not dropped on noise.
	MinSamplesForFilter int
	// ProbeQuarantineEnabled rejects nodes that consecutive active probes could
	// not reach. Unlike the health filter it needs no samples and no score: a
	// node that cannot answer a probe at all is not a candidate, regardless of
	// what its last successful requests say. Exposed as a switch so the
	// behaviour can be turned off without redeploying.
	ProbeQuarantineEnabled bool
}

// allows reports whether a node is healthy enough to be a routing candidate.
// Nodes with too few observations are always allowed through.
func (w HealthWeights) allows(pool PoolAccessor, h node.Hash) bool {
	if w.FilterThresholdPercent <= 0 || w.MinSamplesForFilter <= 0 {
		return true
	}
	entry, ok := pool.GetEntry(h)
	if !ok || entry == nil {
		return true
	}
	// A parked node is out regardless of its sample count: the quarantine is a
	// targeted response to repeated zero-ingress tunnels, not a health reading,
	// so the "too few observations" exemption must not reopen it.
	if entry.NoIngressQuarantined(time.Now()) {
		return false
	}
	if w.ProbeQuarantineEnabled && entry.ProbeQuarantined(time.Now()) {
		return false
	}
	if entry.HealthSamples() < uint32(w.MinSamplesForFilter) {
		return true
	}
	return entry.HealthScore()*100 >= float64(w.FilterThresholdPercent)
}

// rejects is the inverse of allows for nodes whose health is actually known.
//
// Unlike !allows it stays false when the score cannot be read or is not backed
// by enough observations: "not yet measured" must never be read as "bad", so
// the two answers agree on the only case that matters — a node is rejected
// only once its own track record says so. Unmeasured nodes are held back by
// the scoring penalty instead.
func (w HealthWeights) rejects(pool PoolAccessor, h node.Hash) bool {
	if w.FilterThresholdPercent <= 0 || w.MinSamplesForFilter <= 0 {
		return false
	}
	entry, ok := pool.GetEntry(h)
	if !ok || entry == nil {
		return false
	}
	if entry.NoIngressQuarantined(time.Now()) {
		return true
	}
	if w.ProbeQuarantineEnabled && entry.ProbeQuarantined(time.Now()) {
		return true
	}
	if entry.HealthSamples() < uint32(w.MinSamplesForFilter) {
		return false
	}
	return entry.HealthScore()*100 < float64(w.FilterThresholdPercent)
}

// healthPenalty turns a node's health into a score penalty in nanoseconds.
//
// A node with too few observations starts at a perfect 1.0, so on score alone it
// ties with a node that has served thousands of successful requests. Paying for
// the doubt fixes that: the taper charges the full penalty at zero observations
// and nothing once the score is backed by enough evidence, which is the same
// line HealthWeights.allows draws for filtering — the two can then never
// disagree about what "not yet measured" means.
//
// The penalty is the larger of the two rather than their sum, so a node that has
// already proven itself bad is not charged twice for the same doubt.
func healthPenalty(entry *node.NodeEntry, w HealthWeights) float64 {
	if entry == nil || w.PenaltyNs <= 0 {
		return 0
	}
	penalty := (1 - entry.HealthScore()) * w.PenaltyNs
	if w.MinSamplesForFilter > 0 {
		if samples := entry.HealthSamples(); samples < uint32(w.MinSamplesForFilter) {
			ramp := 1 - float64(samples)/float64(w.MinSamplesForFilter)
			if unknown := ramp * w.PenaltyNs; unknown > penalty {
				penalty = unknown
			}
		}
	}
	return penalty
}

var randomRouteRNGPool = sync.Pool{
	New: func() any {
		return rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	},
}

// randomRoute selects a routable node using P2C with latency/load scoring.
// It intentionally trusts Platform.View as the routable source of truth and
// does not do extra pool scans/availability validation on the hot path.
// Post-pick race handling (node removed right after selection) is handled by
// the caller in RouteRequest.
// excludePickAttempts bounds how many candidates are drawn when avoiding nodes
// this request has already failed on.
const excludePickAttempts = 8

func randomRoute(
	plat *platform.Platform,
	stats *IPLoadStats,
	pool PoolAccessor,
	targetDomain string,
	authorities []string,
	p2cWindow time.Duration,
	health HealthWeights,
	exclude []node.Hash,
) (node.Hash, error) {
	view := plat.View()
	size := view.Size()
	if size == 0 {
		return node.Zero, ErrNoAvailableNodes
	}

	isExcluded := func(h node.Hash) bool {
		for _, e := range exclude {
			if e == h {
				return true
			}
		}
		return false
	}

	rng := randomRouteRNGPool.Get().(*rand.Rand)
	defer randomRouteRNGPool.Put(rng)

	// Skip nodes this request already failed on. Random draws are used first to
	// keep the choice unbiased; if they keep landing on excluded nodes the scan
	// below guarantees routing still succeeds. Without it, a view where most
	// nodes are excluded could fail a request that does have a usable node.
	pick := func() (node.Hash, bool) {
		for i := 0; i < excludePickAttempts; i++ {
			h, ok := view.RandomPick(rng)
			if !ok {
				return node.Zero, false
			}
			if !isExcluded(h) {
				return h, true
			}
		}

		var fallback node.Hash
		found := false
		view.Range(func(h node.Hash) bool {
			if isExcluded(h) {
				return true
			}
			fallback = h
			found = true
			return false
		})
		if !found {
			return node.Zero, false
		}
		return fallback, true
	}

	// Health filtering is best-effort: if no healthy candidate turns up, the
	// original pick is kept. Routing must never fail because of health.
	filter := func(h node.Hash, chosen []node.Hash) node.Hash {
		if health.allows(pool, h) {
			return h
		}
		for i := 0; i < healthFilterAttempts; i++ {
			candidate, ok := pick()
			if !ok {
				break
			}
			if candidate == h || containsHash(chosen, candidate) {
				continue
			}
			if health.allows(pool, candidate) {
				return candidate
			}
		}
		return h
	}

	// Pick 1st candidate.
	h1, ok1 := pick()
	if !ok1 {
		return node.Zero, ErrNoAvailableNodes
	}
	// With a single node there is nothing to swap in, so skip filtering
	// entirely rather than drawing candidates that can only be that node.
	if size == 1 {
		return h1, nil
	}

	h1 = filter(h1, nil)

	// Draw the rest of the field. The width adapts to how much of the pool the
	// health filter accepts: two-choice is right for a healthy fleet, but in a
	// pool where most nodes are failing, a third and fourth draw is what turns
	// "one of two bad nodes" into a decent chance of a good one.
	chosen := []node.Hash{h1}
	width := choiceWidth(view, pool, health, rng)
	if width > size {
		width = size
	}
	for len(chosen) < width {
		// Best-effort distinctness: a view that keeps handing back nodes
		// already in the field is simply narrower than requested.
		var candidate node.Hash
		found := false
		for i := 0; i < excludePickAttempts; i++ {
			h, ok := pick()
			if !ok {
				break
			}
			if !containsHash(chosen, h) {
				candidate = h
				found = true
				break
			}
		}
		if !found {
			break
		}
		chosen = append(chosen, filter(candidate, chosen))
	}

	// The quick filter draws a few extra candidates and gives up quickly. When
	// any pick is still rejected the pool is mostly unhealthy, and random draws
	// are a bad way to find the acceptable minority: scan the view instead,
	// bounded, and compare among what turns up. If nothing acceptable exists at
	// all, fall through and compare the rejected field as before — health must
	// never turn a routable pool into a failed request.
	for _, h := range chosen {
		if health.allows(pool, h) {
			continue
		}
		if candidates := healthyCandidateScan(view, pool, exclude, health); len(candidates) > 0 {
			return pickStrictCandidate(
				candidates, rng, plat, stats, pool, targetDomain, authorities, p2cWindow, health,
			), nil
		}
		break
	}

	// Lower score is better. The field is reduced pairwise so every comparison
	// keeps the three-level latency lookup (and its per-node fallback) that the
	// two-choice path always used. Ties go to the later candidate, as before.
	best := chosen[0]
	for _, candidate := range chosen[1:] {
		latBest, latCandidate := compareLatencies(
			best, candidate, pool, targetDomain, authorities, p2cWindow,
		)
		bestScore := calculateScore(best, latBest, plat, stats, pool, health)
		candidateScore := calculateScore(candidate, latCandidate, plat, stats, pool, health)
		if candidateScore <= bestScore {
			best = candidate
		}
	}
	return best, nil
}

// Choice width for one pick. Classic power-of-two-choices is two; a pool where
// most nodes are rejected is exactly where extra draws pay off.
const (
	p2cMinChoices = 2
	p2cMaxChoices = 4
	// choiceSampleCap bounds the nodes inspected to estimate how much of the
	// pool the health filter accepts. Sizing the width must not turn every
	// request into a full view scan.
	choiceSampleCap = 32
	// choiceWidenPercent and choiceWidenMorePercent are the acceptable-share
	// cut points: below the former the width steps to 3, below the latter to 4.
	choiceWidenPercent     = 60
	choiceWidenMorePercent = 35
)

// choiceWidth reports how many distinct candidates one pick should draw.
//
// It costs a bounded sample (at most choiceSampleCap health checks) and is
// skipped entirely when health weighting is switched off, where there is no
// signal to size the width from.
func choiceWidth(
	view platform.ReadOnlyView,
	pool PoolAccessor,
	health HealthWeights,
	rng *rand.Rand,
) int {
	if health.FilterThresholdPercent <= 0 || health.MinSamplesForFilter <= 0 {
		return p2cMinChoices
	}
	size := view.Size()
	if size <= p2cMinChoices {
		return p2cMinChoices
	}

	sampled, accepted := 0, 0
	if size <= choiceSampleCap {
		view.Range(func(h node.Hash) bool {
			sampled++
			if health.allows(pool, h) {
				accepted++
			}
			return true
		})
	} else {
		seen := make(map[node.Hash]struct{}, choiceSampleCap)
		// The attempt bound keeps a view that keeps returning the same few
		// nodes from turning the estimate into an open-ended loop.
		for attempts := 0; sampled < choiceSampleCap && attempts < choiceSampleCap*4; attempts++ {
			h, ok := view.RandomPick(rng)
			if !ok {
				break
			}
			if _, dup := seen[h]; dup {
				continue
			}
			seen[h] = struct{}{}
			sampled++
			if health.allows(pool, h) {
				accepted++
			}
		}
	}
	if sampled == 0 {
		return p2cMinChoices
	}

	switch percent := accepted * 100 / sampled; {
	case percent < choiceWidenMorePercent:
		return p2cMaxChoices
	case percent < choiceWidenPercent:
		return 3
	default:
		return p2cMinChoices
	}
}

// healthyCandidateScan collects up to healthyScanCap routable, non-excluded
// candidates that the health filter accepts, scanning the platform view in
// shard order.
//
// It is the fallback for when random draws keep landing on rejected nodes: the
// cap keeps the cost constant no matter how large the view is, and stopping
// early leaves a subset of the acceptable nodes — the same trade-off
// strictCandidates already makes for rebinds.
func healthyCandidateScan(
	view platform.ReadOnlyView,
	pool PoolAccessor,
	exclude []node.Hash,
	health HealthWeights,
) []node.Hash {
	candidates := make([]node.Hash, 0, 16)
	view.Range(func(h node.Hash) bool {
		if containsHash(exclude, h) {
			return true
		}
		if !health.allows(pool, h) {
			return true
		}
		candidates = append(candidates, h)
		return len(candidates) < healthyScanCap
	})
	return candidates
}

// compareLatencies determines the latency values for h1 and h2.
// Implements the comparison logic:
// 1. Target domain present in both and recent.
// 2. Common authority domains present in both and recent.
// 3. Per-node fallback: each side keeps whatever it measured on its own.
//
// The third step is not "fall back to zero for both", which is what it used to
// be. A user target is almost never probed on two nodes at once, so the pair
// almost always reached step 3; returning (0, 0) there made the score ignore
// latency entirely and pick by lease count. Keeping a one-sided measurement
// lets the measured node win on merit, and leaves a genuine 0 only where the
// node really has nothing.
func compareLatencies(
	h1, h2 node.Hash,
	pool PoolAccessor,
	target string,
	authorities []string,
	window time.Duration,
) (time.Duration, time.Duration) {
	e1, ok1 := pool.GetEntry(h1)
	e2, ok2 := pool.GetEntry(h2)
	if !ok1 || !ok2 || e1.LatencyTable == nil || e2.LatencyTable == nil {
		return 0, 0
	}

	now := time.Now()

	// 1. Target domain check.
	// target can be empty if extracted domain is invalid/empty, handle gracefully.
	lat1, ok1 := lookupRecentDomainLatency(e1, target, now, window)
	lat2, ok2 := lookupRecentDomainLatency(e2, target, now, window)
	if ok1 && ok2 {
		return lat1, lat2
	}

	// 2. Authority intersection check.
	lat1, lat2, ok := averageComparableAuthorityLatencies(e1, e2, authorities, now, window)
	if ok {
		return lat1, lat2
	}

	// 3. Per-node fallback.
	lat1, _ = nodeLatency(e1, target, authorities, now, window)
	lat2, _ = nodeLatency(e2, target, authorities, now, window)
	return lat1, lat2
}

func isRecent(t time.Time, now time.Time, window time.Duration) bool {
	return now.Sub(t) <= window
}

// coldStartLatency stands in for the latency of a node that has no usable
// measurement for this target.
//
// Scoring zero for "unknown" was wrong in the same way as comparing an unknown
// latency against a measured one: a zero beat every measured node, so the
// least-known node won and the latency policies stopped meaning anything. A
// fixed, moderate estimate keeps unmeasured nodes in the running without
// handing them the pick; the health penalty then charges them for the
// remaining doubt, and the sample-count gate keeps fresh nodes from being
// filtered outright.
const coldStartLatency = 400 * time.Millisecond

// effectiveLatency maps "no measurement" onto a comparable estimate.
func effectiveLatency(latency time.Duration) time.Duration {
	if latency > 0 {
		return latency
	}
	return coldStartLatency
}

// calculateScore computes the score for a node based on platform allocation policy.
// Lower is better.
func calculateScore(
	h node.Hash,
	latency time.Duration,
	plat *platform.Platform,
	stats *IPLoadStats,
	pool PoolAccessor,
	health HealthWeights,
) float64 {
	entry, _ := pool.GetEntry(h)
	// If entry is nil (race), treat as high load/latency?
	// But we hold ref via pool, only deletion removes it.
	// Assuming existence since we just picked it from view.

	// Lease count from stats.
	var leaseCount int64
	if entry != nil {
		ip := entry.GetEgressIP()
		if ip.IsValid() {
			leaseCount = stats.Get(ip)
		}
	}

	var score float64
	switch plat.AllocationPolicy {
	case platform.AllocationPolicyPreferLowLatency:
		score = float64(effectiveLatency(latency))
	case platform.AllocationPolicyPreferIdleIP:
		// This policy is about spreading leases, not latency, so an unknown
		// latency is not a reason to score the node differently.
		score = float64(leaseCount)
	case platform.AllocationPolicyBalanced:
		fallthrough
	default:
		// (LeaseCount + 1) * Latency
		score = float64(leaseCount+1) * float64(effectiveLatency(latency))
	}

	// Health penalty, applied the same way under every policy. A fully healthy
	// node adds nothing, so an all-healthy fleet is unaffected.
	score += healthPenalty(entry, health)
	return score
}
