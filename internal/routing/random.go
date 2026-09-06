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
	filter := func(h node.Hash, exclude node.Hash) node.Hash {
		if health.allows(pool, h) {
			return h
		}
		for i := 0; i < healthFilterAttempts; i++ {
			candidate, ok := pick()
			if !ok {
				break
			}
			if candidate == exclude || candidate == h {
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

	h1 = filter(h1, node.Zero)

	// Pick 2nd candidate; best-effort to make it distinct.
	h2, ok2 := pick()
	if !ok2 {
		return h1, nil
	}
	if h2 == h1 {
		for i := 0; i < 3; i++ {
			candidate, ok := pick()
			if !ok {
				break
			}
			if candidate != h1 {
				h2 = candidate
				break
			}
		}
		if h2 == h1 {
			return h1, nil
		}
	}
	h2 = filter(h2, h1)

	// Determine effective latency for comparison.
	lat1, lat2 := compareLatencies(h1, h2, pool, targetDomain, authorities, p2cWindow)

	// Calculate scores.
	s1 := calculateScore(h1, lat1, plat, stats, pool, health)
	s2 := calculateScore(h2, lat2, plat, stats, pool, health)

	// Lower score is better.
	selected := h2 // favor h2 on tie
	if s1 < s2 {
		selected = h1
	}
	return selected, nil
}

// compareLatencies determines the latency values for h1 and h2.
// Implements the 3-level comparison logic:
// 1. Target domain present in both and recent.
// 2. Common authority domains present in both and recent.
// 3. Fallback to 0 (empty) for both.
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

	// 3. Fallback.
	return 0, 0
}

func isRecent(t time.Time, now time.Time, window time.Duration) bool {
	return now.Sub(t) <= window
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
	if latency <= 0 {
		// If latency is 0 (empty/incompatible), score = LeaseCount strictly.
		score = float64(leaseCount)
	} else {
		// Policy-based scoring.
		switch plat.AllocationPolicy {
		case platform.AllocationPolicyPreferLowLatency:
			score = float64(latency)
		case platform.AllocationPolicyPreferIdleIP:
			score = float64(leaseCount)
		case platform.AllocationPolicyBalanced:
			fallthrough
		default:
			// (LeaseCount + 1) * Latency
			score = float64(leaseCount+1) * float64(latency)
		}
	}

	// Health penalty, applied the same way under every policy. A fully healthy
	// node adds nothing, so an all-healthy fleet is unaffected.
	score += healthPenalty(entry, health)
	return score
}
