package routing

import (
	"math/rand/v2"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

// strictCandidateScanCap bounds how many eligible candidates are collected for
// one strict pick. Rebinds are rare, but a very large platform would otherwise
// allocate a slice the size of its whole view on every one of them. Hitting the
// cap stops the scan early, so the collected set is a subset of the eligible
// nodes rather than all of them; within that subset the choice is still made by
// the same two-choice comparison, so the truncation does not bias the pick
// toward any particular node.
const strictCandidateScanCap = 256

// StrictPolicy describes the healthy-first candidate set a sticky lease is
// rebound into.
//
// It exists because the ordinary health filter cannot express this: HealthWeights
// deliberately lets a node with too few observations through, so that a fresh
// node is not dropped on noise. A rebind needs the opposite answer — a node
// that was never measured cannot be called healthy — and that difference is the
// whole point of this type rather than a parameter of HealthWeights.
type StrictPolicy struct {
	// ThresholdPercent is the health a node must reach to be a candidate, as a
	// percentage. 0 accepts any measured node.
	ThresholdPercent int
	// FallbackPercent is the second tier, consulted only when the first yields
	// nothing. 0 means "no second tier", not "accept anything": with the tier
	// disabled the search goes straight to the unmeasured-tolerant last resort.
	FallbackPercent int
	// MinSamples is how many observations a node needs before its score is
	// trusted. Below it the node is not a candidate at any tier, which is what
	// keeps never-measured nodes from being handed a rebind.
	MinSamples int
}

// strictRoute selects a node for a rebind, preferring health over everything
// else and refusing to hand back the node the request just failed on.
//
// Unlike randomRoute this never falls back to the node it was asked to avoid:
// the tiers narrow what is acceptable, they never widen it back to the failed
// node. Only when no routable node is left at all does it fail, and the caller
// keeps the existing lease in that case.
func strictRoute(
	plat *platform.Platform,
	stats *IPLoadStats,
	pool PoolAccessor,
	targetDomain string,
	authorities []string,
	p2cWindow time.Duration,
	health HealthWeights,
	strict StrictPolicy,
	exclude []node.Hash,
) (node.Hash, *node.NodeEntry, error) {
	view := plat.View()
	if view.Size() == 0 {
		return node.Zero, nil, ErrNoAvailableNodes
	}

	rng := randomRouteRNGPool.Get().(*rand.Rand)
	defer randomRouteRNGPool.Put(rng)

	// Tiers, healthiest first. The fallback tier is skipped when it is not
	// configured or would repeat the primary one, so a pool that already
	// answered the first scan is not scanned twice for the same answer.
	tiers := [][2]int{{strict.ThresholdPercent, strict.MinSamples}}
	if strict.FallbackPercent > 0 && strict.FallbackPercent < strict.ThresholdPercent {
		tiers = append(tiers, [2]int{strict.FallbackPercent, strict.MinSamples})
	}
	// Last resort: anything routable except the failed node. It deliberately
	// drops the sample requirement — a pool that has not been measured yet
	// (every node is unmeasured right after a restart) must still be able to
	// move a lease off a node that just failed, or nothing would ever move.
	tiers = append(tiers, [2]int{0, 0})

	for _, tier := range tiers {
		candidates := strictCandidates(view, pool, exclude, tier[0], tier[1])
		if len(candidates) == 0 {
			continue
		}
		h := pickStrictCandidate(
			candidates, rng, plat, stats, pool, targetDomain, authorities, p2cWindow, health,
		)
		entry, ok := pool.GetEntry(h)
		if !ok || entry == nil {
			// The node was dropped between the scan and the pick. One retry is
			// enough: the caller treats a missing entry as "no node available"
			// rather than looping on a fleet that is actively shrinking.
			continue
		}
		return h, entry, nil
	}

	return node.Zero, nil, ErrNoAvailableNodes
}

// strictEligible reports whether a node meets one tier: enough observations to
// trust the score, and a score of at least minPercent. minSamples 0 and
// minPercent 0 accept every node, which is how the last-resort tier is
// expressed.
//
// The sample requirement is the part that differs from HealthWeights.allows:
// there, too few observations means "let it through, it is not fair to judge
// yet"; here it means "no evidence, not eligible". A rebind is the one place
// where the absence of a track record must not be read as a good one.
func strictEligible(entry *node.NodeEntry, minPercent int, minSamples int) bool {
	if minSamples > 0 && entry.HealthSamples() < uint32(minSamples) {
		return false
	}
	return entry.HealthScore()*100 >= float64(minPercent)
}

// strictCandidates collects up to strictCandidateScanCap routable nodes that
// meet one tier.
func strictCandidates(
	view platform.ReadOnlyView,
	pool PoolAccessor,
	exclude []node.Hash,
	minPercent int,
	minSamples int,
) []node.Hash {
	candidates := make([]node.Hash, 0, 16)
	view.Range(func(h node.Hash) bool {
		if containsHash(exclude, h) {
			return true
		}
		entry, ok := pool.GetEntry(h)
		if !ok || entry == nil {
			return true
		}
		if !strictEligible(entry, minPercent, minSamples) {
			return true
		}
		candidates = append(candidates, h)
		return len(candidates) < strictCandidateScanCap
	})
	return candidates
}

// pickStrictCandidate applies the same power-of-two-choices comparison the
// ordinary path uses, so a rebind prefers the better of two eligible nodes
// rather than taking the luck of a single draw. Lower score wins.
func pickStrictCandidate(
	candidates []node.Hash,
	rng *rand.Rand,
	plat *platform.Platform,
	stats *IPLoadStats,
	pool PoolAccessor,
	targetDomain string,
	authorities []string,
	p2cWindow time.Duration,
	health HealthWeights,
) node.Hash {
	if len(candidates) == 1 {
		return candidates[0]
	}

	first := rng.IntN(len(candidates))
	second := rng.IntN(len(candidates) - 1)
	if second >= first {
		second++
	}

	h1, h2 := candidates[first], candidates[second]
	lat1, lat2 := compareLatencies(h1, h2, pool, targetDomain, authorities, p2cWindow)
	s1 := calculateScore(h1, lat1, plat, stats, pool, health)
	s2 := calculateScore(h2, lat2, plat, stats, pool, health)
	if s1 < s2 {
		return h1
	}
	return h2
}

// strictEligibility adapts a StrictPolicy into the predicate
// chooseSameIPRotationCandidate expects, at the policy's primary threshold.
//
// The fallback tier is deliberately not consulted here: staying on the same
// egress IP is a bonus, not a requirement, so it must not be able to widen the
// pool beyond what the primary threshold allows. When no same-IP node qualifies
// the caller falls through to strictRoute, which applies the full tiering.
func strictEligibility(strict StrictPolicy) func(*node.NodeEntry) bool {
	if strict.MinSamples <= 0 && strict.ThresholdPercent <= 0 {
		return nil
	}
	return func(entry *node.NodeEntry) bool {
		return strictEligible(entry, strict.ThresholdPercent, strict.MinSamples)
	}
}

// selectLiveStrictRoute resolves a rebind target using the router's current
// tuning. It is a thin wrapper so the rebind path reads as one call instead of
// six arguments of plumbing.
func (r *Router) selectLiveStrictRoute(
	plat *platform.Platform,
	stats *IPLoadStats,
	targetDomain string,
	exclude []node.Hash,
) (node.Hash, *node.NodeEntry, error) {
	return strictRoute(
		plat,
		stats,
		r.pool,
		targetDomain,
		r.authorities(),
		r.p2cWindow(),
		r.healthWeights(),
		r.stickyStrict(),
		exclude,
	)
}
