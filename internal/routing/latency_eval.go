package routing

import (
	"time"

	"github.com/Resinat/Resin/internal/node"
)

func lookupRecentDomainLatency(
	entry *node.NodeEntry,
	domain string,
	now time.Time,
	window time.Duration,
) (time.Duration, bool) {
	if entry == nil || entry.LatencyTable == nil || domain == "" {
		return 0, false
	}
	stats, ok := entry.LatencyTable.GetDomainStats(domain)
	if !ok || !isRecent(stats.LastUpdated, now, window) {
		return 0, false
	}
	return stats.Ewma, true
}

func averageRecentAuthorityLatency(
	entry *node.NodeEntry,
	authorities []string,
	now time.Time,
	window time.Duration,
) (time.Duration, bool) {
	if entry == nil || entry.LatencyTable == nil || len(authorities) == 0 {
		return 0, false
	}
	var sum time.Duration
	var count int64
	for _, domain := range authorities {
		latency, ok := lookupRecentDomainLatency(entry, domain, now, window)
		if !ok {
			continue
		}
		sum += latency
		count++
	}
	if count == 0 {
		return 0, false
	}
	return time.Duration(int64(sum) / count), true
}

// nodeLatency is a single node's best available latency estimate for a target:
// the target's own sample when recent, otherwise the average over the probe
// authorities this node has measured.
//
// It exists so a node that has measurements is not treated as "unknown" just
// because the node it is compared against happens to share no sample with it.
// Without it, a pair where only one side was measured compares as (0, 0) and
// the routing decision falls back to lease counts alone.
func nodeLatency(
	entry *node.NodeEntry,
	target string,
	authorities []string,
	now time.Time,
	window time.Duration,
) (time.Duration, bool) {
	if latency, ok := lookupRecentDomainLatency(entry, target, now, window); ok {
		return latency, true
	}
	return averageRecentAuthorityLatency(entry, authorities, now, window)
}

func averageComparableAuthorityLatencies(
	e1, e2 *node.NodeEntry,
	authorities []string,
	now time.Time,
	window time.Duration,
) (time.Duration, time.Duration, bool) {
	if e1 == nil || e2 == nil || e1.LatencyTable == nil || e2.LatencyTable == nil || len(authorities) == 0 {
		return 0, 0, false
	}
	var sum1, sum2 time.Duration
	var count int64
	for _, domain := range authorities {
		latency1, ok1 := lookupRecentDomainLatency(e1, domain, now, window)
		latency2, ok2 := lookupRecentDomainLatency(e2, domain, now, window)
		if !ok1 || !ok2 {
			continue
		}
		sum1 += latency1
		sum2 += latency2
		count++
	}
	if count == 0 {
		return 0, 0, false
	}
	return time.Duration(int64(sum1) / count), time.Duration(int64(sum2) / count), true
}
