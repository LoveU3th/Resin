package proxy

import (
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// HealthRecorder abstracts passive health feedback reporting.
// topology.GlobalNodePool satisfies this interface.
//
// RecordLatency is reserved for proactive probes: it advances the node's
// probe-attempt timestamps. Traffic-path latency samples must go through
// RecordPassiveLatency, which leaves probe scheduling untouched.
type HealthRecorder interface {
	RecordResult(hash node.Hash, success bool)
	RecordLatency(hash node.Hash, rawTarget string, latency *time.Duration)
	RecordPassiveLatency(hash node.Hash, rawTarget string, latency *time.Duration)
}

type passiveHealthRecorder interface {
	RecordPassiveResult(platformID string, hash node.Hash, success bool)
}

// stagedPassiveHealthRecorder is implemented by recorders that can tell which
// phase of the request a result came from. Optional: callers fall back to
// RecordPassiveResult when it is absent.
type stagedPassiveHealthRecorder interface {
	RecordPassiveStageResult(platformID string, hash node.Hash, stage string, success bool)
}

// slowPassiveHealthRecorder records a failure that means "the origin was slow"
// rather than "the node is broken": the node was reached and answered, just not
// fast enough. Optional, like the staged one.
//
// It exists because such a failure must not feed the breaker. A node that is
// merely slow is still serving traffic, and evicting it would remove capacity
// that works. It does lower the health score, so a node that is genuinely slow
// still loses selection weight.
type slowPassiveHealthRecorder interface {
	RecordPassiveSlowFailure(platformID string, hash node.Hash)
}

func recordPassiveResultAsync(health HealthRecorder, route routing.RouteResult, success bool) {
	recordPassiveStageResultAsync(health, route, node.PassiveStageConnect, success)
}

// recordPassiveStageResultAsync reports a result for one request phase.
// Connect-phase failures say the node was unreachable; transfer-phase failures
// Connect-phase failures say the node was unreachable; transfer-phase failures
// say it was reached, so recorders that distinguish them weigh the latter less.
func recordPassiveStageResultAsync(
	health HealthRecorder,
	route routing.RouteResult,
	stage string,
	success bool,
) {
	if health == nil {
		return
	}
	if recorder, ok := health.(stagedPassiveHealthRecorder); ok {
		go recorder.RecordPassiveStageResult(route.PlatformID, route.NodeHash, stage, success)
		return
	}
	if recorder, ok := health.(passiveHealthRecorder); ok {
		go recorder.RecordPassiveResult(route.PlatformID, route.NodeHash, success)
		return
	}
	go health.RecordResult(route.NodeHash, success)
}

// connDropRecorder is implemented by recorders that accept a "the pooled
// connection was dead" signal.
type connDropRecorder interface {
	RecordConnDrop(platformID string, hash node.Hash)
}

// recordConnDropAsync reports a dead pooled connection.
//
// This is a weight-down signal, not a failure: the node was reached before, we
// simply found a connection the peer had already dropped without telling us.
// Several idle connections can expire together and produce a burst of these, so
// letting them trip the breaker would evict a node that is probably fine.
func recordConnDropAsync(health HealthRecorder, route routing.RouteResult) {
	if health == nil {
		return
	}
	if recorder, ok := health.(connDropRecorder); ok {
		go recorder.RecordConnDrop(route.PlatformID, route.NodeHash)
	}
}

// recordPassiveSlowFailureAsync reports a failure attributable to a slow origin
// rather than a broken node.
//
// The distinction matters for the breaker: this lowers the health score but
// deliberately does not count toward eviction. Reporting it as an ordinary
// failure would eventually eject a node that is working, which is the opposite
// of what the breaker is for.
func recordPassiveSlowFailureAsync(health HealthRecorder, route routing.RouteResult) {
	if health == nil {
		return
	}
	if recorder, ok := health.(slowPassiveHealthRecorder); ok {
		go recorder.RecordPassiveSlowFailure(route.PlatformID, route.NodeHash)
		return
	}
	// Recorders without the distinction cannot tell slow from broken, so fall
	// back to the weaker transfer-stage failure: it is the closest match.
	recordPassiveStageResultAsync(health, route, node.PassiveStageTransfer, false)
}
