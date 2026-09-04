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
