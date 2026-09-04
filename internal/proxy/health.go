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
