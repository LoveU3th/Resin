package service

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/topology"
)

func newSummaryTestService(t *testing.T, minSamples int) (*ControlPlaneService, func()) {
	t.Helper()

	pool := newNodeListTestPool(newNodeListTestSubMgr(t))
	cfg := &config.RuntimeConfig{HealthMinSamplesForFilter: minSamples}
	var cfgPtr atomic.Pointer[config.RuntimeConfig]
	cfgPtr.Store(cfg)

	svc := &ControlPlaneService{Pool: pool, RuntimeCfg: &cfgPtr}
	return svc, func() {}
}

// A node with too few observations scores 1.0 under the EWMA. Showing that would
// present "not measured yet" as "perfectly healthy", so the score is withheld
// until it is backed by enough samples.
func TestNodeSummary_SuccessRateWithheldUntilMeasured(t *testing.T) {
	svc, cleanup := newSummaryTestService(t, 8)
	defer cleanup()

	hash := node.Hash{7}
	entry := node.NewNodeEntry(hash, []byte(`{"type":"stub"}`), time.Now(), 16)

	summary := svc.nodeEntryToSummary(hash, entry)

	if summary.HealthSamples != 0 {
		t.Fatalf("samples: got %d, want 0", summary.HealthSamples)
	}
	if summary.SuccessRate != nil {
		t.Fatalf("unmeasured node must not report a success rate, got %v", *summary.SuccessRate)
	}
}

// Once enough observations exist the score is exposed, and it reflects failures.
func TestNodeSummary_SuccessRateExposedWhenMeasured(t *testing.T) {
	svc, cleanup := newSummaryTestService(t, 4)
	defer cleanup()

	hash := node.Hash{7}
	entry := node.NewNodeEntry(hash, []byte(`{"type":"stub"}`), time.Now(), 16)
	for i := 0; i < 10; i++ {
		entry.RecordHealthSample(i%2 == 0, 1, 0, 0)
	}

	summary := svc.nodeEntryToSummary(hash, entry)

	if summary.HealthSamples != 10 {
		t.Fatalf("samples: got %d, want 10", summary.HealthSamples)
	}
	if summary.SuccessRate == nil {
		t.Fatal("expected a success rate once the sample count is reached")
	}
	// Half the samples failed, so the score must be mid-range, not 1.0.
	if rate := *summary.SuccessRate; rate <= 0.1 || rate >= 0.9 {
		t.Fatalf("success rate %v does not reflect a 50%% outcome", rate)
	}
}

// The threshold comes from runtime config, so it can be tuned without a rebuild.
func TestHealthMinSamples_ReadsRuntimeConfig(t *testing.T) {
	svc, cleanup := newSummaryTestService(t, 12)
	defer cleanup()

	if got := healthMinSamples(svc); got != 12 {
		t.Fatalf("healthMinSamples: got %d, want 12", got)
	}

	// A service without runtime config must not panic.
	var bare *ControlPlaneService
	if got := healthMinSamples(bare); got != 0 {
		t.Fatalf("healthMinSamples(nil): got %d, want 0", got)
	}
}

func newNodeListTestSubMgr(t *testing.T) *topology.SubscriptionManager {
	t.Helper()
	return topology.NewSubscriptionManager()
}

// A threshold of zero means "no threshold", so the score is always shown. It
// must not be read as "hide the score entirely".
func TestNodeSummary_ZeroThresholdAlwaysShows(t *testing.T) {
	svc, cleanup := newSummaryTestService(t, 0)
	defer cleanup()

	entry := node.NewNodeEntry(node.Hash{9}, []byte(`{"type":"stub"}`), time.Now(), 16)

	summary := svc.nodeEntryToSummary(node.Hash{9}, entry)

	if summary.SuccessRate == nil {
		t.Fatal("with no threshold the score must be shown even when unmeasured")
	}
}
