package config

import (
	"testing"
	"time"
)

// A config persisted before the health-scoring fields existed reads them back
// as zero. The new behaviour must not stay switched off because of that.
func TestApplyCompatibilityDefaults_BackfillsNewFields(t *testing.T) {
	old := &RuntimeConfig{
		LatencyTestURL:       "https://cloudflare.com/cdn-cgi/trace",
		RequestLogTotalMaxMB: 200,
		CacheFlushInterval:   Duration(0),
	}

	out, changed := ApplyCompatibilityDefaults(old, nil)
	if !changed {
		t.Fatal("expected backfill to be reported")
	}
	if out.FailoverMaxAttempts != 2 {
		t.Fatalf("FailoverMaxAttempts: got %d, want 2", out.FailoverMaxAttempts)
	}
	if !out.FailoverEnabled {
		t.Fatal("failover must be enabled for a config that predates it")
	}
	if out.MaxConsecutiveFailures != 3 {
		t.Fatalf("MaxConsecutiveFailures: got %d, want 3", out.MaxConsecutiveFailures)
	}
	// These are the ones that used to be lost silently: a zero meant the
	// feature stayed off no matter what the defaults said.
	if out.HealthPenaltyMs != 2000 {
		t.Fatalf("HealthPenaltyMs: got %d, want 2000", out.HealthPenaltyMs)
	}
	if out.HealthFilterThresholdPercent != 40 {
		t.Fatalf("HealthFilterThresholdPercent: got %d, want 40", out.HealthFilterThresholdPercent)
	}
	if out.HealthTransferFailureWeightPercent != 50 {
		t.Fatalf("HealthTransferFailureWeightPercent: got %d, want 50", out.HealthTransferFailureWeightPercent)
	}
	if out.SchemaVersion != RuntimeConfigSchemaVersion {
		t.Fatalf("SchemaVersion: got %d, want %d", out.SchemaVersion, RuntimeConfigSchemaVersion)
	}

	// A config with the feature deliberately disabled must not be re-enabled.
	disabled := &RuntimeConfig{
		SchemaVersion:             RuntimeConfigSchemaVersion,
		RequestLogTotalMaxMB:      200,
		FailoverEnabled:           false,
		FailoverMaxAttempts:       1,
		MaxConsecutiveFailures:    5,
		HealthMinSamplesForFilter: 8,
	}
	out2, _ := ApplyCompatibilityDefaults(disabled, nil)
	if out2.FailoverEnabled {
		t.Fatal("an explicitly disabled failover must not be re-enabled")
	}
	if out2.MaxConsecutiveFailures != 5 {
		t.Fatalf("operator value overwritten: got %d, want 5", out2.MaxConsecutiveFailures)
	}

	// Zero-as-a-setting must survive a restart, not be reset to a default.
	// SchemaVersion says this config was written when the fields already
	// existed, so a zero here is the operator's choice, not a missing key.
	noCooldown := &RuntimeConfig{
		SchemaVersion:        RuntimeConfigSchemaVersion,
		RequestLogTotalMaxMB: 200,
		CircuitCooldown:      0,
		HealthPenaltyMs:      0,
		FailoverMaxAttempts:  2,
	}
	out3, _ := ApplyCompatibilityDefaults(noCooldown, nil)
	if out3.CircuitCooldown != 0 {
		t.Fatalf("CircuitCooldown=0 is a valid setting and must be preserved, got %v", out3.CircuitCooldown)
	}
	if out3.HealthPenaltyMs != 0 {
		t.Fatalf("HealthPenaltyMs=0 is a valid setting and must be preserved, got %v", out3.HealthPenaltyMs)
	}
}

// A config written before the sticky strict fields existed must get them: the
// point of the schema bump is that a missing key cannot leave the feature off
// silently.
func TestApplyCompatibilityDefaults_BackfillsStickyStrictFields(t *testing.T) {
	// Version 0 predates every versioned field.
	v0 := &RuntimeConfig{RequestLogTotalMaxMB: 200}
	out, changed := ApplyCompatibilityDefaults(v0, nil)
	if !changed {
		t.Fatal("expected a version 0 config to be migrated")
	}
	if !out.StickyStrictRebindEnabled {
		t.Fatal("sticky strict rebind must default to on")
	}
	if out.StickyStrictThresholdPercent != 85 {
		t.Fatalf("StickyStrictThresholdPercent: got %d, want 85", out.StickyStrictThresholdPercent)
	}
	if out.StickyStrictFallbackPercent != 70 {
		t.Fatalf("StickyStrictFallbackPercent: got %d, want 70", out.StickyStrictFallbackPercent)
	}
	if out.StickyStrictCooldown != Duration(5*time.Minute) {
		t.Fatalf("StickyStrictCooldown: got %v, want 5m0s", out.StickyStrictCooldown)
	}

	// A version 1 config knew the health and failover fields but not these, so
	// the v2 block runs and leaves everything the operator set alone.
	v1 := &RuntimeConfig{
		SchemaVersion:                1,
		RequestLogTotalMaxMB:         200,
		MaxConsecutiveFailures:       3,
		HealthPenaltyMs:              1500,
		HealthFilterThresholdPercent: 40,
		HealthMinSamplesForFilter:    8,
		FailoverEnabled:              true,
		FailoverMaxAttempts:          3,
		MaxLatencyTestInterval:       Duration(10 * time.Minute),
		StickyStrictThresholdPercent: 0,
		StickyStrictFallbackPercent:  0,
		StickyStrictRebindEnabled:    false,
		StickyStrictCooldown:         0,
	}
	out1, changed1 := ApplyCompatibilityDefaults(v1, nil)
	if !changed1 {
		t.Fatal("expected a version 1 config to be migrated")
	}
	if out1.SchemaVersion != RuntimeConfigSchemaVersion {
		t.Fatalf("SchemaVersion: got %d, want %d", out1.SchemaVersion, RuntimeConfigSchemaVersion)
	}
	if out1.StickyStrictThresholdPercent != 85 || !out1.StickyStrictRebindEnabled {
		t.Fatalf("v2 fields not backfilled: threshold %d, enabled %v",
			out1.StickyStrictThresholdPercent, out1.StickyStrictRebindEnabled)
	}
	if out1.HealthPenaltyMs != 1500 {
		t.Fatalf("v1 operator value overwritten: HealthPenaltyMs got %d, want 1500", out1.HealthPenaltyMs)
	}
	if out1.FailoverMaxAttempts != 3 {
		t.Fatalf("v1 operator value overwritten: FailoverMaxAttempts got %d, want 3", out1.FailoverMaxAttempts)
	}

	// At the current version the fields are the operator's, including a
	// deliberate switch-off and thresholds tuned away from the defaults.
	current := &RuntimeConfig{
		SchemaVersion:                RuntimeConfigSchemaVersion,
		RequestLogTotalMaxMB:         200,
		MaxConsecutiveFailures:       3,
		StickyStrictRebindEnabled:    false,
		StickyStrictThresholdPercent: 60,
		StickyStrictFallbackPercent:  30,
		StickyStrictCooldown:         Duration(time.Minute),
	}
	out2, changed2 := ApplyCompatibilityDefaults(current, nil)
	if changed2 {
		t.Fatal("a config at the current schema version must not be re-migrated")
	}
	if out2.StickyStrictRebindEnabled {
		t.Fatal("an explicitly disabled rebind must not be re-enabled")
	}
	if out2.StickyStrictThresholdPercent != 60 || out2.StickyStrictFallbackPercent != 30 {
		t.Fatalf("tuned thresholds overwritten: %d/%d, want 60/30",
			out2.StickyStrictThresholdPercent, out2.StickyStrictFallbackPercent)
	}
	if out2.StickyStrictCooldown != Duration(time.Minute) {
		t.Fatalf("StickyStrictCooldown: got %v, want the operator's 1m", out2.StickyStrictCooldown)
	}
}

// The fill-in must not run twice: once the config carries the schema version,
// an operator's zero survives every later restart and every unrelated patch.
func TestApplyCompatibilityDefaults_MigrationRunsOnce(t *testing.T) {
	old := &RuntimeConfig{RequestLogTotalMaxMB: 200}

	first, changed := ApplyCompatibilityDefaults(old, nil)
	if !changed {
		t.Fatal("expected the first pass to report a change")
	}
	// The operator turns the health penalty off after the upgrade.
	first.HealthPenaltyMs = 0

	second, changedAgain := ApplyCompatibilityDefaults(first, nil)
	if changedAgain {
		t.Fatal("a config already at the current schema version must not be re-migrated")
	}
	if second.HealthPenaltyMs != 0 {
		t.Fatalf("HealthPenaltyMs: got %d, want the operator's 0", second.HealthPenaltyMs)
	}
}

// An operator who has already set the values by hand keeps them. This is the
// case that matters in practice: the config was written by a release that knew
// the fields but not the version stamp, so every field is non-zero and nothing
// may be overwritten — including values deliberately tuned away from default.
func TestApplyCompatibilityDefaults_KeepsValuesSetByHand(t *testing.T) {
	byHand := &RuntimeConfig{
		RequestLogTotalMaxMB:               200,
		MaxConsecutiveFailures:             3,
		HealthEwmaWindow:                   20,
		HealthEwmaMinSamples:               5,
		HealthPenaltyMs:                    2000,
		HealthFilterThresholdPercent:       40,
		HealthMinSamplesForFilter:          8,
		CircuitCooldown:                    Duration(30 * time.Second),
		CircuitMaxCooldown:                 Duration(30 * time.Minute),
		HealthRecoveryFloorPercent:         60,
		HealthTransferFailureWeightPercent: 50,
		FailoverEnabled:                    true,
		FailoverMaxAttempts:                2,
		FailoverAttemptBudget:              Duration(60 * time.Second),
		FailoverTotalBudget:                Duration(90 * time.Second),
		// Tuned by hand: 10 minutes, not the 1h default.
		MaxLatencyTestInterval: Duration(10 * time.Minute),
	}

	out, _ := ApplyCompatibilityDefaults(byHand, nil)

	if out.MaxLatencyTestInterval != Duration(10*time.Minute) {
		t.Fatalf("MaxLatencyTestInterval: got %v, want the operator's 10m", out.MaxLatencyTestInterval)
	}
	if out.HealthPenaltyMs != 2000 {
		t.Fatalf("HealthPenaltyMs: got %d, want the operator's 2000", out.HealthPenaltyMs)
	}
	if out.HealthFilterThresholdPercent != 40 {
		t.Fatalf("HealthFilterThresholdPercent: got %d, want 40", out.HealthFilterThresholdPercent)
	}
	// The stamp is the only thing that changes, so the next restart leaves it
	// alone.
	if out.SchemaVersion != RuntimeConfigSchemaVersion {
		t.Fatalf("SchemaVersion: got %d, want %d", out.SchemaVersion, RuntimeConfigSchemaVersion)
	}
}
