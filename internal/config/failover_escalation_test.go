package config

import (
	"testing"
	"time"
)

// The shipped default must enable the escalation: a pool where nodes fail to
// connect is exactly the case it exists for.
func TestDefaultRuntimeConfig_EnablesConnectEscalation(t *testing.T) {
	cfg := NewDefaultRuntimeConfig()
	if cfg.FailoverConnectExtraAttempts != 2 {
		t.Fatalf("FailoverConnectExtraAttempts: got %d, want 2", cfg.FailoverConnectExtraAttempts)
	}
}

// A config persisted before the field existed reads it back as zero, and a zero
// must not leave the feature silently off — the same reasoning the v1 and v2
// migration blocks apply.
func TestApplyCompatibilityDefaults_BackfillsConnectEscalation(t *testing.T) {
	v2 := &RuntimeConfig{
		SchemaVersion:                2,
		RequestLogTotalMaxMB:         200,
		FailoverEnabled:              true,
		FailoverMaxAttempts:          2,
		StickyStrictThresholdPercent: 85,
	}

	out, changed := ApplyCompatibilityDefaults(v2, nil)
	if !changed {
		t.Fatal("expected a v2 config to be migrated")
	}
	if out.FailoverConnectExtraAttempts != 2 {
		t.Fatalf("FailoverConnectExtraAttempts: got %d, want 2", out.FailoverConnectExtraAttempts)
	}
	if out.SchemaVersion != RuntimeConfigSchemaVersion {
		t.Fatalf("SchemaVersion: got %d, want %d", out.SchemaVersion, RuntimeConfigSchemaVersion)
	}

	// A version 0 config gets the same fill-in through the same block.
	v0 := &RuntimeConfig{RequestLogTotalMaxMB: 200}
	out0, _ := ApplyCompatibilityDefaults(v0, nil)
	if out0.FailoverConnectExtraAttempts != 2 {
		t.Fatalf("v0 config: got %d, want 2", out0.FailoverConnectExtraAttempts)
	}
}

// Once the config is at the current schema version the field is the operator's:
// zero (escalation off) and any other value must survive a restart.
func TestApplyCompatibilityDefaults_PreservesOperatorEscalationValue(t *testing.T) {
	off := &RuntimeConfig{
		SchemaVersion:                RuntimeConfigSchemaVersion,
		RequestLogTotalMaxMB:         200,
		MaxConsecutiveFailures:       3,
		FailoverEnabled:              true,
		FailoverMaxAttempts:          2,
		FailoverConnectExtraAttempts: 0,
	}
	out, changed := ApplyCompatibilityDefaults(off, nil)
	if changed {
		t.Fatal("a current-version config must not be rewritten")
	}
	if out.FailoverConnectExtraAttempts != 0 {
		t.Fatalf("explicitly disabled escalation overwritten: got %d, want 0",
			out.FailoverConnectExtraAttempts)
	}

	custom := &RuntimeConfig{
		SchemaVersion:                RuntimeConfigSchemaVersion,
		RequestLogTotalMaxMB:         200,
		MaxConsecutiveFailures:       3,
		FailoverEnabled:              true,
		FailoverMaxAttempts:          3,
		FailoverConnectExtraAttempts: 5,
	}
	out2, _ := ApplyCompatibilityDefaults(custom, nil)
	if out2.FailoverConnectExtraAttempts != 5 {
		t.Fatalf("operator value overwritten: got %d, want 5", out2.FailoverConnectExtraAttempts)
	}
}

// The escalation must not outlive the budget that bounds the whole request: a
// request that keeps failing slowly stops escalating (an abandoned attempt is
// not a connect failure), so the extra attempts only ever pile onto failures
// that surfaced fast. The shipped budgets leave room for that.
func TestDefaultRuntimeConfig_EscalationLeavesRoomInTotalBudget(t *testing.T) {
	cfg := NewDefaultRuntimeConfig()
	if cfg.FailoverConnectExtraAttempts <= 0 {
		t.Fatal("precondition: the shipped default must enable escalation")
	}
	// Two extra attempts on top of the configured cap.
	if got := cfg.FailoverMaxAttempts + cfg.FailoverConnectExtraAttempts; got != 4 {
		t.Fatalf("escalated cap: got %d, want 4", got)
	}
	if time.Duration(cfg.FailoverTotalBudget) <= 0 {
		t.Fatal("precondition: the shipped default must keep a total budget")
	}
}
