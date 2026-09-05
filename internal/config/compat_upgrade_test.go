package config

import "testing"

// A config persisted before these fields existed reads them back as zero. The
// new behaviour must not stay switched off because of that.
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

	// A config with the feature deliberately disabled must not be re-enabled.
	disabled := &RuntimeConfig{
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
	noCooldown := &RuntimeConfig{
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
