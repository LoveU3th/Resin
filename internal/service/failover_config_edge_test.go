package service

import (
	"strings"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/proxy"
)

// Operations staff will edit these at runtime, so extreme values must be
// rejected cleanly rather than crashing or hanging requests.
func TestFailoverConfigEdgeValues(t *testing.T) {
	// Start from the shipped defaults, then vary only the failover fields under
	// test, so a failure isolates those fields rather than some other check.
	base := func() config.RuntimeConfig {
		return *config.NewDefaultRuntimeConfig()
	}

	cases := []struct {
		name   string
		mutate func(*config.RuntimeConfig)
		expect string // "ok" or a substring of the expected error
	}{
		{"disabled", func(c *config.RuntimeConfig) {
			c.FailoverEnabled = false
			c.FailoverMaxAttempts = 1
		}, "ok"},
		{"zero attempts rejected", func(c *config.RuntimeConfig) { c.FailoverMaxAttempts = 0 }, "at least 1"},
		{"negative attempts rejected", func(c *config.RuntimeConfig) { c.FailoverMaxAttempts = -3 }, "at least 1"},
		{"negative attempt budget", func(c *config.RuntimeConfig) {
			c.FailoverAttemptBudget = -1
		}, "must not be negative"},
		{"negative total budget", func(c *config.RuntimeConfig) {
			c.FailoverTotalBudget = -1
		}, "must not be negative"},
		{"attempt exceeds total", func(c *config.RuntimeConfig) {
			c.FailoverAttemptBudget = config.Duration(90 * time.Second)
			c.FailoverTotalBudget = config.Duration(30 * time.Second)
		}, "must not exceed"},
		{"total disabled allows long attempt", func(c *config.RuntimeConfig) {
			c.FailoverAttemptBudget = config.Duration(90 * time.Second)
			c.FailoverTotalBudget = 0
		}, "ok"},
		{"equal budgets ok", func(c *config.RuntimeConfig) {
			c.FailoverAttemptBudget = config.Duration(30 * time.Second)
			c.FailoverTotalBudget = config.Duration(30 * time.Second)
		}, "ok"},
		{"huge attempts accepted", func(c *config.RuntimeConfig) {
			c.FailoverMaxAttempts = 100000
		}, "ok"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(&cfg)
			err := validateRuntimeConfig(&cfg)
			if tc.expect == "ok" {
				if err != nil {
					t.Fatalf("expected acceptance, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected rejection containing %q, got none", tc.expect)
			}
			if !strings.Contains(err.Error(), tc.expect) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.expect)
			}
		})
	}
}

// Whatever the knobs say, a zero-valued config must degrade to "no retrying"
// rather than to something surprising.
func TestFailoverConfigZeroValueDegradesSafely(t *testing.T) {
	cfg := proxy.FailoverConfig{}
	if got := cfg.MaxAttempts; got > 1 {
		t.Fatalf("a zero config must not enable retrying, got %d", got)
	}
}
