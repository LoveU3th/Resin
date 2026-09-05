package main

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
)

// Recheck of the unbounded-leak guard in resinApp.failoverConfig.
//
// An abandoned attempt is released only when the upstream finally gives up on
// it. With no response-header timeout there is nothing to make that happen, so
// every abandoned attempt would park a goroutine and a socket forever. The
// guard is supposed to neutralise the attempt budget in that configuration —
// and to leave every sane configuration untouched.

func newRecheckApp(headerTimeout time.Duration, mutate func(*config.RuntimeConfig)) *resinApp {
	env := &config.EnvConfig{ProxyTransportResponseHeaderTimeout: headerTimeout}
	rt := &atomic.Pointer[config.RuntimeConfig]{}
	cfg := config.NewDefaultRuntimeConfig()
	if mutate != nil {
		mutate(cfg)
	}
	rt.Store(cfg)
	return &resinApp{envCfg: env, runtimeCfg: rt}
}

// The configuration that used to leak: a real attempt budget with no bound on
// how long the upstream may take to answer.
func TestRecheck_FailoverBudgetDisabledWhenNoHeaderTimeout(t *testing.T) {
	app := newRecheckApp(0, nil)
	got := app.failoverConfig()

	if got.AttemptBudget != 0 {
		t.Fatalf("AttemptBudget: got %v, want 0 — an abandoned attempt could never be released", got.AttemptBudget)
	}
	// Everything else must be left alone: the guard is about the budget only.
	if !got.Enabled {
		t.Fatal("Enabled: got false, want true (only the attempt budget should be neutralised)")
	}
	if got.MaxAttempts != config.NewDefaultRuntimeConfig().FailoverMaxAttempts {
		t.Fatalf("MaxAttempts: got %d, want %d", got.MaxAttempts, config.NewDefaultRuntimeConfig().FailoverMaxAttempts)
	}
	if got.TotalBudget != time.Duration(config.NewDefaultRuntimeConfig().FailoverTotalBudget) {
		t.Fatalf("TotalBudget: got %v, want %v", got.TotalBudget, config.NewDefaultRuntimeConfig().FailoverTotalBudget)
	}
}

// The shipped default (60s) must not be touched.
func TestRecheck_FailoverBudgetKeptWithDefaultHeaderTimeout(t *testing.T) {
	app := newRecheckApp(60*time.Second, nil)
	got := app.failoverConfig()

	want := time.Duration(config.NewDefaultRuntimeConfig().FailoverAttemptBudget)
	if got.AttemptBudget != want {
		t.Fatalf("AttemptBudget: got %v, want %v — the guard must not fire on the default config", got.AttemptBudget, want)
	}
}

// Any positive header timeout keeps the budget, however small.
func TestRecheck_FailoverBudgetKeptWithAnyPositiveHeaderTimeout(t *testing.T) {
	app := newRecheckApp(time.Millisecond, nil)
	if got := app.failoverConfig(); got.AttemptBudget == 0 {
		t.Fatal("a positive response-header timeout must keep the attempt budget")
	}
}

// A negative timeout cannot survive env validation, but if it ever got through
// it means "unbounded" here too, so the guard must catch it.
func TestRecheck_FailoverBudgetDisabledOnNegativeHeaderTimeout(t *testing.T) {
	app := newRecheckApp(-time.Second, nil)
	if got := app.failoverConfig(); got.AttemptBudget != 0 {
		t.Fatalf("AttemptBudget: got %v, want 0", got.AttemptBudget)
	}
}

// Operators who never armed the budget in the first place must not see the
// warning path change anything.
func TestRecheck_FailoverBudgetStaysZeroWhenUnset(t *testing.T) {
	app := newRecheckApp(60*time.Second, func(c *config.RuntimeConfig) {
		c.FailoverAttemptBudget = 0
	})
	if got := app.failoverConfig(); got.AttemptBudget != 0 {
		t.Fatalf("AttemptBudget: got %v, want 0", got.AttemptBudget)
	}
}
