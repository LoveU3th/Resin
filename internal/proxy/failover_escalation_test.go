package proxy

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// escalatingStub hands out nodes in order and fails the first failUntil
// attempts at the connect stage — nothing was ever sent — so every failure is
// safe to retry.
type escalatingStub struct {
	nodes     []node.Hash
	attempted []node.Hash
	failUntil int
}

func (s *escalatingStub) params(cfg FailoverConfig) FailoverParams[string] {
	return FailoverParams[string]{
		Config: cfg,
		Resolve: func(exclude []node.Hash) (routedOutbound, *ProxyError) {
			for _, h := range s.nodes {
				excluded := false
				for _, e := range exclude {
					if e == h {
						excluded = true
						break
					}
				}
				if !excluded {
					return routedOutbound{Route: routing.RouteResult{NodeHash: h}}, nil
				}
			}
			return routedOutbound{}, ErrNoAvailableNodes
		},
		Run: func(_ context.Context, routed routedOutbound, st *AttemptState) (string, error) {
			s.attempted = append(s.attempted, routed.Route.NodeHash)
			if len(s.attempted) <= s.failUntil {
				st.dialAttempted.Store(true) // dialSucceeded stays false: unreachable
				return "", errors.New("dial failed")
			}
			return "served", nil
		},
		Classify: classifyEstablishmentFailure,
	}
}

// In a pool where nodes fail to connect, a flat MaxAttempts gives up while
// nodes that would have worked were never tried. Every failure so far is
// provably pre-upstream, so the cap may widen.
func TestRunFailover_EscalatesOnConnectFailures(t *testing.T) {
	stub := &escalatingStub{nodes: []node.Hash{{1}, {2}, {3}, {4}}, failUntil: 3}

	result := runFailover(context.Background(), stub.params(FailoverConfig{
		Enabled:                     true,
		MaxAttempts:                 2,
		ConnectFailureExtraAttempts: 2, // cap becomes 4
	}))

	if result.Value != "served" {
		t.Fatalf("value: got %q, want served (err=%v)", result.Value, result.LastErr)
	}
	if result.Attempts != 4 {
		t.Fatalf("attempts: got %d, want 4 (2 configured + 2 escalation)", result.Attempts)
	}
	if len(result.FailedNodes) != 3 {
		t.Fatalf("failed nodes: got %v, want the first three", result.FailedNodes)
	}
}

// Without escalation the same request stops at the configured cap.
func TestRunFailover_NoEscalationWithoutExtraAttempts(t *testing.T) {
	stub := &escalatingStub{nodes: []node.Hash{{1}, {2}, {3}, {4}}, failUntil: 99}

	result := runFailover(context.Background(), stub.params(FailoverConfig{
		Enabled:     true,
		MaxAttempts: 2,
	}))

	if result.Value != "" {
		t.Fatalf("expected failure, got %q", result.Value)
	}
	if result.Attempts != 2 {
		t.Fatalf("attempts: got %d, want 2", result.Attempts)
	}
}

// The escalation is only justified while nothing has been sent. A failure the
// classifier marks transfer-stage — even a retryable one — ends it, so the cap
// stays at the configured value.
func TestRunFailover_EscalationStopsOnNonConnectFailure(t *testing.T) {
	var attempts int
	params := FailoverParams[string]{
		Config: FailoverConfig{Enabled: true, MaxAttempts: 2, ConnectFailureExtraAttempts: 5},
		Resolve: func(_ []node.Hash) (routedOutbound, *ProxyError) {
			attempts++
			return routedOutbound{Route: routing.RouteResult{NodeHash: node.Hash{byte(attempts)}}}, nil
		},
		Run: func(_ context.Context, _ routedOutbound, st *AttemptState) (string, error) {
			st.dialAttempted.Store(true)
			st.dialSucceeded.Store(true)
			st.addWritten(64) // bytes went out: transfer-stage territory
			return "", errors.New("broke mid-transfer")
		},
		Classify: func(error, *AttemptState) attemptVerdict {
			// Retryable, but not connect-stage: the request may have landed.
			return attemptVerdict{retryable: true, stage: node.PassiveStageTransfer}
		},
	}

	result := runFailover(context.Background(), params)

	if result.Attempts != 2 {
		t.Fatalf("attempts: got %d, want 2 (escalation must not apply to transfer failures)", result.Attempts)
	}
}

// Escalation widens the attempt cap, never the time budget: the total budget
// still stops the loop.
//
// The assertion avoids wall-clock reasoning about how many attempts fit: it
// pins the invariant directly (no attempt may start once the budget is spent)
// and proves the bound is doing work by running the same stub without a budget
// first. Under load an attempt may take longer, which can only reduce the
// count — never break the invariant.
func TestRunFailover_EscalationRespectsTotalBudget(t *testing.T) {
	const budget = 50 * time.Millisecond

	newParams := func(totalBudget time.Duration, starts *[]time.Time) FailoverParams[string] {
		stub := &escalatingStub{nodes: []node.Hash{{1}, {2}, {3}, {4}, {5}, {6}}, failUntil: 99}
		params := stub.params(FailoverConfig{
			Enabled:                     true,
			MaxAttempts:                 2,
			ConnectFailureExtraAttempts: 8, // cap 10, more nodes than the budget allows
			TotalBudget:                 totalBudget,
		})
		inner := params.Run
		params.Run = func(ctx context.Context, routed routedOutbound, st *AttemptState) (string, error) {
			if starts != nil {
				*starts = append(*starts, time.Now())
			}
			time.Sleep(30 * time.Millisecond) // each attempt costs real time
			return inner(ctx, routed, st)
		}
		return params
	}

	// Control: with no budget the escalated cap tries every node, so a bounded
	// run must attempt strictly fewer.
	var controlStarts []time.Time
	runFailover(context.Background(), newParams(0, &controlStarts))
	if len(controlStarts) != 6 {
		t.Fatalf("control: got %d attempts, want 6 (no budget, cap 10)", len(controlStarts))
	}

	var starts []time.Time
	requestStart := time.Now()
	result := runFailover(context.Background(), newParams(budget, &starts))

	for i, at := range starts {
		if at.Sub(requestStart) > budget {
			t.Fatalf("attempt %d started %v into a %v budget",
				i+1, at.Sub(requestStart), budget)
		}
	}
	if len(starts) >= len(controlStarts) {
		t.Fatalf("the total budget did not bound escalation: %d attempts, control %d",
			len(starts), len(controlStarts))
	}
	if result.Attempts != len(starts) {
		t.Fatalf("Attempts %d does not match the %d attempts that ran",
			result.Attempts, len(starts))
	}
}

// A config that asked for a single attempt must not get more because the first
// one failed to connect. This is what the reverse path produces for a request
// whose body cannot be replayed.
func TestFailoverConfig_EscalationDisabledWhenRetryingOff(t *testing.T) {
	cfg := FailoverConfig{Enabled: true, MaxAttempts: 1, ConnectFailureExtraAttempts: 5}
	if got := cfg.escalatedMaxAttempts(true); got != 1 {
		t.Fatalf("MaxAttempts=1: got %d, want 1", got)
	}
	if eff := cfg.effective(); eff.ConnectFailureExtraAttempts != 0 {
		t.Fatalf("MaxAttempts=1 must zero the escalation, got %d", eff.ConnectFailureExtraAttempts)
	}

	cfg = FailoverConfig{Enabled: false, MaxAttempts: 5, ConnectFailureExtraAttempts: 5}
	if got := cfg.escalatedMaxAttempts(true); got != 1 {
		t.Fatalf("disabled: got %d, want 1", got)
	}
	if eff := cfg.effective(); eff.ConnectFailureExtraAttempts != 0 {
		t.Fatalf("disabled must zero the escalation, got %d", eff.ConnectFailureExtraAttempts)
	}
}

// A negative value degrades to "no escalation" rather than shrinking the cap
// below the configured attempts.
func TestFailoverConfig_EscalationClampsNegative(t *testing.T) {
	cfg := FailoverConfig{Enabled: true, MaxAttempts: 3, ConnectFailureExtraAttempts: -4}
	if got := cfg.escalatedMaxAttempts(true); got != 3 {
		t.Fatalf("negative extra attempts: got %d, want 3", got)
	}
	if got := cfg.escalatedMaxAttempts(false); got != 3 {
		t.Fatalf("without connect-only failures: got %d, want 3", got)
	}
}
