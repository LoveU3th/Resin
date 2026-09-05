package proxy

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// When every lingering slot is taken, a slow attempt must fail fast rather than
// piling up another abandoned attempt.
func TestLingeringAttemptLimit_ShedsWhenFull(t *testing.T) {
	// Fill the limiter with attempts that never finish.
	release := make(chan struct{})
	for i := 0; i < maxLingeringAttempts; i++ {
		p := FailoverParams[*int]{
			Config: FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 5 * time.Millisecond},
			Resolve: func([]node.Hash) (routedOutbound, *ProxyError) {
				return routedOutbound{Route: routing.RouteResult{NodeHash: node.Hash{1}}}, nil
			},
			// Blocks until released: these attempts linger forever.
			Run: func(context.Context, routedOutbound, *AttemptState) (*int, error) {
				<-release
				v := 1
				return &v, nil
			},
			Cleanup: func(*int) {},
		}
		res := runFailover(context.Background(), p)
		if !res.Abandoned {
			t.Fatalf("attempt %d: expected abandonment", i)
		}
	}

	// Now the limiter is full: a further slow attempt must be shed, not abandoned.
	var shed atomic.Bool
	p := FailoverParams[*int]{
		Config: FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 5 * time.Millisecond},
		Resolve: func([]node.Hash) (routedOutbound, *ProxyError) {
			return routedOutbound{Route: routing.RouteResult{NodeHash: node.Hash{1}}}, nil
		},
		Run: func(context.Context, routedOutbound, *AttemptState) (*int, error) {
			shed.Store(true)
			<-release
			return nil, errors.New("never")
		},
		Cleanup: func(*int) {},
	}
	res := runFailover(context.Background(), p)
	if res.Abandoned {
		t.Fatal("a shed request must not be reported as abandoned: no slot was taken")
	}
	if !errors.Is(res.LastErr, errAttemptOverloaded) {
		t.Fatalf("LastErr: got %v, want errAttemptOverloaded", res.LastErr)
	}
	// It must read as a timeout, so the client sees 504 rather than a node failure.
	if !isTimeoutError(res.LastErr) {
		t.Fatal("an overloaded attempt must be reported as a timeout")
	}

	// Release the lingering attempts so the limiter drains.
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for len(lingeringAttempts) > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(lingeringAttempts); got != 0 {
		t.Fatalf("limiter did not drain: %d slots still held", got)
	}
}
