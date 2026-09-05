package proxy

import (
	"context"
	"errors"
	"net/http"
	"runtime"
	"testing"
	"time"
)

// Recheck of the two release paths in runAttempt.
//
// Both branches that stop waiting on an attempt (per-attempt budget expiry, and
// client cancellation) must still release whatever the attempt eventually
// produces. A tunneled socket is the case that matters: nothing else closes it,
// because dialTunnelConn only cancels its dial context when the connection is
// closed.

// TestRecheck_ClientCancelReleasesLateAttemptValue covers the ctx.Done branch:
// the client goes away, then the attempt succeeds anyway. The value it produced
// must still be cleaned up.
func TestRecheck_ClientCancelReleasesLateAttemptValue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gate := make(chan struct{})
	released := make(chan int, 1)

	p := FailoverParams[int]{
		Config: FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: time.Minute},
		Run: func(_ context.Context, _ routedOutbound, _ *AttemptState) (int, error) {
			<-gate
			return 42, nil
		},
		Cleanup: func(v int) { released <- v },
	}

	type outcome struct {
		v   int
		err error
	}
	out := make(chan outcome, 1)
	go func() {
		v, err := p.runAttempt(ctx, routedOutbound{}, newAttemptState())
		out <- outcome{v: v, err: err}
	}()

	// Let runAttempt reach its select, then cancel the client context.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case o := <-out:
		if !errors.Is(o.err, context.Canceled) {
			t.Fatalf("err: got %v, want context.Canceled", o.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runAttempt did not return after the client cancelled")
	}

	// The attempt finishes after the caller already gave up on it.
	close(gate)

	select {
	case v := <-released:
		if v != 42 {
			t.Fatalf("released value: got %d, want 42", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a successful attempt that outlived its caller was never released")
	}
}

// TestRecheck_AbandonedAttemptReleasesLateValue covers the budget-expiry branch.
func TestRecheck_AbandonedAttemptReleasesLateValue(t *testing.T) {
	gate := make(chan struct{})
	released := make(chan int, 1)

	p := FailoverParams[int]{
		Config: FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 20 * time.Millisecond},
		Run: func(_ context.Context, _ routedOutbound, _ *AttemptState) (int, error) {
			<-gate
			return 7, nil
		},
		Cleanup: func(v int) { released <- v },
	}

	_, err := p.runAttempt(context.Background(), routedOutbound{}, newAttemptState())
	if !errors.Is(err, errAttemptAbandoned) {
		t.Fatalf("err: got %v, want errAttemptAbandoned", err)
	}

	close(gate)

	select {
	case v := <-released:
		if v != 7 {
			t.Fatalf("released value: got %d, want 7", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("an abandoned attempt that later succeeded was never released")
	}
}

// An attempt that fails outright must not be handed to Cleanup: there is
// nothing to release, and calling it with a zero value would be a nil deref on
// the *http.Response path.
func TestRecheck_FailedLateAttemptIsNotCleanedUp(t *testing.T) {
	gate := make(chan struct{})
	cleanups := make(chan struct{}, 1)

	p := FailoverParams[*http.Response]{
		Config: FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 20 * time.Millisecond},
		Run: func(_ context.Context, _ routedOutbound, _ *AttemptState) (*http.Response, error) {
			<-gate
			return nil, errors.New("upstream broke")
		},
		Cleanup: func(*http.Response) { cleanups <- struct{}{} },
	}

	if _, err := p.runAttempt(context.Background(), routedOutbound{}, newAttemptState()); !errors.Is(err, errAttemptAbandoned) {
		t.Fatalf("err: got %v, want errAttemptAbandoned", err)
	}
	close(gate)

	select {
	case <-cleanups:
		t.Fatal("Cleanup must not be called for an attempt that failed")
	case <-time.After(200 * time.Millisecond):
	}
}

// Goroutine accounting: repeated abandonment must not pile up goroutines once
// the attempt has been released. This is the claim the reverted cancel() change
// was supposed to improve — verify the current "no cancel + Cleanup" form really
// does settle.
func TestRecheck_AbandonedAttemptsDoNotPileUpGoroutines(t *testing.T) {
	gate := make(chan struct{})
	released := make(chan struct{}, 256)

	p := FailoverParams[int]{
		Config: FailoverConfig{Enabled: true, MaxAttempts: 1, AttemptBudget: 5 * time.Millisecond},
		Run: func(_ context.Context, _ routedOutbound, _ *AttemptState) (int, error) {
			<-gate
			return 1, nil
		},
		Cleanup: func(int) { released <- struct{}{} },
	}

	const n = 64
	for i := 0; i < n; i++ {
		if _, err := p.runAttempt(context.Background(), routedOutbound{}, newAttemptState()); !errors.Is(err, errAttemptAbandoned) {
			t.Fatalf("attempt %d: got %v, want errAttemptAbandoned", i, err)
		}
	}

	before := runtime.NumGoroutine()
	close(gate)
	for i := 0; i < n; i++ {
		select {
		case <-released:
		case <-time.After(3 * time.Second):
			t.Fatalf("only %d of %d abandoned attempts were released", i, n)
		}
	}

	// Wait for the releasing goroutines to actually exit, then compare.
	settleDeadline := time.Now().Add(3 * time.Second)
	settled := 0
	for time.Now().Before(settleDeadline) {
		settled = runtime.NumGoroutine()
		if settled <= before {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if settled > before {
		t.Fatalf("goroutines: settled=%d, want <= %d (abandoned attempts are piling up)", settled, before)
	}
}
