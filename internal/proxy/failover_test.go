package proxy

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// TestClassifyEstablishmentFailure covers the two retry rules and, just as
// importantly, the cases that must NOT be retried. Retrying a request the server
// already saw would submit it twice.
func TestClassifyEstablishmentFailure(t *testing.T) {
	upstreamErr := errors.New("upstream broke")

	cases := []struct {
		name          string
		dialAttempted bool
		dialSucceeded bool
		gotConn       bool
		reused        bool
		written       int64
		err           error
		wantRetry     bool
		wantDeadConn  bool
	}{
		{
			name:          "dial failure sent nothing, retry is safe",
			dialAttempted: true, dialSucceeded: false,
			err:       upstreamErr,
			wantRetry: true,
		},
		{
			name:          "fresh connection, no bytes written yet",
			dialAttempted: true, dialSucceeded: true, written: 0,
			err:       upstreamErr,
			wantRetry: true,
		},
		{
			name:          "bytes already written, retry would duplicate",
			dialAttempted: true, dialSucceeded: true, written: 128,
			err:       upstreamErr,
			wantRetry: false,
		},
		{
			name:          "pooled connection, cannot tell what was sent",
			dialAttempted: false, dialSucceeded: false,
			gotConn: true, reused: true,
			err:          upstreamErr,
			wantRetry:    false,
			wantDeadConn: true,
		},
		{
			name:          "local failure, no connection involved at all",
			dialAttempted: false, dialSucceeded: false,
			gotConn: false, reused: false,
			err:       upstreamErr,
			wantRetry: false,
		},
		{
			name:          "client cancel is not a node failure",
			dialAttempted: true, dialSucceeded: false,
			err:       context.Canceled,
			wantRetry: false,
		},
		{
			name:          "abandoned attempt may have sent the request",
			dialAttempted: true, dialSucceeded: true, written: 0,
			err:       errAttemptAbandoned,
			wantRetry: false,
		},
		{
			name:          "nil error is not a failure at all",
			dialAttempted: true, dialSucceeded: true, written: 64,
			err:       nil,
			wantRetry: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := newAttemptState()
			if tc.dialAttempted {
				st.dialAttempted.Store(true)
			}
			if tc.dialSucceeded {
				st.dialSucceeded.Store(true)
			}
			if tc.gotConn {
				st.gotConn.Store(true)
			}
			if tc.reused {
				st.reused.Store(true)
			}
			st.addWritten(tc.written)

			got := classifyEstablishmentFailure(tc.err, st)
			if got.retryable != tc.wantRetry {
				t.Fatalf("retryable: got %v, want %v", got.retryable, tc.wantRetry)
			}
			if got.deadConn != tc.wantDeadConn {
				t.Fatalf("deadConn: got %v, want %v", got.deadConn, tc.wantDeadConn)
			}
		})
	}
}

// A response-header timeout must never be retried: by then the request line and
// headers are on the wire and the server may already be acting on them.
func TestClassifyEstablishmentFailure_ResponseHeaderTimeoutIsNotRetried(t *testing.T) {
	st := newAttemptState()
	st.dialAttempted.Store(true)
	st.dialSucceeded.Store(true)
	st.addWritten(256) // headers were written

	if got := classifyEstablishmentFailure(context.DeadlineExceeded, st); got.retryable {
		t.Fatal("a response-header timeout must not be retried")
	}
}

// Only bodyless requests can honestly be sent twice: a server-side request
// never carries GetBody, so once the body is read it is gone.
func TestReplayUnsafe(t *testing.T) {
	noBody, err := http.NewRequest(http.MethodGet, "http://example.com/", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if replayUnsafe(noBody) {
		t.Fatal("a bodyless request must be replayable")
	}

	withBody, err := http.NewRequest(
		http.MethodPost, "http://example.com/", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	defer withBody.Body.Close()
	if !replayUnsafe(withBody) {
		t.Fatal("a request with a body must not be replayed")
	}
}

func TestFailoverConfig_DisabledMeansNoRetry(t *testing.T) {
	cfg := FailoverConfig{Enabled: false, MaxAttempts: 5}
	if got := cfg.maxAttempts(); got != 1 {
		t.Fatalf("maxAttempts with failover disabled: got %d, want 1", got)
	}

	cfg = FailoverConfig{Enabled: true, MaxAttempts: 0}
	if got := cfg.maxAttempts(); got != 1 {
		t.Fatalf("maxAttempts with a zero value: got %d, want 1", got)
	}
}

// helper for building a routing stub that hands out nodes in order, skipping any
// the request has already failed on.
type failoverStub struct {
	nodes     []node.Hash
	attempted []node.Hash
	// failUntil succeeds from this attempt number onwards (1-based).
	failUntil int
}

func (s *failoverStub) params() FailoverParams[string] {
	return FailoverParams[string]{
		Config: FailoverConfig{Enabled: true, MaxAttempts: 3},
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
				// A dial failure: nothing reached the node.
				st.dialAttempted.Store(true)
				return "", errors.New("dial failed")
			}
			return "served", nil
		},
		Classify: classifyEstablishmentFailure,
	}
}

func TestRunFailover_FirstAttemptSucceeds(t *testing.T) {
	stub := &failoverStub{nodes: []node.Hash{{1}, {2}}, failUntil: 0}

	result := runFailover(context.Background(), stub.params())

	if result.Value != "served" {
		t.Fatalf("value: got %q, want served (err=%v)", result.Value, result.LastErr)
	}
	if result.Attempts != 1 {
		t.Fatalf("attempts: got %d, want 1", result.Attempts)
	}
	if len(result.FailedNodes) != 0 {
		t.Fatalf("failed nodes: got %v, want none", result.FailedNodes)
	}
}

func TestRunFailover_RetriesOnAnotherNode(t *testing.T) {
	stub := &failoverStub{nodes: []node.Hash{{1}, {2}}, failUntil: 1}

	result := runFailover(context.Background(), stub.params())

	if result.Value != "served" {
		t.Fatalf("value: got %q, want served (err=%v)", result.Value, result.LastErr)
	}
	if result.Attempts != 2 {
		t.Fatalf("attempts: got %d, want 2", result.Attempts)
	}
	if len(result.FailedNodes) != 1 || result.FailedNodes[0] != (node.Hash{1}) {
		t.Fatalf("failed nodes: got %v, want [1]", result.FailedNodes)
	}
	if result.Route.NodeHash != (node.Hash{2}) {
		t.Fatalf("serving node: got %v, want {2}", result.Route.NodeHash)
	}
}

func TestRunFailover_ExhaustsCandidates(t *testing.T) {
	stub := &failoverStub{nodes: []node.Hash{{1}, {2}, {3}}, failUntil: 99}

	result := runFailover(context.Background(), stub.params())

	if result.Value != "" {
		t.Fatalf("expected no value, got %q", result.Value)
	}
	if result.LastErr == nil {
		t.Fatal("expected the upstream error to be reported")
	}
	if len(result.FailedNodes) != 3 {
		t.Fatalf("failed nodes: got %v, want all three", result.FailedNodes)
	}
}

// When the retry runs out of nodes, the original upstream failure must win.
// Reporting "no nodes available" would mask a timeout as a capacity problem.
func TestRunFailover_UpstreamErrorWinsOverRouteExhaustion(t *testing.T) {
	stub := &failoverStub{nodes: []node.Hash{{1}}, failUntil: 99}

	result := runFailover(context.Background(), stub.params())

	if result.RouteErr != nil {
		t.Fatalf("got route error %v, want the upstream error", result.RouteErr)
	}
	if result.LastErr == nil {
		t.Fatal("expected the upstream error, got none")
	}
}

// A failure that is not retryable must stop the loop immediately.
func TestRunFailover_StopsWhenNotRetryable(t *testing.T) {
	params := FailoverParams[string]{
		Config: FailoverConfig{Enabled: true, MaxAttempts: 5},
		Resolve: func(_ []node.Hash) (routedOutbound, *ProxyError) {
			return routedOutbound{Route: routing.RouteResult{NodeHash: node.Hash{1}}}, nil
		},
		Run: func(_ context.Context, _ routedOutbound, st *AttemptState) (string, error) {
			// Bytes were written, so this cannot be retried.
			st.dialAttempted.Store(true)
			st.dialSucceeded.Store(true)
			st.addWritten(512)
			return "", errors.New("broke after writing")
		},
		Classify: classifyEstablishmentFailure,
	}

	result := runFailover(context.Background(), params)

	if result.Attempts != 1 {
		t.Fatalf("attempts: got %d, want 1 (unretryable failures stop immediately)", result.Attempts)
	}
}

// The per-attempt budget abandons a slow attempt and moves on, rather than
// waiting for it forever.
func TestRunFailover_AbandonsSlowAttempt(t *testing.T) {
	params := FailoverParams[string]{
		Config: FailoverConfig{
			Enabled:       true,
			MaxAttempts:   2,
			AttemptBudget: 20 * time.Millisecond,
		},
		Resolve: func(_ []node.Hash) (routedOutbound, *ProxyError) {
			return routedOutbound{Route: routing.RouteResult{NodeHash: node.Hash{1}}}, nil
		},
		Run: func(ctx context.Context, _ routedOutbound, _ *AttemptState) (string, error) {
			select {
			case <-time.After(time.Second):
				return "slow", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
		Classify: classifyEstablishmentFailure,
	}

	started := time.Now()
	result := runFailover(context.Background(), params)
	elapsed := time.Since(started)

	if elapsed > time.Second {
		t.Fatalf("the attempt budget was ignored: took %v", elapsed)
	}
	// An abandoned attempt must not be retried: whether its request went out is
	// unknown, so the honest answer is to stop.
	if result.Attempts != 1 {
		t.Fatalf("attempts: got %d, want 1 (an abandoned attempt is not retried)", result.Attempts)
	}
	if !result.Abandoned {
		t.Fatal("expected the result to be marked abandoned")
	}
	if result.Value != "" {
		t.Fatalf("expected no value from an abandoned attempt, got %q", result.Value)
	}
}

// Every attempt being retryable is the normal "node unreachable" case, and it is
// easy to double-count: OnAttempt reports each attempt as it ends, and the
// caller then reports the failure again. That would push the last node toward
// eviction twice over for a single failed request, so the verdict is exposed for
// the caller to check.
func TestRunFailover_AllRetryableExposesLastVerdict(t *testing.T) {
	var verdicts []attemptVerdict
	stub := &failoverStub{nodes: []node.Hash{{1}, {2}}, failUntil: 99}
	params := stub.params()
	inner := params.OnAttempt
	params.OnAttempt = func(res routing.RouteResult, v attemptVerdict) {
		verdicts = append(verdicts, v)
		if inner != nil {
			inner(res, v)
		}
	}

	result := runFailover(context.Background(), params)

	if result.Value != "" {
		t.Fatalf("expected failure, got %q", result.Value)
	}
	if !result.LastVerdict.retryable {
		t.Fatal("the final verdict should be retryable, so the caller must not report it again")
	}
	if len(verdicts) != 2 {
		t.Fatalf("OnAttempt calls: got %d, want 2", len(verdicts))
	}
	for i, v := range verdicts {
		if !v.retryable {
			t.Fatalf("verdict %d: expected retryable", i)
		}
	}
}

// A non-retryable failure must be reported by the caller, so its verdict says
// the opposite.
func TestRunFailover_NonRetryableLeavesLastVerdictClear(t *testing.T) {
	stub := &failoverStub{nodes: []node.Hash{{1}, {2}}, failUntil: 99}
	params := stub.params()
	// Bytes written, so nothing can be replayed safely.
	params.Run = func(_ context.Context, _ routedOutbound, st *AttemptState) (string, error) {
		st.dialAttempted.Store(true)
		st.dialSucceeded.Store(true)
		st.addWritten(256)
		return "", errors.New("broke after writing")
	}

	result := runFailover(context.Background(), params)

	if result.LastVerdict.retryable {
		t.Fatal("a non-retryable failure must leave LastVerdict unretryable so the caller reports it")
	}
	if result.Attempts != 1 {
		t.Fatalf("attempts: got %d, want 1", result.Attempts)
	}
}

// A budget expiry is a timeout, not a generic failure: a slow origin must not be
// recorded as an unreachable node.
func TestErrAbandonedReportsAsTimeout(t *testing.T) {
	if !isTimeoutError(errAttemptAbandoned) {
		t.Fatal("an abandoned attempt must be classified as a timeout (504), not a plain failure (502)")
	}
	if !errors.Is(errAttemptAbandoned, errAttemptAbandoned) {
		t.Fatal("errors.Is must keep working for the value-typed sentinel")
	}
}
