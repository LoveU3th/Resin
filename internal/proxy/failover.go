package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// errAbandoned reports an attempt left running because its budget expired.
//
// It reports as a timeout deliberately. Budget expiry means the node was slow to
// answer, and treating it as a plain failure would record an unreachable node at
// full weight — enough of those would evict a node that is merely slow, which is
// the opposite of what failover is for.
//
// It is never shown to a client: the caller falls back to the last real failure,
// or to a timeout when a later attempt also fails.
type errAbandoned struct{}

func (errAbandoned) Error() string { return "attempt abandoned: budget expired" }
func (errAbandoned) Timeout() bool { return true }

var (
	errAttemptAbandoned = errAbandoned{}
	// errNoRouteForAttempt means no node was available for a retry. The caller
	// distinguishes it from an upstream failure so the status code stays right.
	errNoRouteForAttempt = errors.New("no node available for retry")
)

// AttemptState captures what one attempt at a request actually did.
//
// It exists to answer a single question safely: did this request reach the
// upstream server? If it did, retrying it on another node would submit the
// request twice, which is unacceptable for anything that is not idempotent.
type AttemptState struct {
	dialAttempted atomic.Bool
	dialSucceeded atomic.Bool
	written       atomic.Int64
	// gotConn is set by the httptrace hook below. It distinguishes "the
	// transport reused a pooled connection" from "the transport failed before it
	// ever dialed", which dialAttempted alone cannot.
	gotConn atomic.Bool
	reused  atomic.Bool
	// proto is the negotiated protocol, set from the final response.
	proto atomic.Pointer[string]
}

func newAttemptState() *AttemptState {
	return &AttemptState{}
}

// dialed reports whether this attempt established a brand-new connection.
// Only then can the byte counter below be trusted to start from zero.
func (s *AttemptState) dialed() bool {
	return s.dialAttempted.Load() && s.dialSucceeded.Load()
}

func (s *AttemptState) addWritten(n int64) {
	if n > 0 {
		s.written.Add(n)
	}
}

// bytesWritten is how much of the request body reached this connection during
// this attempt. It is only meaningful when dialed() is true: on a pooled
// connection the counter includes earlier requests, so it cannot be attributed.
func (s *AttemptState) bytesWritten() int64 {
	return s.written.Load()
}

type attemptStateKey struct{}

// withAttemptState attaches an AttemptState to a context so the transport's
// dial hook can report back what it did.
func withAttemptState(ctx context.Context, st *AttemptState) context.Context {
	return context.WithValue(ctx, attemptStateKey{}, st)
}

// clientTrace returns hooks that report connection acquisition back to the
// attempt state. Callers must attach it to the request they hand to the
// transport; httptrace composes nested traces, so it can be added alongside an
// existing one.
func (s *AttemptState) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			s.gotConn.Store(true)
			s.reused.Store(info.Reused)
		},
	}
}

func attemptStateFrom(ctx context.Context) *AttemptState {
	if st, ok := ctx.Value(attemptStateKey{}).(*AttemptState); ok {
		return st
	}
	return nil
}

// dialObserverConn counts request bytes written on a connection that this
// attempt dialed, so a failure before any byte was written can be detected.
type dialObserverConn struct {
	net.Conn
	st *AttemptState
}

func (c *dialObserverConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.st.addWritten(int64(n))
	return n, err
}

// attemptVerdict is the outcome of classifying a failed attempt.
type attemptVerdict struct {
	// retryable means the request provably did not reach the upstream, so
	// sending it to a different node cannot cause a duplicate side effect.
	retryable bool
	// deadConn means a pooled connection was found to be dead. It is a signal
	// about the node (a peer that vanished without a FIN), but it is not by
	// itself enough to retry on.
	deadConn bool
}

// FailoverConfig controls request-level failover.
type FailoverConfig struct {
	Enabled bool
	// MaxAttempts counts the initial attempt, so 1 means "no retry".
	MaxAttempts int
	// AttemptBudget bounds a single attempt. Reaching it abandons that attempt
	// and moves on; the abandoned attempt keeps running in the background under
	// its own dial and response-header timeouts.
	AttemptBudget time.Duration
	// TotalBudget bounds all attempts together.
	TotalBudget time.Duration
}

// effective normalises zero values so a partially filled config degrades to
// "disabled" rather than to something surprising.
func (c FailoverConfig) effective() FailoverConfig {
	if !c.Enabled || c.MaxAttempts <= 1 {
		c.MaxAttempts = 1
	}
	if c.MaxAttempts < 1 {
		c.MaxAttempts = 1
	}
	return c
}

func (c FailoverConfig) maxAttempts() int {
	cfg := c.effective()
	return cfg.MaxAttempts
}

// FailoverParams drives runFailover.
type FailoverParams[T any] struct {
	Config FailoverConfig
	// Resolve picks a node, excluding the ones already tried.
	Resolve func(exclude []node.Hash) (routedOutbound, *ProxyError)
	// Run performs the request. It must attach the AttemptState to the request
	// context, and must arrange for gotConn to be observable.
	Run      func(ctx context.Context, routed routedOutbound, st *AttemptState) (T, error)
	Classify func(err error, st *AttemptState) attemptVerdict
	// OnAttempt reports each attempt, including abandoned ones, so health
	// feedback and metrics see every node that was tried.
	OnAttempt func(routing.RouteResult, attemptVerdict)
	// Cleanup releases whatever a successful attempt produced. It is called on
	// the result of an abandoned attempt once that attempt finally finishes —
	// without it, a response body would never be closed and a socket would stay
	// out of the pool until the GC got around to it.
	Cleanup func(T)
}

// FailoverResult is the outcome of runFailover.
type FailoverResult[T any] struct {
	Value       T
	Route       routing.RouteResult
	Attempts    int
	FailedNodes []node.Hash
	LastErr     error
	// LastVerdict is the classification of the final attempt. Callers use it to
	// avoid recording a failure twice: a retryable attempt is already reported
	// through OnAttempt, so only a non-retryable one needs reporting here.
	LastVerdict attemptVerdict
	// RouteErr is set when the last failure was routing rather than upstream.
	// It carries its own status code and must take precedence over LastErr.
	RouteErr  *ProxyError
	Abandoned bool
}

// runFailover tries a request on up to MaxAttempts distinct nodes, retrying only
// when the request provably did not reach the upstream.
//
// The budget is enforced by abandoning an attempt rather than by cancelling the
// request context: the context is inherited by the response body, so putting a
// deadline on it would cut off streaming responses and long downloads. An
// abandoned attempt keeps running in the background and its result is discarded.
func runFailover[T any](ctx context.Context, p FailoverParams[T]) FailoverResult[T] {
	maxAttempts := p.Config.maxAttempts()

	var (
		exclude     []node.Hash
		failedNodes []node.Hash
		lastErr     error
		lastVerdict attemptVerdict
		abandoned   bool
		attempts    int
	)

	startedAt := time.Now()

	for attempt := 0; attempt < maxAttempts; attempt++ {
		attempts = attempt + 1

		if p.Config.TotalBudget > 0 && time.Since(startedAt) >= p.Config.TotalBudget {
			break
		}

		routed, routeErr := p.Resolve(exclude)
		if routeErr != nil {
			// Nothing left to try. Prefer the last upstream error so a timeout
			// is not reported as "no nodes available". The last verdict travels
			// with it: it describes the final attempt that actually ran, which is
			// what decides whether the caller still owes a health record.
			if lastErr != nil {
				return FailoverResult[T]{
					LastErr:     lastErr,
					LastVerdict: lastVerdict,
					Attempts:    attempts,
					FailedNodes: failedNodes,
					Abandoned:   abandoned,
				}
			}
			return FailoverResult[T]{
				RouteErr:    routeErr,
				Attempts:    attempts,
				FailedNodes: failedNodes,
				Abandoned:   abandoned,
			}
		}

		st := newAttemptState()
		value, err := p.runAttempt(ctx, routed, st)
		if err == nil {
			if p.OnAttempt != nil {
				p.OnAttempt(routed.Route, attemptVerdict{})
			}
			return FailoverResult[T]{
				Value:       value,
				Route:       routed.Route,
				Attempts:    attempts,
				FailedNodes: failedNodes,
				Abandoned:   abandoned,
			}
		}

		if errors.Is(err, errAttemptAbandoned) {
			abandoned = true
		}
		verdict := p.classify(err, st)
		if p.OnAttempt != nil {
			p.OnAttempt(routed.Route, attemptVerdict{retryable: verdict.retryable, deadConn: verdict.deadConn})
		}

		lastErr = err
		lastVerdict = verdict
		exclude = append(exclude, routed.Route.NodeHash)
		failedNodes = append(failedNodes, routed.Route.NodeHash)

		if !verdict.retryable {
			break
		}
		if attempt+1 >= maxAttempts {
			break
		}
	}

	return FailoverResult[T]{
		LastErr:     lastErr,
		LastVerdict: lastVerdict,
		Attempts:    attempts,
		FailedNodes: failedNodes,
		Abandoned:   abandoned,
	}
}

// runAttempt runs one attempt, giving up on it when the per-attempt budget
// expires. The abandoned attempt is not cancelled: it finishes on its own and
// its result is dropped, which is why its value must be discarded carefully by
// the caller-supplied Run.
func (p FailoverParams[T]) runAttempt(
	ctx context.Context,
	routed routedOutbound,
	st *AttemptState,
) (T, error) {
	var zero T
	if p.Config.AttemptBudget <= 0 {
		return p.Run(ctx, routed, st)
	}

	type attemptResult struct {
		value T
		err   error
	}
	done := make(chan attemptResult, 1)
	go func() {
		value, err := p.Run(ctx, routed, st)
		done <- attemptResult{value: value, err: err}
	}()

	timer := time.NewTimer(p.Config.AttemptBudget)
	defer timer.Stop()

	select {
	case res := <-done:
		return res.value, res.err
	case <-timer.C:
		// Abandoned. The attempt keeps running in the background, so wait for it
		// on its own and release whatever it produced — otherwise the response
		// body (or a tunneled socket) is never closed.
		if p.Cleanup != nil {
			go func() {
				if res := <-done; res.err == nil {
					p.Cleanup(res.value)
				}
			}()
		}
		return zero, errAttemptAbandoned
	case <-ctx.Done():
		// The client went away. Nothing to release: the transport observes the
		// cancellation itself.
		return zero, ctx.Err()
	}
}

func (p FailoverParams[T]) classify(err error, st *AttemptState) attemptVerdict {
	if p.Classify == nil {
		return attemptVerdict{}
	}
	return p.Classify(err, st)
}

// classifyEstablishmentFailure decides whether a failed attempt may be retried
// on another node.
//
// The rule is deliberately narrow: retry only when the request provably did not
// reach the upstream. Retrying a request the server already saw would submit it
// twice, which is unacceptable for anything that is not idempotent.
//
//	R1 — no connection was obtained, so nothing was sent and nothing can be
//	     duplicated. Covers dial, TLS handshake and proxy handshake failures.
//	R2 — this attempt dialed the connection itself and wrote no bytes before
//	     failing. The counter starts at zero on a freshly dialed connection, so
//	     a zero reading means the request never left.
//
// A response-header timeout is deliberately excluded: by then the request line
// and headers have been written and the server may already be acting on them,
// so declaring that safe would be a lie.
func classifyEstablishmentFailure(err error, st *AttemptState) attemptVerdict {
	if err == nil || st == nil {
		return attemptVerdict{}
	}

	// The client going away is neither a node failure nor a retry candidate.
	if errors.Is(err, context.Canceled) {
		return attemptVerdict{}
	}

	deadConn := isDeadConnSignal(err, st)

	// An abandoned attempt is still running in the background, so whether its
	// request was sent is unknown. Guessing risks a duplicate submission.
	if errors.Is(err, errAttemptAbandoned) {
		return attemptVerdict{deadConn: deadConn}
	}

	switch {
	case !st.gotConn.Load() && !st.dialAttempted.Load():
		// The transport failed before it obtained any connection — an invalid
		// scheme or a local TLS setup problem, for example. No connection is
		// involved, so this says nothing about the node.
		return attemptVerdict{}
	case !st.dialAttempted.Load():
		// A pooled connection was used. Go's transport already retries writes
		// that fail before any byte was sent, so an error surfacing here means
		// either that retry also failed or bytes had gone out. The counter
		// includes earlier requests and cannot settle it, so do not retry.
		return attemptVerdict{deadConn: true}
	case !st.dialSucceeded.Load():
		// The dial failed: no connection, nothing sent.
		return attemptVerdict{retryable: true, deadConn: deadConn}
	case st.dialed() && st.bytesWritten() == 0:
		// Fresh connection and the request was never written.
		return attemptVerdict{retryable: true, deadConn: deadConn}
	default:
		return attemptVerdict{deadConn: deadConn}
	}
}

// isDeadConnSignal reports whether the failure looks like a pooled connection
// that the peer had already dropped without telling us — a half-open
// connection, which is exactly the "it connects, then breaks" symptom.
//
// It is real evidence against the node, but it is not by itself grounds to
// retry: the request may already have gone out on that connection.
func isDeadConnSignal(err error, st *AttemptState) bool {
	if st == nil || st.dialed() {
		// A connection this attempt just dialed cannot be stale.
		return false
	}
	// Without proof a connection was handed over, this is a local failure, not
	// evidence about the node.
	if !st.gotConn.Load() && st.reused.Load() == false && !st.dialAttempted.Load() {
		return false
	}
	// A timeout may mean the origin is slow rather than the connection dead.
	if isTimeoutError(err) {
		return false
	}
	switch extractErrnoCode(err) {
	case "ECONNRESET", "EPIPE", "ECONNABORTED":
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF)
}

func isTimeoutError(err error) bool {
	return os.IsTimeout(err) || errors.Is(err, context.DeadlineExceeded)
}

// replayUnsafe reports whether a request cannot be sent twice.
//
// Server-side requests never carry GetBody, so once the body has been read for
// the first attempt it is gone. Retrying would send a truncated or empty body,
// which is worse than not retrying at all. This is why failover effectively
// covers GET and HEAD only — which is also where "the page will not load"
// shows up most.
func replayUnsafe(req *http.Request) bool {
	return req.Body != nil && req.Body != http.NoBody
}

// reverseFailoverTransport is an http.RoundTripper that retries on another node
// when the request provably did not reach the first one.
//
// httputil.ReverseProxy calls Transport itself, so unlike the forward path there
// is no opportunity to wrap the call from outside; the retry has to live inside
// the transport. The resolved node is recorded so the caller can attribute the
// result to the node that actually served it.
type reverseFailoverTransport struct {
	cfg       FailoverConfig
	resolve   func(exclude []node.Hash) (routedOutbound, *ProxyError)
	transport func(routed routedOutbound) *http.Transport
	onAttempt func(res routing.RouteResult, verdict attemptVerdict)
	// onRun reports the node each attempt is about to use, used for latency
	// sampling that is not tied to success or failure.
	onRun func(routed routedOutbound)
	// tlsTrace returns hooks for measuring TLS handshake latency. It is built
	// per attempt because the node is only known at that point; during Director
	// it is not, which is why hooks cannot be attached there.
	tlsTrace func(routed routedOutbound) *httptrace.ClientTrace

	// result is kept so the caller can see which node served the request, which
	// ones were tried, and whether the failure was retryable.
	result *FailoverResult[*http.Response]

	// current is the node of the attempt in flight. ModifyResponse runs while
	// ServeHTTP is still executing, so it cannot wait for result and reads this
	// instead.
	current atomic.Pointer[routing.RouteResult]
}

// currentRoute reports the node of the attempt in flight, if any.
func (t *reverseFailoverTransport) currentRoute() (routing.RouteResult, bool) {
	if t == nil {
		return routing.RouteResult{}, false
	}
	if r := t.current.Load(); r != nil {
		return *r, true
	}
	return routing.RouteResult{}, false
}

func (t *reverseFailoverTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	cfg := t.cfg
	// A body cannot be replayed, so there is no honest retry to make.
	if !cfg.Enabled || replayUnsafe(req) {
		cfg.MaxAttempts = 1
	}

	result := runFailover(req.Context(), FailoverParams[*http.Response]{
		Config:  cfg,
		Resolve: t.resolve,
		Run: func(ctx context.Context, routed routedOutbound, st *AttemptState) (*http.Response, error) {
			inFlight := routed.Route
			t.current.Store(&inFlight)
			if t.onRun != nil {
				t.onRun(routed)
			}
			attemptReq := req.Clone(withAttemptState(ctx, st))
			attemptReq = attemptReq.WithContext(
				httptrace.WithClientTrace(attemptReq.Context(), st.clientTrace()))
			if t.tlsTrace != nil {
				if trace := t.tlsTrace(routed); trace != nil {
					attemptReq = attemptReq.WithContext(
						httptrace.WithClientTrace(attemptReq.Context(), trace))
				}
			}
			return t.transport(routed).RoundTrip(attemptReq)
		},
		Classify: classifyEstablishmentFailure,
		// An abandoned attempt still finishes in the background; without this its
		// response body would never be closed and the connection would stay out
		// of the pool.
		Cleanup: func(resp *http.Response) {
			if resp != nil {
				resp.Body.Close()
			}
		},
		OnAttempt: t.onAttempt,
	})
	t.result = &result

	if result.Value != nil {
		return result.Value, nil
	}
	if result.RouteErr != nil {
		return nil, errNoRouteForAttempt
	}
	return nil, result.LastErr
}
