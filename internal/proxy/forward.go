package proxy

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/routing"
)

// ForwardProxyConfig holds dependencies for the forward proxy.
type ForwardProxyConfig struct {
	ProxyToken        string
	AuthVersion       string
	Router            *routing.Router
	Pool              outbound.PoolAccessor
	Health            HealthRecorder
	Events            EventEmitter
	MetricsSink       MetricsEventSink
	OutboundTransport OutboundTransportConfig
	TransportPool     *OutboundTransportPool
	ProxyBypassRules  []string
	// StickyAccountSource derives the sticky account from the request target
	// when the client supplies no account. See platform.NormalizeForwardStickyAccount.
	StickyAccountSource string
	// Failover controls request-level retries on other nodes. Left zero-valued
	// it means no retries, preserving the previous behaviour.
	Failover FailoverConfig
}

// ForwardProxy implements an HTTP forward proxy with Proxy-Authorization
// authentication, HTTP request forwarding, and CONNECT tunneling.
type ForwardProxy struct {
	token               string
	authVersion         config.AuthVersion
	router              *routing.Router
	pool                outbound.PoolAccessor
	health              HealthRecorder
	events              EventEmitter
	metricsSink         MetricsEventSink
	transportConfig     OutboundTransportConfig
	transportPool       *OutboundTransportPool
	transportPoolOnce   sync.Once
	directTransport     *http.Transport
	directOnce          sync.Once
	bypass              *TargetBypassMatcher
	stickyAccountSource platform.ForwardStickyAccount
	failover            FailoverConfig
}

// NewForwardProxy creates a new forward proxy handler.
func NewForwardProxy(cfg ForwardProxyConfig) *ForwardProxy {
	ev := cfg.Events
	if ev == nil {
		ev = NoOpEventEmitter{}
	}
	transportCfg := normalizeOutboundTransportConfig(cfg.OutboundTransport)
	transportPool := cfg.TransportPool
	if transportPool == nil {
		transportPool = NewOutboundTransportPool(transportCfg)
	}
	authVersion := config.NormalizeAuthVersion(cfg.AuthVersion)
	if authVersion == "" {
		authVersion = config.AuthVersionLegacyV0
	}
	stickyAccountSource, _ := platform.NormalizeForwardStickyAccount(cfg.StickyAccountSource)
	return &ForwardProxy{
		token:               cfg.ProxyToken,
		authVersion:         authVersion,
		router:              cfg.Router,
		pool:                cfg.Pool,
		health:              cfg.Health,
		events:              ev,
		metricsSink:         cfg.MetricsSink,
		transportConfig:     transportCfg,
		transportPool:       transportPool,
		bypass:              NewTargetBypassMatcher(cfg.ProxyBypassRules),
		stickyAccountSource: stickyAccountSource,
		failover:            cfg.Failover,
	}
}

// applyStickyAccountSource fills in the sticky account from the request target
// when the client did not provide one.
func (p *ForwardProxy) applyStickyAccountSource(account, target string) string {
	if p == nil {
		return account
	}
	return resolveForwardStickyAccount(p.stickyAccountSource, account, target)
}

func (p *ForwardProxy) outboundHTTPTransport(routed routedOutbound) *http.Transport {
	p.transportPoolOnce.Do(func() {
		if p.transportPool == nil {
			p.transportPool = NewOutboundTransportPool(p.transportConfig)
		}
	})
	return p.transportPool.Get(routed.Route.NodeHash, routed.Outbound, p.metricsSink)
}

func (p *ForwardProxy) directHTTPTransport() *http.Transport {
	p.directOnce.Do(func() {
		p.directTransport = newDirectHTTPTransport(p.transportConfig, p.metricsSink)
	})
	return p.directTransport
}

func (p *ForwardProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.handleCONNECT(w, r)
	} else {
		p.handleHTTP(w, r)
	}
}

func (p *ForwardProxy) effectiveAuthVersion() config.AuthVersion {
	if p == nil {
		return config.AuthVersionLegacyV0
	}
	if p.authVersion == config.AuthVersionV1 {
		return config.AuthVersionV1
	}
	return config.AuthVersionLegacyV0
}

// authenticate parses Proxy-Authorization and returns (platformName, account, error).
func (p *ForwardProxy) authenticate(r *http.Request) (string, string, *ProxyError) {
	if p.effectiveAuthVersion() == config.AuthVersionV1 {
		return p.authenticateV1(r)
	}
	return p.authenticateLegacy(r)
}

func (p *ForwardProxy) authenticateLegacy(r *http.Request) (string, string, *ProxyError) {
	auth := r.Header.Get("Proxy-Authorization")

	// Empty configured proxy token means auth is intentionally disabled.
	// In this mode, Proxy-Authorization is optional; when present and parseable,
	// we still extract Platform:Account identity.
	// Accepted credential formats in Basic payload:
	// 1) "platform:account" (two fields)
	// 2) "token:platform:account" (legacy three-field shape)
	if p.token == "" {
		platName, account, ok := parseProxyAuthorizationIdentityWhenAuthDisabledLegacy(auth)
		if !ok {
			return "", "", nil
		}
		return platName, account, nil
	}

	user, pass, ok := parseProxyAuthorizationLegacy(auth)
	if !ok {
		return "", "", ErrAuthRequired
	}
	if user != p.token {
		return "", "", ErrAuthFailed
	}

	platName, account := parseLegacyPlatformAccountIdentity(pass)
	return platName, account, nil
}

// parseProxyAuthorizationLegacy parses legacy Basic payload:
// "PROXY_TOKEN:Platform:Account".
//
// This parser is intentionally legacy-only and must not be reused by V1 code.
func parseProxyAuthorizationLegacy(auth string) (user string, pass string, ok bool) {
	credential, ok := parseProxyAuthorizationCredentialLegacy(auth)
	if !ok {
		return "", "", false
	}

	// Legacy format: user:pass where user=PROXY_TOKEN, pass=Platform:Account.
	// Split on first ":" to get user and pass.
	colonIdx := strings.IndexByte(credential, ':')
	if colonIdx < 0 {
		return "", "", false
	}
	user = credential[:colonIdx]
	pass = credential[colonIdx+1:]
	return user, pass, true
}

func (p *ForwardProxy) authenticateV1(r *http.Request) (string, string, *ProxyError) {
	auth := r.Header.Get("Proxy-Authorization")
	if p.token == "" {
		credential, ok := parseProxyAuthorizationCredentialV1(auth)
		if !ok {
			return "", "", nil
		}
		platName, account := parseForwardCredentialV1WhenAuthDisabled(credential)
		return platName, account, nil
	}

	credential, ok := parseProxyAuthorizationCredentialV1(auth)
	if !ok {
		return "", "", ErrAuthRequired
	}
	token, platName, account := parseForwardCredentialV1(credential)
	if token != p.token {
		return "", "", ErrAuthFailed
	}
	return platName, account, nil
}

func parseProxyAuthorizationIdentityWhenAuthDisabledLegacy(auth string) (platName string, account string, ok bool) {
	credential, ok := parseProxyAuthorizationCredentialLegacy(auth)
	if !ok {
		return "", "", false
	}
	platName, account = parseLegacyAuthDisabledIdentityCredential(credential)
	return platName, account, true
}

// parseProxyAuthorizationCredentialLegacy decodes Basic credential for
// LEGACY_V0 forward-auth flows.
//
// This function intentionally duplicates V1 decoding logic so legacy and V1
// parsing paths remain structurally isolated for future legacy removal.
func parseProxyAuthorizationCredentialLegacy(auth string) (string, bool) {
	if auth == "" {
		return "", false
	}

	// Expect "<scheme> <base64>"; scheme is case-insensitive per RFC.
	authFields := strings.Fields(auth)
	if len(authFields) != 2 || !strings.EqualFold(authFields[0], "Basic") {
		return "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(authFields[1])
	if err != nil {
		return "", false
	}
	return string(decoded), true
}

// parseProxyAuthorizationCredentialV1 decodes Basic credential for V1
// forward-auth flows.
//
// This function intentionally duplicates legacy decoding logic so V1 remains
// independent from LEGACY_V0 parser implementation.
func parseProxyAuthorizationCredentialV1(auth string) (string, bool) {
	if auth == "" {
		return "", false
	}

	// Expect "<scheme> <base64>"; scheme is case-insensitive per RFC.
	authFields := strings.Fields(auth)
	if len(authFields) != 2 || !strings.EqualFold(authFields[0], "Basic") {
		return "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(authFields[1])
	if err != nil {
		return "", false
	}
	return string(decoded), true
}

// hop-by-hop headers that must not be forwarded to the next hop.
var hopByHopHeaders = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"TE",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// stripHopByHopHeaders removes hop-by-hop headers from a header map,
// including any headers listed in the Connection header.
func stripHopByHopHeaders(header http.Header) {
	if header == nil {
		return
	}
	// Remove custom headers listed in Connection.
	for _, connHeaders := range header.Values("Connection") {
		for _, h := range strings.Split(connHeaders, ",") {
			if h = strings.TrimSpace(h); h != "" {
				header.Del(h)
			}
		}
	}
	for _, h := range hopByHopHeaders {
		header.Del(h)
	}
}

// copyEndToEndHeaders copies only end-to-end headers from src to dst and
// returns the canonical wire-format header length after filtering.
func copyEndToEndHeaders(dst, src http.Header) int64 {
	if dst == nil || src == nil {
		return 0
	}
	headers := src.Clone()
	stripHopByHopHeaders(headers)
	totalLen := headerWireLen(headers)
	for k, vv := range headers {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
	return totalLen
}

// prepareForwardOutboundRequest clones an inbound forward-proxy request into a
// client request suitable for http.Transport.RoundTrip.
func prepareForwardOutboundRequest(in *http.Request) *http.Request {
	req := in.Clone(in.Context())
	req.RequestURI = ""
	// Do not propagate client-side close semantics to upstream transport reuse.
	req.Close = false
	stripHopByHopHeaders(req.Header)
	return req
}

func (p *ForwardProxy) handleHTTP(w http.ResponseWriter, r *http.Request) {
	platName, account, authErr := p.authenticate(r)
	if authErr != nil {
		writeProxyError(w, authErr)
		return
	}

	account = p.applyStickyAccountSource(account, r.Host)

	lifecycle := newRequestLifecycle(p.events, r, ProxyTypeForward, false)
	lifecycle.setTarget(r.Host, r.URL.String())
	defer lifecycle.finish()
	lifecycle.setAccount(account)

	if p.bypass != nil && p.bypass.ShouldBypass(r.Host) {
		// Bypassed traffic has no node to fail over to.
		p.forwardDirect(w, r, lifecycle)
		return
	}
	p.forwardViaNodes(w, r, lifecycle, platName, account)
}

// forwardAttempt carries one attempt's response together with the counters that
// belong to it. Returning them instead of stashing them in the enclosing scope
// matters: an abandoned attempt is still running in the background, and letting
// it write to shared counters would race with the attempt that replaces it.
type forwardAttempt struct {
	resp        *http.Response
	trace       *upstreamRequestTrace
	headerBytes int64
	bodyCounter *countingReadCloser
}

// forwardDirect sends a bypassed request through the local transport. There is
// no node involved, so there is nothing to fail over to.
func (p *ForwardProxy) forwardDirect(
	w http.ResponseWriter,
	r *http.Request,
	lifecycle *requestLifecycle,
) {
	upstreamTrace := newUpstreamRequestTrace(lifecycle.markFirstByteReceived)
	outReq := prepareForwardOutboundRequest(r)
	outReq = outReq.WithContext(httptrace.WithClientTrace(outReq.Context(), upstreamTrace.clientTrace()))
	// Guarded assignment, not wrapForwardBody(...) directly: assigning a nil
	// *countingReadCloser to the io.ReadCloser field yields a non-nil interface
	// holding a nil pointer, and net/http then calls Read on it and panics.
	if bodyCounter := wrapForwardBody(outReq.Body); bodyCounter != nil {
		outReq.Body = bodyCounter
	}

	resp, err := p.directHTTPTransport().RoundTrip(outReq)
	if err != nil {
		proxyErr := classifyUpstreamError(err)
		if proxyErr == nil {
			// The client went away; not a failure worth recording.
			lifecycle.setNetOK(true)
			return
		}
		lifecycle.setProxyError(proxyErr)
		lifecycle.setUpstreamError("forward_roundtrip", err)
		lifecycle.setHTTPStatus(proxyErr.HTTPCode)
		writeProxyError(w, proxyErr)
		return
	}
	defer resp.Body.Close()
	p.writeForwardUpstreamResponse(w, r, lifecycle, resp, routing.RouteResult{}, false)
}

// forwardViaNodes sends a request through the node pool, retrying on a different
// node only when the request provably never reached the first one.
func (p *ForwardProxy) forwardViaNodes(
	w http.ResponseWriter,
	r *http.Request,
	lifecycle *requestLifecycle,
	platName, account string,
) {
	domain := netutil.ExtractDomain(r.Host)

	cfg := p.failover
	outReq := prepareForwardOutboundRequest(r)
	// A body cannot be replayed: server-side requests never carry GetBody, so
	// anything with a body can only honestly be sent once.
	if !cfg.Enabled || replayUnsafe(outReq) {
		cfg.MaxAttempts = 1
	}

	result := runFailover(r.Context(), FailoverParams[*forwardAttempt]{
		Config: cfg,
		Resolve: func(exclude []node.Hash) (routedOutbound, *ProxyError) {
			return resolveRoutedOutboundExcluding(p.router, p.pool, platName, account, r.Host, exclude)
		},
		Run: func(ctx context.Context, routed routedOutbound, st *AttemptState) (*forwardAttempt, error) {
			// Trace and counters are per attempt: sharing them across attempts
			// would report the same bytes and first-byte delay several times.
			trace := newUpstreamRequestTrace(lifecycle.markFirstByteReceived)
			attemptReq := outReq.Clone(withAttemptState(ctx, st))
			attemptReq = attemptReq.WithContext(
				httptrace.WithClientTrace(attemptReq.Context(), trace.clientTrace()))
			// Separate trace so the attempt learns whether the transport handed
			// over a connection, and whether that connection was pooled.
			attemptReq = attemptReq.WithContext(
				httptrace.WithClientTrace(attemptReq.Context(), st.clientTrace()))
			var counter *countingReadCloser
			if bodyCounter := wrapForwardBody(attemptReq.Body); bodyCounter != nil {
				counter = bodyCounter
				attemptReq.Body = bodyCounter
			}

			if p.health != nil {
				go p.health.RecordPassiveLatency(routed.Route.NodeHash, domain, nil)
			}
			resp, err := p.outboundHTTPTransport(routed).RoundTrip(attemptReq)
			if err != nil {
				return nil, err
			}
			return &forwardAttempt{
				resp:        resp,
				trace:       trace,
				headerBytes: headerWireLen(attemptReq.Header),
				bodyCounter: counter,
			}, nil
		},
		Classify: classifyEstablishmentFailure,
		Cleanup: func(attempt *forwardAttempt) {
			if attempt != nil && attempt.resp != nil {
				attempt.resp.Body.Close()
			}
		},
		OnAttempt: func(res routing.RouteResult, verdict attemptVerdict) {
			if p.health == nil {
				return
			}
			if verdict.retryable {
				// The node never received the request, so it failed to serve it.
				recordPassiveStageResultAsync(p.health, res, node.PassiveStageConnect, false)
				return
			}
			if verdict.deadConn {
				// Weight the node down without touching the breaker: a batch of
				// pooled connections ageing out together should not evict a node.
				recordConnDropAsync(p.health, res)
			}
		},
	})

	if result.Value != nil {
		attempt := result.Value
		defer attempt.resp.Body.Close()
		lifecycle.setRouteResult(result.Route)
		if attempt.trace.shouldCommitEgress() {
			lifecycle.addEgressBytes(attempt.headerBytes)
			if attempt.bodyCounter != nil {
				lifecycle.addEgressBytes(attempt.bodyCounter.Total())
			}
		}
		p.writeForwardUpstreamResponse(w, r, lifecycle, attempt.resp, result.Route, true)
		return
	}

	if result.RouteErr != nil {
		lifecycle.setProxyError(result.RouteErr)
		lifecycle.setHTTPStatus(result.RouteErr.HTTPCode)
		writeProxyError(w, result.RouteErr)
		return
	}

	proxyErr := classifyUpstreamError(result.LastErr)
	if proxyErr == nil {
		// The client went away before the upstream responded, so this is not a
		// node failure and health must be left alone.
		lifecycle.setNetOK(true)
		return
	}
	lifecycle.setProxyError(proxyErr)
	lifecycle.setUpstreamError("forward_roundtrip", result.LastErr)
	lifecycle.setHTTPStatus(proxyErr.HTTPCode)
	// A retryable failure was already reported through OnAttempt as each attempt
	// ended; only a non-retryable one still needs reporting. Recording both
	// would count the same node twice at full weight, and that feeds the
	// breaker — enough of it would evict a node for one failed request.
	if !result.LastVerdict.retryable && len(result.FailedNodes) > 0 {
		stage := node.PassiveStageConnect
		if result.Abandoned {
			// Ran out of budget, which usually means a slow origin rather than
			// an unreachable node, so it is the weaker signal.
			stage = node.PassiveStageTransfer
		}
		last := result.FailedNodes[len(result.FailedNodes)-1]
		recordPassiveStageResultAsync(p.health, routing.RouteResult{NodeHash: last}, stage, false)
	}
	writeProxyError(w, proxyErr)
}

func wrapForwardBody(body io.ReadCloser) *countingReadCloser {
	if body == nil || body == http.NoBody {
		return nil
	}
	return newCountingReadCloser(body)
}

func (p *ForwardProxy) writeForwardUpstreamResponse(
	w http.ResponseWriter,
	r *http.Request,
	lifecycle *requestLifecycle,
	resp *http.Response,
	route routing.RouteResult,
	hasRoute bool,
) {
	lifecycle.setHTTPStatus(resp.StatusCode)
	lifecycle.setNetOK(true)

	// Copy end-to-end response headers and body.
	lifecycle.addIngressBytes(copyEndToEndHeaders(w.Header(), resp.Header))
	w.WriteHeader(resp.StatusCode)
	copiedBytes, copyErr := io.Copy(w, resp.Body)
	lifecycle.addIngressBytes(copiedBytes)
	if copyErr != nil {
		if shouldRecordForwardCopyFailure(r, copyErr) {
			lifecycle.setProxyError(ErrUpstreamRequestFailed)
			lifecycle.setUpstreamError("forward_upstream_to_client_copy", copyErr)
			lifecycle.setNetOK(false)
			if hasRoute {
				// The response had already started arriving, so this is a weaker
				// signal about the node than a failure to reach it at all.
				recordPassiveStageResultAsync(p.health, route, node.PassiveStageTransfer, false)
			}
		}
		return
	}

	// Full body transfer succeeded — count as network success even for 5xx HTTP.
	if hasRoute {
		recordPassiveResultAsync(p.health, route, true)
	}
}

func (p *ForwardProxy) handleCONNECT(w http.ResponseWriter, r *http.Request) {
	target := r.Host
	platName, account, authErr := p.authenticate(r)
	if authErr != nil {
		writeProxyError(w, authErr)
		return
	}
	account = p.applyStickyAccountSource(account, target)

	lifecycle := newRequestLifecycle(p.events, r, ProxyTypeForward, true)
	lifecycle.setTarget(target, "")
	defer lifecycle.finish()
	lifecycle.setAccount(account)

	prepare := prepareConnectTunnel(
		r.Context(),
		p.tunnelDeps(),
		platName,
		account,
		target,
	)
	if prepare.route.PlatformID != "" {
		lifecycle.setRouteResult(prepare.route)
	}
	if prepare.session == nil {
		if prepare.proxyErr != nil {
			lifecycle.setProxyError(prepare.proxyErr)
			if prepare.upstreamStage != "" {
				lifecycle.setUpstreamError(prepare.upstreamStage, prepare.upstreamErr)
			}
			lifecycle.setHTTPStatus(prepare.proxyErr.HTTPCode)
			writeProxyError(w, prepare.proxyErr)
		} else if prepare.canceled {
			lifecycle.setNetOK(true)
		}
		return
	}

	// Hijack the client connection.
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		prepare.session.upstreamConn.Close()
		lifecycle.setProxyError(ErrUpstreamRequestFailed)
		lifecycle.setUpstreamError("connect_hijack", errors.New("response writer does not support hijacking"))
		lifecycle.setHTTPStatus(ErrUpstreamRequestFailed.HTTPCode)
		prepare.session.recordResult(false)
		writeProxyError(w, ErrUpstreamRequestFailed)
		return
	}

	clientConn, clientBuf, err := hijacker.Hijack()
	if err != nil {
		prepare.session.upstreamConn.Close()
		lifecycle.setProxyError(ErrUpstreamRequestFailed)
		lifecycle.setUpstreamError("connect_hijack", err)
		prepare.session.recordResult(false)
		return
	}

	// Write the raw CONNECT success line with proper reason phrase.
	if _, err := clientBuf.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		prepare.session.upstreamConn.Close()
		clientConn.Close()
		lifecycle.setProxyError(ErrUpstreamRequestFailed)
		lifecycle.setUpstreamError("connect_client_response_write", err)
		lifecycle.setNetOK(false)
		return
	}
	if err := clientBuf.Flush(); err != nil {
		prepare.session.upstreamConn.Close()
		clientConn.Close()
		lifecycle.setProxyError(ErrUpstreamRequestFailed)
		lifecycle.setUpstreamError("connect_client_response_flush", err)
		lifecycle.setNetOK(false)
		return
	}
	lifecycle.setHTTPStatus(http.StatusOK)
	relay := pumpPreparedTunnel(clientConn, clientBuf.Reader, prepare.session, tunnelPumpOptions{
		requireBidirectionalTraffic: true,
		onFirstIngressByte:          lifecycle.markFirstByteReceived,
	})
	lifecycle.addIngressBytes(relay.ingressBytes)
	lifecycle.addEgressBytes(relay.egressBytes)
	if relay.proxyErr != nil {
		lifecycle.setProxyError(relay.proxyErr)
		lifecycle.setUpstreamError(relay.upstreamStage, relay.upstreamErr)
	}
	lifecycle.setNetOK(relay.netOK)
	prepare.session.recordResult(relay.netOK)
}

// shouldRecordForwardCopyFailure decides whether an HTTP response body copy
// error should be treated as an upstream/node failure.
func shouldRecordForwardCopyFailure(r *http.Request, copyErr error) bool {
	if copyErr == nil {
		return false
	}
	// Client-side cancellation while streaming should not penalise node health.
	if r != nil && errors.Is(r.Context().Err(), context.Canceled) {
		return false
	}
	return classifyUpstreamError(copyErr) != nil
}

// tunnelDeps builds the dependencies for CONNECT tunnelling, including the dial
// timeout and failover settings that keep a non-responsive node from parking the
// connection.
func (p *ForwardProxy) tunnelDeps() tunnelDeps {
	return tunnelDeps{
		router:      p.router,
		pool:        p.pool,
		health:      p.health,
		metricsSink: p.metricsSink,
		bypass:      p.bypass,
		dialTimeout: dialTimeoutFor(p.transportConfig),
		failover:    p.failover,
	}
}
