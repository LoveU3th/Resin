package routing

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/puzpuzpuz/xsync/v4"
)

var (
	ErrPlatformNotFound = errors.New("platform not found")
)

type PoolAccessor interface {
	GetEntry(hash node.Hash) (*node.NodeEntry, bool)
	GetPlatform(id string) (*platform.Platform, bool)
	GetPlatformByName(name string) (*platform.Platform, bool)
	RangePlatforms(fn func(*platform.Platform) bool)
}

// Router handles route selection and lease management.
type Router struct {
	pool            PoolAccessor
	states          *xsync.Map[string, *PlatformRoutingState]
	authorities     func() []string
	p2cWindow       func() time.Duration
	onLeaseEvent    LeaseEventFunc
	nodeTagResolver func(node.Hash) string
	// Health tuning, all optional: nil means health does not affect routing.
	healthPenaltyMs              func() int
	healthFilterThresholdPercent func() int
	healthMinSamplesForFilter    func() int
	// Sticky strict rebind tuning, all optional: nil leaves the behaviour off.
	stickyStrictRebindEnabled    func() bool
	stickyStrictThresholdPercent func() int
	stickyStrictFallbackPercent  func() int
	stickyStrictCooldown         func() time.Duration
}

type RouterConfig struct {
	Pool        PoolAccessor
	Authorities func() []string
	P2CWindow   func() time.Duration
	// OnLeaseEvent is called synchronously; handlers must stay lightweight.
	OnLeaseEvent LeaseEventFunc
	// NodeTagResolver resolves a node hash to its display tag ("<Sub>/<Tag>").
	// If nil, NodeTag will be empty.
	NodeTagResolver func(node.Hash) string
	// Health tuning for node scoring. All optional; nil leaves health out of
	// routing decisions entirely.
	HealthPenaltyMs              func() int
	HealthFilterThresholdPercent func() int
	HealthMinSamplesForFilter    func() int
	// Sticky strict rebind: moves a lease whose node failed to connect into the
	// healthy pool on the account's next request. All optional; nil leaves the
	// behaviour off.
	StickyStrictRebindEnabled    func() bool
	StickyStrictThresholdPercent func() int
	StickyStrictFallbackPercent  func() int
	StickyStrictCooldown         func() time.Duration
}

func NewRouter(cfg RouterConfig) *Router {
	return &Router{
		pool:                         cfg.Pool,
		states:                       xsync.NewMap[string, *PlatformRoutingState](),
		authorities:                  cfg.Authorities,
		p2cWindow:                    cfg.P2CWindow,
		onLeaseEvent:                 cfg.OnLeaseEvent,
		nodeTagResolver:              cfg.NodeTagResolver,
		healthPenaltyMs:              cfg.HealthPenaltyMs,
		healthFilterThresholdPercent: cfg.HealthFilterThresholdPercent,
		healthMinSamplesForFilter:    cfg.HealthMinSamplesForFilter,
		stickyStrictRebindEnabled:    cfg.StickyStrictRebindEnabled,
		stickyStrictThresholdPercent: cfg.StickyStrictThresholdPercent,
		stickyStrictFallbackPercent:  cfg.StickyStrictFallbackPercent,
		stickyStrictCooldown:         cfg.StickyStrictCooldown,
	}
}

// healthWeights reads the current health tuning. Nil accessors yield zero
// values, which disable the corresponding behaviour.
func (r *Router) healthWeights() HealthWeights {
	var w HealthWeights
	if r.healthPenaltyMs != nil {
		// Scoring works in nanoseconds, so convert the configured milliseconds.
		w.PenaltyNs = float64(r.healthPenaltyMs()) * float64(time.Millisecond)
	}
	if r.healthFilterThresholdPercent != nil {
		w.FilterThresholdPercent = r.healthFilterThresholdPercent()
	}
	if r.healthMinSamplesForFilter != nil {
		w.MinSamplesForFilter = r.healthMinSamplesForFilter()
	}
	return w
}

// stickyStrict reads the current strict-pool tuning used when a lease has to be
// moved off a node that failed to connect.
//
// The sample requirement is taken from the health filter setting on purpose:
// both answer "how many observations before this score means something", and
// having two knobs for one notion would only invite them to disagree.
func (r *Router) stickyStrict() StrictPolicy {
	var p StrictPolicy
	if r.stickyStrictThresholdPercent != nil {
		p.ThresholdPercent = r.stickyStrictThresholdPercent()
	}
	if r.stickyStrictFallbackPercent != nil {
		p.FallbackPercent = r.stickyStrictFallbackPercent()
	}
	if r.healthMinSamplesForFilter != nil {
		p.MinSamples = r.healthMinSamplesForFilter()
	}
	return p
}

// stickyRebindEnabled reports whether a failed attempt on a lease node should
// arm a rebind. An unconfigured router leaves it off, like the health tuning.
func (r *Router) stickyRebindEnabled() bool {
	return r.stickyStrictRebindEnabled != nil && r.stickyStrictRebindEnabled()
}

// stickyCooldownNs is how long an armed rebind stays active.
func (r *Router) stickyCooldownNs() int64 {
	if r.stickyStrictCooldown == nil {
		return 0
	}
	return int64(r.stickyStrictCooldown())
}

type RouteResult struct {
	PlatformID   string
	PlatformName string
	NodeHash     node.Hash
	EgressIP     netip.Addr
	NodeTag      string // display tag: "<Subscription>/<Tag>" (DESIGN.md §601)
	LeaseCreated bool
	// Borrowed means the node was handed out for this request only, without
	// touching the caller's lease. Used when a request has to avoid its sticky
	// node: skipping the lease write keeps the egress IP stable across the
	// requests that do not fail.
	Borrowed bool
}

// RouteOptions carries per-request routing constraints.
type RouteOptions struct {
	// Exclude lists nodes this request must not use, because it already failed
	// on them. Empty for ordinary requests.
	Exclude []node.Hash
}

const livePickAttempts = 2 // first pick + one retry

type leaseInvalidationReason int

const (
	leaseInvalidationNone leaseInvalidationReason = iota
	leaseInvalidationExpire
	leaseInvalidationRemove
)

func (r *Router) RouteRequest(platName, account, target string) (RouteResult, error) {
	return r.RouteRequestExcluding(platName, account, target, RouteOptions{})
}

// RouteRequestExcluding behaves like RouteRequest but avoids the given nodes.
// Callers use it when retrying a request on a different node.
func (r *Router) RouteRequestExcluding(
	platName, account, target string,
	opt RouteOptions,
) (RouteResult, error) {
	plat, err := r.resolvePlatform(platName)
	if err != nil {
		return RouteResult{}, err
	}

	targetDomain := netutil.ExtractDomain(target)
	state := r.ensurePlatformState(plat.ID)
	var result RouteResult
	if account == "" {
		result, err = r.routeRandom(plat, state, targetDomain, opt)
	} else {
		result, err = r.routeSticky(plat, state, account, targetDomain, time.Now(), opt)
	}
	if err != nil {
		return RouteResult{}, err
	}
	result = withPlatformContext(plat, result)
	if r.nodeTagResolver != nil {
		result.NodeTag = r.nodeTagResolver(result.NodeHash)
	}
	return result, nil
}

func withPlatformContext(plat *platform.Platform, res RouteResult) RouteResult {
	res.PlatformID = plat.ID
	res.PlatformName = plat.Name
	return res
}

func (r *Router) resolvePlatform(platName string) (*platform.Platform, error) {
	if platName == "" {
		if p, ok := r.pool.GetPlatform(platform.DefaultPlatformID); ok {
			return p, nil
		}
		return nil, ErrPlatformNotFound
	}
	p, ok := r.pool.GetPlatformByName(platName)
	if !ok {
		return nil, ErrPlatformNotFound
	}
	return p, nil
}

func (r *Router) ensurePlatformState(platformID string) *PlatformRoutingState {
	state, _ := r.states.LoadOrCompute(platformID, func() (*PlatformRoutingState, bool) {
		return NewPlatformRoutingState(), false
	})
	return state
}

func (r *Router) routeRandom(
	plat *platform.Platform,
	state *PlatformRoutingState,
	targetDomain string,
	opt RouteOptions,
) (RouteResult, error) {
	h, entry, err := r.selectLiveRandomRoute(plat, state.IPLoadStats, targetDomain, opt.Exclude)
	if err != nil {
		return RouteResult{}, err
	}
	return RouteResult{
		NodeHash:     h,
		EgressIP:     entry.GetEgressIP(),
		LeaseCreated: false,
	}, nil
}

func (r *Router) routeSticky(
	plat *platform.Platform,
	state *PlatformRoutingState,
	account string,
	targetDomain string,
	now time.Time,
	opt RouteOptions,
) (RouteResult, error) {
	nowNs := now.UnixNano()
	var result RouteResult
	var routeErr error

	_, _ = state.Leases.leases.Compute(account, func(current Lease, loaded bool) (Lease, xsync.ComputeOp) {
		newLease, op, routeResult, err := r.decideStickyLease(
			plat,
			state,
			account,
			targetDomain,
			now,
			nowNs,
			current,
			loaded,
			opt.Exclude,
		)
		if err != nil {
			routeErr = err
			return newLease, op
		}
		result = routeResult
		return newLease, op
	})

	return result, routeErr
}

func (r *Router) decideStickyLease(
	plat *platform.Platform,
	state *PlatformRoutingState,
	account string,
	targetDomain string,
	now time.Time,
	nowNs int64,
	current Lease,
	loaded bool,
	exclude []node.Hash,
) (Lease, xsync.ComputeOp, RouteResult, error) {
	hadPreviousLease := loaded
	invalidation := leaseInvalidationNone

	if loaded && current.IsExpired(now) {
		invalidation = leaseInvalidationExpire
		loaded = false
	}

	if loaded {
		excluded := containsHash(exclude, current.NodeHash)

		// A lease whose node could not be reached is moved into the strict pool
		// before it is served again, so the account stops going back to a node
		// that is known to be unreachable. Only on a fresh request: during a
		// retry the lease is deliberately left alone (see the borrow branch
		// below), and moving it mid-request would relocate the account's egress
		// IP for a failure this request has already worked around.
		if !excluded && current.NeedsStrictRebind(nowNs) {
			if rebound, reboundResult, ok := r.rebindLease(
				plat, state, account, targetDomain, nowNs, current, exclude,
			); ok {
				return rebound, xsync.UpdateOp, reboundResult, nil
			}
			// Nothing eligible: keep the mark and serve the lease anyway. The
			// deadline is what stops a pool with no healthy node from being
			// rescanned on every single request.
		}

		// A node whose own score says it is failing must not be handed out on a
		// lease hit — but the account must not be relocated either, so this
		// request borrows another node. Checked before the hit is recorded on
		// purpose: recording it would emit a LeaseTouch for a request the node
		// does not actually serve.
		if !excluded && r.leaseNodeStillServes(plat, current) &&
			r.healthWeights().rejects(r.pool, current.NodeHash) {
			if borrowed, ok := r.borrowRoute(plat, state, current, targetDomain, nowNs,
				excludeAlso(exclude, current.NodeHash)); ok {
				return current, xsync.CancelOp, borrowed, nil
			}
			// Nothing to borrow — a single-node platform, or every alternative
			// is filtered out too. Fall through and keep serving the lease:
			// health must never turn a routable lease into a failed request,
			// nor into lease churn.
		}

		hitLease, hitResult, hitOK := Lease{}, RouteResult{}, false
		if !excluded {
			hitLease, hitResult, hitOK = r.tryLeaseHit(plat, account, current, nowNs)
		}
		if hitOK {
			return hitLease, xsync.UpdateOp, hitResult, nil
		}

		if excluded {
			if borrowed, ok := r.borrowRoute(plat, state, current, targetDomain, nowNs, exclude); ok {
				// The sticky node is unusable for this request only. Hand out another
				// node without touching the lease (CancelOp), so a single failure
				// does not relocate the account's egress IP.
				return current, xsync.CancelOp, borrowed, nil
			}
		}
		if newLease, rotatedResult, ok := r.tryLeaseSameIPRotation(plat, account, current, targetDomain, nowNs, exclude); ok {
			return newLease, xsync.UpdateOp, rotatedResult, nil
		}
		invalidation = leaseInvalidationRemove
	}

	return r.createOrAbortStickyLease(
		plat,
		state,
		account,
		targetDomain,
		now,
		nowNs,
		current,
		hadPreviousLease,
		invalidation,
		exclude,
	)
}

func (r *Router) createOrAbortStickyLease(
	plat *platform.Platform,
	state *PlatformRoutingState,
	account string,
	targetDomain string,
	now time.Time,
	nowNs int64,
	previous Lease,
	hadPreviousLease bool,
	invalidation leaseInvalidationReason,
	exclude []node.Hash,
) (Lease, xsync.ComputeOp, RouteResult, error) {
	newLease, createdResult, err := r.createLease(plat, state, targetDomain, now, nowNs, exclude)
	if err != nil {
		// "No other node" is a property of this request's exclude list, not of
		// the account's lease. Keep the lease (CancelOp) so one failed attempt
		// cannot relocate the account's egress IP — a single-node platform
		// would otherwise lose its lease on the first retry and scatter
		// subsequent requests across new addresses.
		if len(exclude) > 0 && errors.Is(err, ErrNoAvailableNodes) {
			return previous, xsync.CancelOp, RouteResult{}, err
		}
		r.cleanupPreviousLease(state, previous, hadPreviousLease, invalidation, plat.ID, account)
		lease, op := abortLeaseCreate(previous, hadPreviousLease)
		return lease, op, RouteResult{}, err
	}

	r.cleanupPreviousLease(state, previous, hadPreviousLease, invalidation, plat.ID, account)
	state.IPLoadStats.Inc(newLease.EgressIP)
	r.emitLeaseEvent(LeaseEvent{
		Type:       LeaseCreate,
		PlatformID: plat.ID,
		Account:    account,
		NodeHash:   newLease.NodeHash,
		EgressIP:   newLease.EgressIP,
	})
	return newLease, xsync.UpdateOp, createdResult, nil
}

// leaseNodeStillServes reports whether the leased node would still be handed
// out on a hit: present in the platform's routable view and still answering
// from the same egress IP.
//
// Split out of tryLeaseHit so the health check can be made on the same premise
// without recording the hit. That matters: borrowing around a node the lease
// would never have hit would pin the account to a dead node for a whole TTL,
// where the old code rotated or recreated the lease straight away.
func (r *Router) leaseNodeStillServes(plat *platform.Platform, current Lease) bool {
	entry, ok := r.pool.GetEntry(current.NodeHash)
	if !ok || entry == nil {
		return false
	}
	return plat.View().Contains(current.NodeHash) && entry.GetEgressIP() == current.EgressIP
}

func (r *Router) tryLeaseHit(
	plat *platform.Platform,
	account string,
	current Lease,
	nowNs int64,
) (Lease, RouteResult, bool) {
	if !r.leaseNodeStillServes(plat, current) {
		return Lease{}, RouteResult{}, false
	}

	newLease := current
	newLease.LastAccessedNs = nowNs
	r.emitLeaseEvent(LeaseEvent{
		Type:       LeaseTouch,
		PlatformID: plat.ID,
		Account:    account,
		NodeHash:   current.NodeHash,
		EgressIP:   current.EgressIP,
	})
	return newLease, RouteResult{
		NodeHash:     current.NodeHash,
		EgressIP:     current.EgressIP,
		LeaseCreated: false,
	}, true
}

func (r *Router) tryLeaseSameIPRotation(
	plat *platform.Platform,
	account string,
	current Lease,
	targetDomain string,
	nowNs int64,
	exclude []node.Hash,
) (Lease, RouteResult, bool) {
	bestHash, ok := chooseSameIPRotationCandidate(
		plat,
		r.pool,
		current.EgressIP,
		targetDomain,
		r.authorities(),
		r.p2cWindow(),
		exclude,
		nil,
	)
	if !ok {
		return Lease{}, RouteResult{}, false
	}

	newLease := current
	newLease.NodeHash = bestHash
	newLease.LastAccessedNs = nowNs
	r.emitLeaseEvent(LeaseEvent{
		Type:       LeaseReplace,
		PlatformID: plat.ID,
		Account:    account,
		NodeHash:   bestHash,
		EgressIP:   current.EgressIP,
	})
	return newLease, RouteResult{
		NodeHash:     bestHash,
		EgressIP:     current.EgressIP,
		LeaseCreated: false,
	}, true
}

func (r *Router) createLease(
	plat *platform.Platform,
	state *PlatformRoutingState,
	targetDomain string,
	now time.Time,
	nowNs int64,
	exclude []node.Hash,
) (Lease, RouteResult, error) {
	h, entry, err := r.selectLiveRandomRoute(plat, state.IPLoadStats, targetDomain, exclude)
	if err != nil {
		return Lease{}, RouteResult{}, err
	}
	ttl := plat.StickyTTLNs
	if ttl <= 0 {
		ttl = int64(24 * time.Hour) // Default safeguard
	}

	lease := Lease{
		NodeHash:       h,
		EgressIP:       entry.GetEgressIP(),
		CreatedAtNs:    nowNs,
		ExpiryNs:       now.Add(time.Duration(ttl)).UnixNano(),
		LastAccessedNs: nowNs,
	}
	return lease, RouteResult{
		NodeHash:     lease.NodeHash,
		EgressIP:     lease.EgressIP,
		LeaseCreated: true,
	}, nil
}

func (r *Router) cleanupPreviousLease(
	state *PlatformRoutingState,
	lease Lease,
	hadPreviousLease bool,
	invalidation leaseInvalidationReason,
	platformID string,
	account string,
) {
	if !hadPreviousLease {
		return
	}
	state.Leases.stats.Dec(lease.EgressIP)
	switch invalidation {
	case leaseInvalidationExpire:
		r.emitLeaseEvent(LeaseEvent{
			Type:        LeaseExpire,
			PlatformID:  platformID,
			Account:     account,
			NodeHash:    lease.NodeHash,
			EgressIP:    lease.EgressIP,
			CreatedAtNs: lease.CreatedAtNs,
		})
	case leaseInvalidationRemove:
		r.emitLeaseEvent(LeaseEvent{
			Type:        LeaseRemove,
			PlatformID:  platformID,
			Account:     account,
			NodeHash:    lease.NodeHash,
			EgressIP:    lease.EgressIP,
			CreatedAtNs: lease.CreatedAtNs,
		})
	}
}

func abortLeaseCreate(current Lease, hadPreviousLease bool) (Lease, xsync.ComputeOp) {
	if hadPreviousLease {
		return current, xsync.DeleteOp
	}
	return current, xsync.CancelOp
}

func (r *Router) emitLeaseEvent(event LeaseEvent) {
	if r.onLeaseEvent != nil {
		r.onLeaseEvent(event)
	}
}

// MarkStickyNodeFailure records that a request could not be served because the
// node backing an account's sticky lease could not be reached. The account's
// next request then moves that lease into the strict pool instead of being
// served by the same node again.
//
// The failed hash is part of the check on purpose: a request that failed on a
// borrowed node leaves the lease alone, because the lease node is not the one
// that failed. Returns true when a lease was actually marked.
func (r *Router) MarkStickyNodeFailure(platformID, account string, failed node.Hash) bool {
	if account == "" || platformID == "" || !r.stickyRebindEnabled() {
		return false
	}
	cooldown := r.stickyCooldownNs()
	if cooldown <= 0 {
		return false
	}
	state, ok := r.states.Load(platformID)
	if !ok || state == nil {
		return false
	}

	deadline := time.Now().UnixNano() + cooldown
	marked := false
	state.Leases.leases.Compute(account, func(current Lease, loaded bool) (Lease, xsync.ComputeOp) {
		if !loaded || current.NodeHash != failed {
			return current, xsync.CancelOp
		}
		current.NeedsRebind = true
		current.StrictUntilNs = deadline
		marked = true
		return current, xsync.UpdateOp
	})
	return marked
}

// rebindLease moves a lease onto a node from the strict pool. It reports
// whether the lease was moved; false means no eligible node exists, and the
// caller keeps serving the current one rather than failing the request.
//
// The lease's own node is always excluded, so a rebind can never land back on
// the node that just failed.
func (r *Router) rebindLease(
	plat *platform.Platform,
	state *PlatformRoutingState,
	account string,
	targetDomain string,
	nowNs int64,
	current Lease,
	exclude []node.Hash,
) (Lease, RouteResult, bool) {
	// Copy rather than append: exclude belongs to the caller and is appended to
	// across attempts of one request.
	avoid := make([]node.Hash, 0, len(exclude)+1)
	avoid = append(avoid, exclude...)
	avoid = append(avoid, current.NodeHash)

	// A healthy node on the same egress IP keeps the address the site already
	// knows, which matters most when a rebind is part of a bulk migration.
	strict := r.stickyStrict()
	if h, ok := chooseSameIPRotationCandidate(
		plat,
		r.pool,
		current.EgressIP,
		targetDomain,
		r.authorities(),
		r.p2cWindow(),
		avoid,
		strictEligibility(strict),
	); ok {
		if entry, found := r.pool.GetEntry(h); found {
			return r.landRebind(plat, state, account, current, h, entry, nowNs)
		}
	}

	h, entry, err := r.selectLiveStrictRoute(plat, state.IPLoadStats, targetDomain, avoid)
	if err != nil {
		return Lease{}, RouteResult{}, false
	}
	return r.landRebind(plat, state, account, current, h, entry, nowNs)
}

// landRebind writes the moved lease. The original window is kept: moving a
// lease must not extend how long the account stays pinned to one address.
func (r *Router) landRebind(
	plat *platform.Platform,
	state *PlatformRoutingState,
	account string,
	current Lease,
	h node.Hash,
	entry *node.NodeEntry,
	nowNs int64,
) (Lease, RouteResult, bool) {
	egressIP := entry.GetEgressIP()
	// The per-IP lease count follows the address, not the node: a move between
	// two nodes that share an address leaves the count untouched.
	if egressIP != current.EgressIP {
		state.Leases.stats.Dec(current.EgressIP)
		state.Leases.stats.Inc(egressIP)
	}

	next := current
	next.NodeHash = h
	next.EgressIP = egressIP
	next.LastAccessedNs = nowNs
	next.NeedsRebind = false
	next.StrictUntilNs = 0

	r.emitLeaseEvent(LeaseEvent{
		Type:        LeaseReplace,
		PlatformID:  plat.ID,
		Account:     account,
		NodeHash:    next.NodeHash,
		EgressIP:    next.EgressIP,
		CreatedAtNs: next.CreatedAtNs,
	})
	return next, RouteResult{
		NodeHash: next.NodeHash,
		EgressIP: next.EgressIP,
	}, true
}

func (r *Router) selectLiveRandomRoute(
	plat *platform.Platform,
	stats *IPLoadStats,
	targetDomain string,
	exclude []node.Hash,
) (node.Hash, *node.NodeEntry, error) {
	var lastMissing node.Hash
	for i := 0; i < livePickAttempts; i++ {
		h, err := randomRoute(plat, stats, r.pool, targetDomain, r.authorities(), r.p2cWindow(), r.healthWeights(), exclude)
		if err != nil {
			return node.Zero, nil, err
		}
		entry, ok := r.pool.GetEntry(h)
		if ok {
			return h, entry, nil
		}
		lastMissing = h
	}
	if lastMissing != node.Zero {
		return node.Zero, nil, fmt.Errorf("%w: selected node %s no longer in pool", ErrNoAvailableNodes, lastMissing.Hex())
	}
	return node.Zero, nil, ErrNoAvailableNodes
}

// chooseSameIPRotationCandidate returns a node that shares targetIP and is not
// excluded, preferring the one with the best recent latency for the target.
//
// eligible, when non-nil, additionally filters candidates. Callers that rotate
// within a platform pass nil; a rebind passes the strict-pool predicate so the
// same-address preference never overrides the health requirement.
func chooseSameIPRotationCandidate(
	plat *platform.Platform,
	pool PoolAccessor,
	targetIP netip.Addr,
	targetDomain string,
	authorities []string,
	window time.Duration,
	exclude []node.Hash,
	eligible func(*node.NodeEntry) bool,
) (node.Hash, bool) {
	bestKnownHash := node.Zero
	bestKnownLatency := time.Duration(math.MaxInt64)
	fallbackHash := node.Zero

	plat.View().Range(func(h node.Hash) bool {
		entry, ok := pool.GetEntry(h)
		if !ok || entry.GetEgressIP() != targetIP || containsHash(exclude, h) {
			return true
		}
		if eligible != nil && !eligible(entry) {
			return true
		}
		if fallbackHash == node.Zero {
			fallbackHash = h
		}

		latency, hasLatency := sameIPCandidateLatency(entry, targetDomain, authorities, window)
		if hasLatency && latency < bestKnownLatency {
			bestKnownLatency = latency
			bestKnownHash = h
		}
		return true
	})

	if bestKnownHash != node.Zero {
		return bestKnownHash, true
	}
	if fallbackHash != node.Zero {
		return fallbackHash, true
	}
	return node.Zero, false
}

func sameIPCandidateLatency(
	entry *node.NodeEntry,
	targetDomain string,
	authorities []string,
	window time.Duration,
) (time.Duration, bool) {
	now := time.Now()
	if latency, ok := lookupRecentDomainLatency(entry, targetDomain, now, window); ok {
		return latency, true
	}

	if latency, ok := averageRecentAuthorityLatency(entry, authorities, now, window); ok {
		return latency, true
	}
	return 0, false
}

// ReadLease implements weak persistence read.
func (r *Router) ReadLease(key model.LeaseKey) *model.Lease {
	state, ok := r.states.Load(key.PlatformID)
	if !ok {
		return nil
	}
	lease, ok := state.Leases.GetLease(key.Account)
	if !ok {
		return nil
	}
	return &model.Lease{
		PlatformID:     key.PlatformID,
		Account:        key.Account,
		NodeHash:       lease.NodeHash.Hex(),
		EgressIP:       lease.EgressIP.String(),
		CreatedAtNs:    lease.CreatedAtNs,
		ExpiryNs:       lease.ExpiryNs,
		LastAccessedNs: lease.LastAccessedNs,
	}
}

// UpsertLease writes or replaces a lease for (platform_id, account).
// It updates per-IP lease counters and emits LeaseCreate/LeaseReplace events.
func (r *Router) UpsertLease(ml model.Lease) error {
	platformID := strings.TrimSpace(ml.PlatformID)
	if platformID == "" {
		return errors.New("platform_id is required")
	}
	account := strings.TrimSpace(ml.Account)
	if account == "" {
		return errors.New("account is required")
	}

	h, err := node.ParseHex(ml.NodeHash)
	if err != nil {
		return fmt.Errorf("parse node_hash: %w", err)
	}
	ip, err := netip.ParseAddr(ml.EgressIP)
	if err != nil {
		return fmt.Errorf("parse egress_ip: %w", err)
	}

	state := r.ensurePlatformState(platformID)
	lease := Lease{
		NodeHash:       h,
		EgressIP:       ip,
		CreatedAtNs:    ml.CreatedAtNs,
		ExpiryNs:       ml.ExpiryNs,
		LastAccessedNs: ml.LastAccessedNs,
	}

	eventType := LeaseCreate
	_, _ = state.Leases.leases.Compute(account, func(current Lease, loaded bool) (Lease, xsync.ComputeOp) {
		if loaded {
			state.Leases.stats.Dec(current.EgressIP)
			eventType = LeaseReplace
		}
		state.Leases.stats.Inc(lease.EgressIP)
		return lease, xsync.UpdateOp
	})

	r.emitLeaseEvent(LeaseEvent{
		Type:       eventType,
		PlatformID: platformID,
		Account:    account,
		NodeHash:   lease.NodeHash,
		EgressIP:   lease.EgressIP,
	})
	return nil
}

// SnapshotIPLoad returns a best-effort point-in-time IP load snapshot for a platform.
// If the platform has no routing state yet, it returns an empty snapshot.
func (r *Router) SnapshotIPLoad(platformID string) map[netip.Addr]int64 {
	state, ok := r.states.Load(platformID)
	if !ok {
		return map[netip.Addr]int64{}
	}
	return state.IPLoadStats.Snapshot()
}

// RestoreLeases restores leases from persistence during bootstrap.
func (r *Router) RestoreLeases(leases []model.Lease) {
	for _, ml := range leases {
		h, err := node.ParseHex(ml.NodeHash)
		if err != nil {
			continue
		}
		ip, err := netip.ParseAddr(ml.EgressIP)
		if err != nil {
			continue
		}

		state, _ := r.states.LoadOrCompute(ml.PlatformID, func() (*PlatformRoutingState, bool) {
			return NewPlatformRoutingState(), false
		})

		l := Lease{
			NodeHash:       h,
			EgressIP:       ip,
			CreatedAtNs:    ml.CreatedAtNs,
			ExpiryNs:       ml.ExpiryNs,
			LastAccessedNs: ml.LastAccessedNs,
		}
		// Directly insert into table and stats
		state.Leases.CreateLease(ml.Account, l)
	}
}

// RangeLeases iterates over all leases for a platform.
// Returns false if the platform has no routing state.
func (r *Router) RangeLeases(platformID string, fn func(account string, lease Lease) bool) bool {
	state, ok := r.states.Load(platformID)
	if !ok {
		return false
	}
	state.Leases.Range(fn)
	return true
}

// DeleteLease removes a single lease by platform and account.
// Returns true if a lease was deleted. Emits a LeaseRemove event.
func (r *Router) DeleteLease(platformID, account string) bool {
	state, ok := r.states.Load(platformID)
	if !ok {
		return false
	}
	lease, deleted := state.Leases.DeleteLease(account)
	if !deleted {
		return false
	}
	r.emitLeaseEvent(LeaseEvent{
		Type:        LeaseRemove,
		PlatformID:  platformID,
		Account:     account,
		NodeHash:    lease.NodeHash,
		EgressIP:    lease.EgressIP,
		CreatedAtNs: lease.CreatedAtNs,
	})
	return true
}

// DeleteAllLeases removes all leases for a platform.
// Returns the number of leases deleted. Emits a LeaseRemove event for each.
func (r *Router) DeleteAllLeases(platformID string) int {
	state, ok := r.states.Load(platformID)
	if !ok {
		return 0
	}
	count := 0
	state.Leases.Range(func(account string, _ Lease) bool {
		removed, deleted := state.Leases.DeleteLease(account)
		if deleted {
			r.emitLeaseEvent(LeaseEvent{
				Type:        LeaseRemove,
				PlatformID:  platformID,
				Account:     account,
				NodeHash:    removed.NodeHash,
				EgressIP:    removed.EgressIP,
				CreatedAtNs: removed.CreatedAtNs,
			})
			count++
		}
		return true
	})
	return count
}

func containsHash(haystack []node.Hash, needle node.Hash) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// excludeAlso returns exclude with h appended, leaving the caller's slice (and
// its backing array) untouched — RouteOptions.Exclude is owned by the caller
// and may be shared across attempts.
func excludeAlso(exclude []node.Hash, h node.Hash) []node.Hash {
	if containsHash(exclude, h) {
		return exclude
	}
	out := make([]node.Hash, 0, len(exclude)+1)
	out = append(out, exclude...)
	return append(out, h)
}

// borrowRoute hands out a node for this request only, leaving the caller's
// lease exactly as it was.
//
// It is used when a request cannot use its sticky node because it just failed
// there. Not writing the lease matters: updating it would relocate the account
// on the strength of one failure, and a flapping node could then drag the
// account across egress IPs. A borrowed node also skips IPLoadStats, so it is
// invisible to load balancing — acceptable, because it also means a retry does
// not make the borrowed IP look busier than it is.
//
// Preference is given to another node behind the same egress IP, so the account
// keeps its address wherever that is possible.
func (r *Router) borrowRoute(
	plat *platform.Platform,
	state *PlatformRoutingState,
	current Lease,
	targetDomain string,
	nowNs int64,
	exclude []node.Hash,
) (RouteResult, bool) {
	if h, ok := chooseSameIPRotationCandidate(
		plat,
		r.pool,
		current.EgressIP,
		targetDomain,
		r.authorities(),
		r.p2cWindow(),
		exclude,
		nil,
	); ok {
		if entry, found := r.pool.GetEntry(h); found {
			return RouteResult{
				NodeHash: h,
				EgressIP: entry.GetEgressIP(),
				Borrowed: true,
			}, true
		}
	}

	h, entry, err := r.selectLiveRandomRoute(plat, state.IPLoadStats, targetDomain, exclude)
	if err != nil {
		return RouteResult{}, false
	}
	return RouteResult{
		NodeHash: h,
		EgressIP: entry.GetEgressIP(),
		Borrowed: true,
	}, true
}
