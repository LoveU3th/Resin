// Package topology coordinates the subscription → node pool → platform view pipeline.
// It owns the GlobalNodePool, PlatformManager, and SubscriptionManager,
// breaking import cycles between the leaf packages (node, subscription, platform).
package topology

import (
	"encoding/json"
	"errors"
	"net/netip"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/puzpuzpuz/xsync/v4"
)

// GlobalNodePool is the system's single source of truth for nodes.
// It uses xsync.Map for concurrent access and xsync.Compute for atomic
// AddNodeFromSub / RemoveNodeFromSub operations.
type GlobalNodePool struct {
	nodes *xsync.Map[node.Hash, *node.NodeEntry]

	// Platform references for dirty-notify.
	platMu         sync.RWMutex
	platformByID   map[string]*platform.Platform // id -> platform
	platformByName map[string]*platform.Platform // name -> platform

	// Subscription lookup — injected by SubscriptionManager.
	subLookup func(subID string) *subscription.Subscription

	// GeoIP lookup — injected at construction.
	geoLookup platform.GeoLookupFunc

	// Persistence callbacks (optional, nil in tests without persistence).
	onNodeAdded      func(hash node.Hash)                        // called after a new node is created
	onNodeRemoved    func(hash node.Hash, entry *node.NodeEntry) // called after a node is deleted from pool
	onSubNodeChanged func(subID string, hash node.Hash, added bool)

	// Health callbacks (optional).
	onNodeDynamicChanged func(hash node.Hash)                // fired on circuit/failure/egress changes
	onNodeLatencyChanged func(hash node.Hash, domain string) // fired on latency upserts and evictions

	// Health config
	maxLatencyTableEntries         int
	maxConsecutiveFailures         func() int
	latencyDecayWindow             func() time.Duration
	latencyAuthorities             func() []string
	healthEwmaWindow               func() int
	healthEwmaMinSamples           func() int
	circuitCooldown                func() time.Duration
	circuitMaxCooldown             func() time.Duration
	healthRecoveryFloorPct         func() int
	healthTransferFailureWeightPct func() int
}

// PoolConfig configures the GlobalNodePool.
type PoolConfig struct {
	SubLookup              func(subID string) *subscription.Subscription
	GeoLookup              platform.GeoLookupFunc
	OnNodeAdded            func(hash node.Hash)
	OnNodeRemoved          func(hash node.Hash, entry *node.NodeEntry)
	OnSubNodeChanged       func(subID string, hash node.Hash, added bool)
	OnNodeDynamicChanged   func(hash node.Hash)
	OnNodeLatencyChanged   func(hash node.Hash, domain string)
	MaxLatencyTableEntries int
	MaxConsecutiveFailures func() int
	LatencyDecayWindow     func() time.Duration
	LatencyAuthorities     func() []string
	// HealthEwmaWindow and HealthEwmaMinSamples tune the per-node success-ratio
	// EWMA. Both are optional; node defaults apply when nil or non-positive.
	HealthEwmaWindow     func() int
	HealthEwmaMinSamples func() int
	// CircuitCooldown is the minimum time a node stays isolated once its
	// breaker opens; a success before it elapses feeds the health score but
	// does not close the breaker. CircuitMaxCooldown caps the exponential
	// backoff applied on repeated half-open failures. Both optional.
	CircuitCooldown    func() time.Duration
	CircuitMaxCooldown func() time.Duration
	// HealthRecoveryFloorPercent is the health score a node is lifted to when
	// its breaker closes, as a percentage. It sits above the filter threshold so
	// the node re-enters routing instead of being filtered out immediately.
	HealthRecoveryFloorPercent func() int
	// HealthTransferFailureWeightPercent scales how much a transfer-phase
	// failure counts against the health score, as a percentage of a
	// connect-phase failure. 100 weights them the same; 0 ignores transfer
	// failures entirely.
	HealthTransferFailureWeightPercent func() int
}

var (
	// ErrPlatformNotRegistered indicates the target platform ID is not registered.
	ErrPlatformNotRegistered = errors.New("platform not registered")
	// ErrPlatformNameConflict indicates another platform already uses the target name.
	ErrPlatformNameConflict = errors.New("platform name conflict")
)

// NewGlobalNodePool creates a new GlobalNodePool.
func NewGlobalNodePool(cfg PoolConfig) *GlobalNodePool {
	maxConsecutiveFailuresFn := cfg.MaxConsecutiveFailures
	if maxConsecutiveFailuresFn == nil {
		panic("topology: NewGlobalNodePool requires non-nil MaxConsecutiveFailures")
	}

	return &GlobalNodePool{
		nodes:                          xsync.NewMap[node.Hash, *node.NodeEntry](),
		subLookup:                      cfg.SubLookup,
		geoLookup:                      cfg.GeoLookup,
		onNodeAdded:                    cfg.OnNodeAdded,
		onNodeRemoved:                  cfg.OnNodeRemoved,
		onSubNodeChanged:               cfg.OnSubNodeChanged,
		onNodeDynamicChanged:           cfg.OnNodeDynamicChanged,
		onNodeLatencyChanged:           cfg.OnNodeLatencyChanged,
		maxLatencyTableEntries:         cfg.MaxLatencyTableEntries,
		maxConsecutiveFailures:         maxConsecutiveFailuresFn,
		latencyDecayWindow:             cfg.LatencyDecayWindow,
		latencyAuthorities:             cfg.LatencyAuthorities,
		healthEwmaWindow:               cfg.HealthEwmaWindow,
		healthEwmaMinSamples:           cfg.HealthEwmaMinSamples,
		circuitCooldown:                cfg.CircuitCooldown,
		circuitMaxCooldown:             cfg.CircuitMaxCooldown,
		healthRecoveryFloorPct:         cfg.HealthRecoveryFloorPercent,
		healthTransferFailureWeightPct: cfg.HealthTransferFailureWeightPercent,
		platformByID:                   make(map[string]*platform.Platform),
		platformByName:                 make(map[string]*platform.Platform),
	}
}

// AddNodeFromSub adds a node to the pool with the given subscription reference.
// Uses xsync.Compute for atomic load-or-create + ref-update.
// Idempotent: adding the same (hash, subID) pair multiple times is safe.
// After mutation, notifies all platforms to re-evaluate the node.
func (p *GlobalNodePool) AddNodeFromSub(hash node.Hash, rawOpts json.RawMessage, subID string) {
	isNew := false
	p.nodes.Compute(hash, func(entry *node.NodeEntry, loaded bool) (*node.NodeEntry, xsync.ComputeOp) {
		if !loaded {
			createdAt := time.Now()
			entry = node.NewNodeEntry(hash, rawOpts, createdAt, p.maxLatencyTableEntries)
			// New subscription nodes start as circuit-open and must be proven healthy by probes.
			entry.CircuitOpenSince.Store(createdAt.UnixNano())
			isNew = true
		}
		entry.AddSubscriptionID(subID)
		return entry, xsync.UpdateOp
	})

	if isNew && p.onNodeAdded != nil {
		p.onNodeAdded(hash)
	}
	if p.onSubNodeChanged != nil {
		p.onSubNodeChanged(subID, hash, true)
	}

	p.notifyAllPlatformsDirty(hash)
}

// RemoveNodeFromSub removes a subscription reference from a node.
// If the node has no remaining references, it is deleted from the pool.
// Uses xsync.Compute for atomic ref-update + conditional delete.
// Idempotent: removing a nonexistent (hash, subID) pair is safe.
func (p *GlobalNodePool) RemoveNodeFromSub(hash node.Hash, subID string) {
	wasDeleted := false
	var deletedEntry *node.NodeEntry // capture entry before map deletion
	p.nodes.Compute(hash, func(entry *node.NodeEntry, loaded bool) (*node.NodeEntry, xsync.ComputeOp) {
		if !loaded {
			return entry, xsync.CancelOp // idempotent no-op
		}
		empty := entry.RemoveSubscriptionID(subID)
		if empty {
			wasDeleted = true
			deletedEntry = entry
			return nil, xsync.DeleteOp
		}
		return entry, xsync.UpdateOp
	})

	if p.onSubNodeChanged != nil {
		p.onSubNodeChanged(subID, hash, false)
	}
	if wasDeleted && p.onNodeRemoved != nil {
		p.onNodeRemoved(hash, deletedEntry)
	}

	p.notifyAllPlatformsDirty(hash)
}

// GetEntry retrieves a node entry by hash.
func (p *GlobalNodePool) GetEntry(hash node.Hash) (*node.NodeEntry, bool) {
	return p.nodes.Load(hash)
}

// Range iterates all nodes in the pool.
func (p *GlobalNodePool) Range(fn func(node.Hash, *node.NodeEntry) bool) {
	p.nodes.Range(fn)
}

// Size returns the number of nodes in the pool.
func (p *GlobalNodePool) Size() int {
	return p.nodes.Size()
}

// LoadNodeFromBootstrap inserts a node during bootstrap recovery.
// No dirty-marks, no platform notifications.
func (p *GlobalNodePool) LoadNodeFromBootstrap(entry *node.NodeEntry) {
	p.nodes.Store(entry.Hash, entry)
}

// RegisterPlatform adds a platform to receive dirty notifications.
func (p *GlobalNodePool) RegisterPlatform(plat *platform.Platform) {
	p.platMu.Lock()
	defer p.platMu.Unlock()
	// Check for existing ID to avoid duplicates.
	if _, exists := p.platformByID[plat.ID]; exists {
		return
	}
	p.platformByID[plat.ID] = plat
	if plat.Name != "" {
		p.platformByName[plat.Name] = plat
	}
}

// UnregisterPlatform removes a platform from dirty notifications.
func (p *GlobalNodePool) UnregisterPlatform(id string) {
	p.platMu.Lock()
	defer p.platMu.Unlock()
	plat, ok := p.platformByID[id]
	if !ok {
		return
	}
	delete(p.platformByID, id)
	if plat.Name != "" {
		// Only delete when name still points at this platform.
		if current, ok := p.platformByName[plat.Name]; ok && current == plat {
			delete(p.platformByName, plat.Name)
		}
	}
}

// ReplacePlatform atomically replaces an existing platform object by ID.
// It follows a copy-on-write update path: the caller builds a new Platform
// instance, this method rebuilds its routable view, then swaps map pointers
// under platMu in one critical section.
func (p *GlobalNodePool) ReplacePlatform(next *platform.Platform) error {
	if next == nil || next.ID == "" {
		return ErrPlatformNotRegistered
	}

	// Build the new platform's view before publish so readers never observe
	// an empty, not-yet-built view due only to replacement.
	p.RebuildPlatform(next)

	p.platMu.Lock()
	defer p.platMu.Unlock()

	current, ok := p.platformByID[next.ID]
	if !ok {
		return ErrPlatformNotRegistered
	}

	if next.Name != "" {
		if existingByName, exists := p.platformByName[next.Name]; exists && existingByName != current {
			return ErrPlatformNameConflict
		}
	}

	p.platformByID[next.ID] = next

	if current.Name != "" {
		if mapped, exists := p.platformByName[current.Name]; exists && mapped == current {
			delete(p.platformByName, current.Name)
		}
	}
	if next.Name != "" {
		p.platformByName[next.Name] = next
	}

	return nil
}

// GetPlatform retrieves a platform by ID.
func (p *GlobalNodePool) GetPlatform(id string) (*platform.Platform, bool) {
	p.platMu.RLock()
	defer p.platMu.RUnlock()
	plat, ok := p.platformByID[id]
	return plat, ok
}

// GetPlatformByName retrieves a platform by Name.
func (p *GlobalNodePool) GetPlatformByName(name string) (*platform.Platform, bool) {
	p.platMu.RLock()
	defer p.platMu.RUnlock()
	plat, ok := p.platformByName[name]
	return plat, ok
}

// RangePlatforms iterates all registered platforms.
func (p *GlobalNodePool) RangePlatforms(fn func(*platform.Platform) bool) {
	for _, plat := range p.platformSnapshot() {
		if !fn(plat) {
			return
		}
	}
}

func (p *GlobalNodePool) platformSnapshot() []*platform.Platform {
	p.platMu.RLock()
	defer p.platMu.RUnlock()

	platforms := make([]*platform.Platform, 0, len(p.platformByID))
	for _, plat := range p.platformByID {
		platforms = append(platforms, plat)
	}
	return platforms
}

// MakeSubLookup builds the SubLookupFunc closure for MatchRegexs / tag resolution.
func (p *GlobalNodePool) MakeSubLookup() node.SubLookupFunc {
	return func(subID string, hash node.Hash) (string, bool, []string, bool) {
		// Compatibility fallback for test wiring that omits SubLookup.
		// We cannot resolve subscription metadata, so treat the reference as
		// "present+enabled" without tags.
		if p.subLookup == nil {
			return "", true, nil, true
		}

		sub := p.subLookup(subID)
		if sub == nil {
			return "", false, nil, false
		}

		managed, ok := sub.ManagedNodes().LoadNode(hash)
		if !ok || managed.Evicted {
			return "", false, nil, false
		}
		tags := managed.Tags
		return sub.Name(), sub.Enabled(), tags, true
	}
}

// ResolveNodeDisplayTag resolves a node hash to its display tag for request logs.
// Rule:
//  1. Prefer enabled subscriptions: among enabled holders, choose earliest-created.
//  2. Within that subscription, choose lexicographically smallest tag.
//  3. If no enabled holder exists, fallback to all holders with the same rule.
//  4. Return "<SubscriptionName>/<Tag>".
//
// Returns empty string when resolution is not possible.
func (p *GlobalNodePool) ResolveNodeDisplayTag(hash node.Hash) string {
	if p.subLookup == nil {
		return ""
	}

	entry, ok := p.GetEntry(hash)
	if !ok || entry == nil {
		return ""
	}
	subIDs := entry.SubscriptionIDs()
	if len(subIDs) == 0 {
		return ""
	}

	pick := func(enabledOnly bool) (string, bool) {
		bestFound := false
		var bestCreatedAtNs int64
		var bestSubID string
		var bestSubName string
		var bestTag string

		for _, subID := range subIDs {
			sub := p.subLookup(subID)
			if sub == nil {
				continue
			}
			if enabledOnly && !sub.Enabled() {
				continue
			}

			managed, ok := sub.ManagedNodes().LoadNode(hash)
			if !ok || managed.Evicted {
				continue
			}
			tags := managed.Tags
			if len(tags) == 0 {
				continue
			}

			smallestTag := tags[0]
			for _, tag := range tags[1:] {
				if tag < smallestTag {
					smallestTag = tag
				}
			}

			createdAtNs := sub.CreatedAtNs
			if !bestFound ||
				createdAtNs < bestCreatedAtNs ||
				(createdAtNs == bestCreatedAtNs && subID < bestSubID) {
				bestFound = true
				bestCreatedAtNs = createdAtNs
				bestSubID = subID
				bestSubName = sub.Name()
				bestTag = smallestTag
			}
		}

		if !bestFound || bestSubName == "" || bestTag == "" {
			return "", false
		}
		return bestSubName + "/" + bestTag, true
	}

	if tag, ok := pick(true); ok {
		return tag
	}
	if tag, ok := pick(false); ok {
		return tag
	}
	return ""
}

// IsNodeDisabled reports whether a node is disabled by subscription state:
// all referencing subscriptions are disabled (or missing / not applicable).
func (p *GlobalNodePool) IsNodeDisabled(hash node.Hash) bool {
	entry, ok := p.GetEntry(hash)
	if !ok || entry == nil {
		return true
	}
	return entry.IsDisabledBySubscriptions(p.MakeSubLookup())
}

// MakeHealthyAndEnabledEvaluator builds a predicate for pool-context health
// aggregates: the node must not be disabled by subscription state and must
// satisfy the entry-local health checks.
func (p *GlobalNodePool) MakeHealthyAndEnabledEvaluator() func(entry *node.NodeEntry) bool {
	subLookup := p.MakeSubLookup()
	return func(entry *node.NodeEntry) bool {
		if entry == nil || entry.IsDisabledBySubscriptions(subLookup) {
			return false
		}
		return entry.IsHealthy()
	}
}

// notifyAllPlatformsDirty tells every registered platform to re-evaluate a node.
func (p *GlobalNodePool) notifyAllPlatformsDirty(hash node.Hash) {
	platforms := p.platformSnapshot()
	if len(platforms) == 0 {
		return
	}

	subLookup := p.MakeSubLookup()
	getEntry := func(h node.Hash) (*node.NodeEntry, bool) {
		return p.nodes.Load(h)
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(platforms) {
		workers = len(platforms)
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, plat := range platforms {
		sem <- struct{}{}
		wg.Add(1)
		go func(plat *platform.Platform) {
			defer wg.Done()
			defer func() { <-sem }()
			plat.NotifyDirty(hash, getEntry, subLookup, p.geoLookup)
		}(plat)
	}
	wg.Wait()
}

// RebuildAllPlatforms triggers a full rebuild on all registered platforms.
func (p *GlobalNodePool) RebuildAllPlatforms() {
	platforms := p.platformSnapshot()
	if len(platforms) == 0 {
		return
	}

	subLookup := p.MakeSubLookup()
	poolRange := func(fn func(node.Hash, *node.NodeEntry) bool) {
		p.nodes.Range(fn)
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(platforms) {
		workers = len(platforms)
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, plat := range platforms {
		sem <- struct{}{}
		wg.Add(1)
		go func(plat *platform.Platform) {
			defer wg.Done()
			defer func() { <-sem }()
			plat.FullRebuild(poolRange, subLookup, p.geoLookup)
		}(plat)
	}
	wg.Wait()
}

// RebuildPlatform triggers a full rebuild on a specific platform.
func (p *GlobalNodePool) RebuildPlatform(plat *platform.Platform) {
	subLookup := p.MakeSubLookup()
	poolRange := func(fn func(node.Hash, *node.NodeEntry) bool) {
		p.nodes.Range(fn)
	}
	plat.FullRebuild(poolRange, subLookup, p.geoLookup)
}

// --- Health Management ---

// SetOnNodeAdded sets the callback fired when a new node is added.
// Must be called before any background workers are started.
func (p *GlobalNodePool) SetOnNodeAdded(fn func(hash node.Hash)) {
	p.onNodeAdded = fn
}

// SetOnNodeRemoved sets the callback fired when a node is removed from the pool.
// Must be called before any background workers are started.
func (p *GlobalNodePool) SetOnNodeRemoved(fn func(hash node.Hash, entry *node.NodeEntry)) {
	p.onNodeRemoved = fn
}

// NotifyNodeDirty triggers platform re-evaluation for a single node.
// Used by OutboundManager after outbound creation to update routable views.
func (p *GlobalNodePool) NotifyNodeDirty(hash node.Hash) {
	p.notifyAllPlatformsDirty(hash)
}

// RangeNodes iterates over all nodes in the pool.
// The callback receives each node's hash and entry. Return false to stop.
func (p *GlobalNodePool) RangeNodes(fn func(node.Hash, *node.NodeEntry) bool) {
	p.nodes.Range(fn)
}

// RecordResult records a probe or passive health-check result.
//
// The breaker is a three-state machine. A failure at or above the consecutive
// failure threshold isolates the node for at least the configured cooldown;
// once that elapses the node is half-open and a probe may close the breaker.
//
// A success arriving during the cooldown feeds the health score but does NOT
// close the breaker. That clamp is the whole point: without it a flapping node
// can rejoin routing on the very next successful request and oscillate between
// removed and routable many times a minute.
//
// Every result is folded into the node's health score. Platforms are notified
// only when the breaker opens or closes, and OnNodeDynamicChanged fires only
// when dynamic fields actually change.
func (p *GlobalNodePool) RecordResult(hash node.Hash, success bool) {
	entry, ok := p.nodes.Load(hash)
	if !ok {
		return
	}

	dynamicChanged, circuitStateChanged := p.recordBreaker(entry, success)
	// A probe result is an unambiguous signal, so it counts in full.
	p.recordHealth(entry, success, 1)

	if circuitStateChanged {
		p.notifyAllPlatformsDirty(hash)
	}
	if dynamicChanged && p.onNodeDynamicChanged != nil {
		p.onNodeDynamicChanged(hash)
	}
}

// recordBreaker applies one result to the breaker only, leaving the health
// score alone. Returns whether dynamic fields changed and whether the breaker
// opened or closed.
func (p *GlobalNodePool) recordBreaker(entry *node.NodeEntry, success bool) (dynamicChanged, circuitStateChanged bool) {
	now := time.Now()

	if success {
		if entry.FailureCount.Swap(0) != 0 {
			dynamicChanged = true
		}

		// Only a success at or past the cooldown may close the breaker.
		if entry.CircuitOpenSince.Load() != 0 && entry.CircuitState(now) != node.CircuitOpen {
			// Clear the backoff before closing. Reversed, a concurrent failure
			// could observe openedAt != 0 with a zeroed cooldown and treat the
			// node as immediately half-open, skipping the cooldown entirely.
			entry.CircuitReopenCount.Store(0)
			entry.CircuitCooldownNs.Store(0)
			if entry.CircuitOpenSince.Swap(0) != 0 {
				dynamicChanged = true
				circuitStateChanged = true
				// Lift the health score to a floor above the filter threshold,
				// so the node re-enters routing instead of being filtered
				// straight back out on its stale score.
				entry.ResetHealth(p.currentHealthRecoveryFloor())
			}
		}
	} else {
		newCount := entry.FailureCount.Add(1)
		dynamicChanged = true
		maxConsecutiveFailures := p.currentMaxConsecutiveFailures()
		if maxConsecutiveFailures > 0 && int(newCount) >= maxConsecutiveFailures {
			openedAt := entry.CircuitOpenSince.Load()
			switch {
			case openedAt == 0 || entry.CircuitCooldownNs.Load() == 0:
				// First isolation, or the node's initial breaker state: nodes
				// start out isolated with no cooldown, and that must not count
				// as a half-open failure or the very first failure would
				// already back off to twice the base cooldown.
				entry.CircuitReopenCount.Store(0)
				entry.CircuitCooldownNs.Store(int64(p.cooldownFor(0)))
				if openedAt == 0 {
					if entry.CircuitOpenSince.CompareAndSwap(0, now.UnixNano()) {
						circuitStateChanged = true
					}
				} else {
					// Restart the clock at this failure. The node is already
					// outside every platform view, so no notification.
					entry.CircuitOpenSince.Store(now.UnixNano())
				}
			case entry.CircuitState(now) == node.CircuitHalfOpen:
				// A half-open probe failed. Keep the original open timestamp so
				// the cooldown is measured from first isolation, and lengthen
				// it: 30s -> 60s -> 120s...
				failures := entry.CircuitReopenCount.Add(1)
				entry.CircuitCooldownNs.Store(int64(p.cooldownFor(failures)))
			default:
				// Still inside the cooldown; nothing to change.
			}
		}
	}

	return dynamicChanged, circuitStateChanged
}

// recordHealth folds one observation into the score with the given weight.
// It is deliberately separate from the breaker: the breaker takes the raw
// result, while health needs to weigh a weak signal less than a strong one.
func (p *GlobalNodePool) recordHealth(entry *node.NodeEntry, success bool, weight float64) {
	entry.RecordHealthSample(success, weight, p.currentHealthEwmaWindow(), p.currentHealthEwmaMinSamples())
}

// RecordConnDrop records that a pooled connection to this node turned out to be
// dead: the peer had dropped it without telling us, which is the "it connects,
// then breaks" symptom.
//
// It feeds the health score but deliberately not the breaker. A batch of idle
// connections expiring together produces a burst of these, and counting them as
// failures would evict a node that is probably fine only because our pool held
// on to its connections longer than the peer did.
//
// platformID is accepted for interface symmetry with the other recorders and is
// not used: the drop is about the node, not about who was using it.
func (p *GlobalNodePool) RecordConnDrop(_ string, hash node.Hash) {
	entry, ok := p.nodes.Load(hash)
	if !ok {
		return
	}
	// Same reduced weight as a transfer failure: the node was reached before,
	// so this is weaker evidence than never reaching it at all.
	p.recordHealth(entry, false, p.currentTransferFailureWeight())
}

// RecordPassiveStageResult records traffic feedback for one phase of a request.
//
// The breaker always sees the raw result — a failure is a failure regardless of
// phase. The health score does not: a connect failure means the node could not
// be reached at all, while a transfer failure means it was reached and had
// already started responding, so the fault may lie with the client or the
// network in between. Transfer failures are therefore weighted lower.
func (p *GlobalNodePool) RecordPassiveStageResult(
	platformID string,
	hash node.Hash,
	stage string,
	success bool,
) {
	entry, ok := p.nodes.Load(hash)
	if !ok {
		return
	}

	weight := 1.0
	if !success && stage == node.PassiveStageTransfer {
		weight = p.currentTransferFailureWeight()
	}

	// The breaker is skipped entirely for platforms that opted out, matching
	// RecordPassiveResult.
	if success || !p.passiveCircuitBreakerDisabled(platformID) {
		dynamicChanged, circuitStateChanged := p.recordBreaker(entry, success)
		if circuitStateChanged {
			p.notifyAllPlatformsDirty(hash)
		}
		if dynamicChanged && p.onNodeDynamicChanged != nil {
			p.onNodeDynamicChanged(hash)
		}
	}

	p.recordHealth(entry, success, weight)
}

// RecordPassiveResult records health feedback from user proxy traffic.
//
// The passive-circuit-breaker switch governs only whether a failure can trip
// the breaker. The health score always sees the result: a platform that opts
// out of passive tripping is precisely the one that most needs to know a node
// is unhealthy, because it will not remove the node on its own. Otherwise such
// a platform would only ever see probe results — and probes are exactly what
// reports "reachable" for a node that breaks on real traffic.
func (p *GlobalNodePool) RecordPassiveResult(platformID string, hash node.Hash, success bool) {
	p.RecordPassiveStageResult(platformID, hash, node.PassiveStageConnect, success)
}

func (p *GlobalNodePool) passiveCircuitBreakerDisabled(platformID string) bool {
	if platformID == "" {
		return false
	}

	p.platMu.RLock()
	defer p.platMu.RUnlock()

	plat, ok := p.platformByID[platformID]
	if !ok || plat == nil {
		return false
	}
	return plat.PassiveCircuitBreakerDisabled
}

func (p *GlobalNodePool) currentMaxConsecutiveFailures() int {
	return p.maxConsecutiveFailures()
}

// currentHealthEwmaWindow and currentHealthEwmaMinSamples read the health EWMA
// tuning. They are optional, so a missing or non-positive value falls through
// to the node package's defaults rather than disabling the score.
func (p *GlobalNodePool) currentHealthEwmaWindow() int {
	if p.healthEwmaWindow == nil {
		return 0
	}
	return p.healthEwmaWindow()
}

func (p *GlobalNodePool) currentHealthEwmaMinSamples() int {
	if p.healthEwmaMinSamples == nil {
		return 0
	}
	return p.healthEwmaMinSamples()
}

const (
	defaultCircuitCooldown    = 30 * time.Second
	defaultCircuitMaxCooldown = 30 * time.Minute
	// defaultHealthRecoveryFloorPercent sits above the default filter
	// threshold (40) so a recovered node is not immediately filtered out.
	defaultHealthRecoveryFloorPercent = 60
	// defaultHealthTransferFailureWeightPercent: a transfer failure means the
	// node was reached and had begun responding, so it is counted at half the
	// weight of a connect failure.
	defaultHealthTransferFailureWeightPercent = 50
	// backoffShiftLimit caps the 2^n growth well below the int64 range.
	backoffShiftLimit = 40
)

func (p *GlobalNodePool) currentCircuitCooldown() time.Duration {
	if p.circuitCooldown == nil {
		return defaultCircuitCooldown
	}
	// 0 is a valid value meaning "no cooldown"; only a negative value (or no
	// accessor at all) falls back to the default.
	if d := p.circuitCooldown(); d >= 0 {
		return d
	}
	return defaultCircuitCooldown
}

func (p *GlobalNodePool) currentCircuitMaxCooldown() time.Duration {
	if p.circuitMaxCooldown == nil {
		return defaultCircuitMaxCooldown
	}
	if d := p.circuitMaxCooldown(); d >= 0 {
		return d
	}
	return defaultCircuitMaxCooldown
}

// currentTransferFailureWeight returns how much a transfer-phase failure counts
// against the health score, relative to a connect-phase failure.
func (p *GlobalNodePool) currentTransferFailureWeight() float64 {
	pct := defaultHealthTransferFailureWeightPercent
	if p.healthTransferFailureWeightPct != nil {
		pct = p.healthTransferFailureWeightPct()
	}
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		pct = 100
	}
	return float64(pct) / 100
}

func (p *GlobalNodePool) currentHealthRecoveryFloor() float64 {
	pct := defaultHealthRecoveryFloorPercent
	if p.healthRecoveryFloorPct != nil {
		pct = p.healthRecoveryFloorPct()
	}
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	return float64(pct) / 100
}

// cooldownFor returns base * 2^failures, capped at the configured maximum.
// The breaker keeps its original open timestamp, so the cooldown is always
// measured from when the node was first isolated, not from the last failure.
func (p *GlobalNodePool) cooldownFor(halfOpenFailures int32) time.Duration {
	base := p.currentCircuitCooldown()
	if base <= 0 {
		// Cooldown disabled: never back off into a positive value.
		return 0
	}
	max := p.currentCircuitMaxCooldown()
	// A maximum that is not configured (0) or that sits below the base is
	// raised to the base, so the first backoff step always has somewhere to go.
	if max < base {
		max = base
	}
	if halfOpenFailures <= 0 {
		return base
	}

	cooldown := base
	for i := int32(0); i < halfOpenFailures && i < backoffShiftLimit; i++ {
		cooldown *= 2
		// cooldown <= 0 means the shift overflowed; clamp to the maximum.
		if cooldown >= max || cooldown <= 0 {
			return max
		}
	}
	if cooldown > max {
		return max
	}
	return cooldown
}

// RecordLatency records a proactive latency probe attempt for the given node and raw target.
// rawTarget is normalized through ExtractDomain (eTLD+1). latency may be nil,
// which means "attempt only" without latency sample writeback.
//
// This refreshes the probe-attempt timestamps that ProbeManager uses to decide
// whether a node is due for a proactive probe. Callers sampling latency from
// user traffic MUST use RecordPassiveLatency instead, otherwise busy nodes
// never appear due and proactive probing is starved entirely.
func (p *GlobalNodePool) RecordLatency(hash node.Hash, rawTarget string, latency *time.Duration) {
	p.recordLatency(hash, rawTarget, latency, true)
}

// RecordPassiveLatency records a latency sample observed from user traffic.
// Identical to RecordLatency except that it leaves the probe-attempt
// timestamps untouched, so proactive probing keeps running on its normal
// schedule even for nodes serving continuous traffic.
func (p *GlobalNodePool) RecordPassiveLatency(hash node.Hash, rawTarget string, latency *time.Duration) {
	p.recordLatency(hash, rawTarget, latency, false)
}

func (p *GlobalNodePool) recordLatency(
	hash node.Hash,
	rawTarget string,
	latency *time.Duration,
	proactive bool,
) {
	entry, ok := p.nodes.Load(hash)
	if !ok {
		return
	}

	domain := netutil.ExtractDomain(rawTarget)
	isAuthority := p.isAuthorityDomain(domain)
	if proactive {
		nowNs := time.Now().UnixNano()
		entry.LastLatencyProbeAttempt.Store(nowNs)
		if isAuthority {
			entry.LastAuthorityLatencyProbeAttempt.Store(nowNs)
		}
	}
	if p.onNodeDynamicChanged != nil {
		p.onNodeDynamicChanged(hash)
	}

	if latency == nil || *latency <= 0 || entry.LatencyTable == nil {
		return
	}

	var decayWindow time.Duration
	if p.latencyDecayWindow != nil {
		decayWindow = p.latencyDecayWindow()
	}
	if decayWindow <= 0 {
		decayWindow = 30 * time.Second // default
	}

	wasEmpty, evictedDomain, evicted := entry.LatencyTable.UpdateClassified(domain, *latency, decayWindow, isAuthority)

	// If the table transitioned from empty to non-empty, the node might
	// now satisfy the HasLatency filter — notify platforms.
	if wasEmpty {
		p.notifyAllPlatformsDirty(hash)
	}

	if p.onNodeLatencyChanged != nil {
		p.onNodeLatencyChanged(hash, domain)
		if evicted {
			p.onNodeLatencyChanged(hash, evictedDomain)
		}
	}
}

// UpdateNodeEgressIP records an egress probe attempt and optionally updates
// the node's egress IP and explicit region metadata.
// Region update rules:
//   - ip=nil,  loc=nil: keep both IP and region unchanged.
//   - ip!=nil, loc=nil: keep region if IP unchanged; clear region if IP changed.
//   - loc!=nil: set region to loc (normalized).
func (p *GlobalNodePool) UpdateNodeEgressIP(hash node.Hash, ip *netip.Addr, loc *string) {
	entry, ok := p.nodes.Load(hash)
	if !ok {
		return
	}

	nowNs := time.Now().UnixNano()
	entry.LastEgressUpdateAttempt.Store(nowNs)

	oldIP := entry.GetEgressIP()
	oldRegion := entry.GetEgressRegion()
	ipChanged := false

	if ip != nil {
		// Record successful egress-IP sample timestamp.
		entry.LastEgressUpdate.Store(nowNs)
		if oldIP != *ip {
			entry.SetEgressIP(*ip)
			ipChanged = true
		}
	}

	regionChanged := false
	switch {
	case loc != nil:
		entry.SetEgressRegion(*loc)
		regionChanged = oldRegion != entry.GetEgressRegion()
	case ip == nil:
		// Attempt-only update: keep region as-is.
	case !ipChanged:
		// IP unchanged and no explicit region: keep existing region.
	default:
		// IP changed without explicit region: clear stale region metadata.
		if oldRegion != "" {
			entry.SetEgressRegion("")
			regionChanged = true
		}
	}

	if ipChanged || regionChanged {
		p.notifyAllPlatformsDirty(hash)
	}
	if p.onNodeDynamicChanged != nil {
		p.onNodeDynamicChanged(hash)
	}
}

func (p *GlobalNodePool) isAuthorityDomain(domain string) bool {
	if domain == "" || p.latencyAuthorities == nil {
		return false
	}
	authorities := p.latencyAuthorities()
	for _, authority := range authorities {
		if strings.EqualFold(strings.TrimSpace(authority), domain) {
			return true
		}
	}
	return false
}
