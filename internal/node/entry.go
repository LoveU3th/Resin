package node

import (
	"encoding/json"
	"math"
	"net/netip"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

// SubLookupFunc resolves a subscription ID + node hash to the subscription's
// name, enabled status, and the tags for that node in that subscription.
// Returns ok=false if the subscription does not exist.
type SubLookupFunc func(subID string, hash Hash) (name string, enabled bool, tags []string, ok bool)

// NodeEntry represents a node in the global pool.
// Static fields are set at creation; dynamic fields use atomics or mutex.
type NodeEntry struct {
	// --- Static (immutable after creation) ---
	Hash       Hash
	RawOptions json.RawMessage
	CreatedAt  time.Time

	// --- Dynamic (guarded by mu) ---
	mu              sync.RWMutex
	subscriptionIDs []string
	LastError       string

	// Atomic dynamic fields for concurrent hot-path reads.
	FailureCount     atomic.Int32
	CircuitOpenSince atomic.Int64 // unix-nano; 0 = not open
	// CircuitReopenCount counts half-open probes that failed. Together with
	// CircuitOpenSince (which is never reset while the breaker stays open) it
	// implements exponential backoff without a second timestamp.
	CircuitReopenCount atomic.Int32
	// CircuitCooldownNs is the cooldown fixed when the breaker opened, as
	// base * 2^reopenCount capped at the configured maximum. Freezing it here
	// means a config change cannot retune a node that is already isolated.
	//
	// Not persisted: after a restart the breaker state comes back from
	// CircuitOpenSince but with no cooldown, so the node takes the
	// first-isolation path and re-probes at the base cooldown. Losing the
	// backoff progress is harmless — it only means a chronic node gets probed a
	// little sooner.
	CircuitCooldownNs atomic.Int64
	egressIP          atomic.Pointer[netip.Addr] // nil before first store
	egressRegion      atomic.Pointer[string]     // lowercase country code from probe trace; nil when unknown
	LastEgressUpdate  atomic.Int64               // unix-nano of last successful egress-IP sample
	// Health score: EWMA of the success ratio, in [0, 1]. Stored as float32
	// bits so it can be read and updated with a single atomic operation —
	// the traffic path must not take a lock for this.
	healthBits atomic.Uint32
	// healthSamples counts observations behind the score, saturating at
	// MaxUint32. Used to gate cold-start convergence and hard filtering.
	healthSamples atomic.Uint32
	// Probe-attempt timestamps (unix-nano). These are updated regardless of
	// probe success/failure, and are used by probe schedulers.
	LastLatencyProbeAttempt          atomic.Int64
	LastAuthorityLatencyProbeAttempt atomic.Int64
	LastEgressUpdateAttempt          atomic.Int64
	LatencyTable                     *LatencyTable // per-domain latency stats; nil if not initialized

	// Outbound instance for this node.
	Outbound atomic.Pointer[adapter.Outbound]
}

// NewNodeEntry creates a NodeEntry with the given static fields.
// maxLatencyTableEntries controls the bounded size of the regular-domain LRU
// partition in the per-domain latency table.
// Pass 0 to skip latency table initialization (e.g. in tests that don't need it).
func NewNodeEntry(hash Hash, rawOptions json.RawMessage, createdAt time.Time, maxLatencyTableEntries int) *NodeEntry {
	e := &NodeEntry{
		Hash:       hash,
		RawOptions: rawOptions,
		CreatedAt:  createdAt,
	}
	// Unknown nodes are treated as healthy until observations say otherwise.
	e.healthBits.Store(math.Float32bits(1))
	if maxLatencyTableEntries > 0 {
		e.LatencyTable = NewLatencyTable(maxLatencyTableEntries)
	}
	return e
}

// --- Health score (success-ratio EWMA) ---

const (
	// DefaultHealthEwmaWindow is how many observations the score effectively
	// spans: alpha = 1/window.
	DefaultHealthEwmaWindow = 20
	// DefaultHealthEwmaMinSamples is the count below which a larger alpha is
	// used, so a fresh node converges quickly instead of sitting near its
	// initial 1.0 after several failures.
	DefaultHealthEwmaMinSamples = 5
)

// HealthScore returns the success-ratio EWMA in [0, 1]. 1 means recent
// observations all succeeded, 0 that they all failed.
//
// The score is stored as float32, so a fully recovered node converges to about
// 1 - 6e-7 rather than exactly 1: below 1.0 the float32 spacing is 2^-24, and
// once the remaining step (alpha * delta) drops under half that spacing the
// value stops moving. The residue costs roughly a microsecond of routing
// penalty — far below the tens-of-milliseconds latencies it competes with, so
// it does not justify a wider type here.
func (e *NodeEntry) HealthScore() float64 {
	return float64(math.Float32frombits(e.healthBits.Load()))
}

// HealthSamples returns how many observations back the score, saturating at
// math.MaxUint32.
func (e *NodeEntry) HealthSamples() uint32 {
	return e.healthSamples.Load()
}

// ResetHealth restores the score to fully healthy and drops the sample count.
// Used when a circuit breaker closes, so a recovered node re-enters with a
// usable score instead of the one that got it isolated. floor, when > 0, sets
// the score to that value instead of 1.
func (e *NodeEntry) ResetHealth(floor float64) {
	next := float32(1)
	if floor > 0 {
		next = float32(floor)
	}
	e.healthBits.Store(math.Float32bits(next))
	e.healthSamples.Store(0)
}

// RecordHealthSample folds one observation into the health score.
//
// The EWMA decays per observation, not per unit of time. A time-decayed score
// would stall for a cold node (say one request an hour) and be dominated by
// the last few minutes for a hot one — worse, a bad node becomes cold precisely
// because traffic is being steered away from it, so it would be stranded at its
// last score with no new evidence to recover from. Counting observations makes
// "failed 8 of the last 20 requests" mean the same thing regardless of rate.
//
// weight scales the observation's influence, for signals that should count for
// less than a full failure (a transfer-phase drop, say). Values outside (0, 1]
// are clamped.
func (e *NodeEntry) RecordHealthSample(success bool, weight float64, window, minSamples int) {
	if weight <= 0 {
		return
	}
	if weight > 1 {
		weight = 1
	}
	if window <= 0 {
		window = DefaultHealthEwmaWindow
	}
	if minSamples <= 0 {
		minSamples = DefaultHealthEwmaMinSamples
	}

	alpha := float32(1) / float32(window)
	if e.healthSamples.Load() < uint32(minSamples) {
		// Cold start: converge faster so the first few failures register.
		alpha = float32(1) / float32(minSamples)
	}

	var x float32
	if success {
		x = 1
	}
	step := alpha * float32(weight)

	for {
		oldBits := e.healthBits.Load()
		old := math.Float32frombits(oldBits)
		next := old + step*(x-old)
		if next < 0 {
			next = 0
		} else if next > 1 {
			next = 1
		}
		nextBits := math.Float32bits(next)
		if nextBits == oldBits || e.healthBits.CompareAndSwap(oldBits, nextBits) {
			break
		}
	}

	// Saturating increment, retried until it lands. A dropped count would make
	// a node look newer than it is and keep it on the cold-start alpha.
	for {
		samples := e.healthSamples.Load()
		if samples >= math.MaxUint32 {
			return
		}
		if e.healthSamples.CompareAndSwap(samples, samples+1) {
			return
		}
	}
}

// SubscriptionIDs returns a copy of the subscription ID slice (thread-safe).
func (e *NodeEntry) SubscriptionIDs() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cp := make([]string, len(e.subscriptionIDs))
	copy(cp, e.subscriptionIDs)
	return cp
}

// AddSubscriptionID adds subID to the subscription set if not already present.
// Must be called under external synchronization (e.g. xsync.Compute).
func (e *NodeEntry) AddSubscriptionID(subID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, id := range e.subscriptionIDs {
		if id == subID {
			return // idempotent
		}
	}
	e.subscriptionIDs = append(e.subscriptionIDs, subID)
}

// RemoveSubscriptionID removes subID from the subscription set.
// Returns true if the set is now empty (node should be deleted).
// Must be called under external synchronization (e.g. xsync.Compute).
func (e *NodeEntry) RemoveSubscriptionID(subID string) (empty bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, id := range e.subscriptionIDs {
		if id == subID {
			e.subscriptionIDs = append(e.subscriptionIDs[:i], e.subscriptionIDs[i+1:]...)
			break
		}
	}
	return len(e.subscriptionIDs) == 0
}

// SubscriptionCount returns the number of subscriptions referencing this node.
func (e *NodeEntry) SubscriptionCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.subscriptionIDs)
}

// MatchRegexs tests whether the node matches ALL given regex filters.
// A match means any tag from any enabled subscription satisfies all regexes.
// Tags are tested in the format "<subscriptionName>/<tag>".
// For an empty regex list:
//   - if subLookup is nil, it matches everything (compatibility fallback);
//   - otherwise, it matches only when at least one enabled subscription exists.
func (e *NodeEntry) MatchRegexs(regexes []*regexp.Regexp, subLookup SubLookupFunc) bool {
	if subLookup == nil {
		return len(regexes) == 0
	}

	e.mu.RLock()
	subs := make([]string, len(e.subscriptionIDs))
	copy(subs, e.subscriptionIDs)
	e.mu.RUnlock()

	if len(regexes) == 0 {
		for _, subID := range subs {
			_, enabled, _, ok := subLookup(subID, e.Hash)
			if ok && enabled {
				return true
			}
		}
		// Empty regex with lookup still requires at least one enabled subscription.
		return false
	}

	if len(subs) == 0 {
		return false
	}

	for _, subID := range subs {
		name, enabled, tags, ok := subLookup(subID, e.Hash)
		if !ok || !enabled {
			continue
		}
		for _, tag := range tags {
			candidate := name + "/" + tag
			if matchesAll(candidate, regexes) {
				return true
			}
		}
	}
	return false
}

// HasEnabledSubscription reports whether the node currently has at least one
// enabled subscription reference, based on subLookup.
//
// subLookup must apply the caller's definition of "subscription still holds
// this node" (for example, excluding evicted managed-node entries).
func (e *NodeEntry) HasEnabledSubscription(subLookup SubLookupFunc) bool {
	if e == nil || subLookup == nil {
		return false
	}

	e.mu.RLock()
	subs := make([]string, len(e.subscriptionIDs))
	copy(subs, e.subscriptionIDs)
	e.mu.RUnlock()

	for _, subID := range subs {
		_, enabled, _, ok := subLookup(subID, e.Hash)
		if ok && enabled {
			return true
		}
	}
	return false
}

// IsDisabledBySubscriptions reports whether the node should be treated as
// disabled: all referencing subscriptions are disabled (or missing/inapplicable
// by subLookup semantics).
func (e *NodeEntry) IsDisabledBySubscriptions(subLookup SubLookupFunc) bool {
	return !e.HasEnabledSubscription(subLookup)
}

// matchesAll returns true if s matches every regex in the list.
func matchesAll(s string, regexes []*regexp.Regexp) bool {
	for _, re := range regexes {
		if !re.MatchString(s) {
			return false
		}
	}
	return true
}

// --- Condition helpers for platform filtering ---

// CircuitState describes where a node's breaker is in its cooldown.
type CircuitState int

const (
	// CircuitClosed means the node is routable.
	CircuitClosed CircuitState = iota
	// CircuitOpen means the node is isolated and its cooldown has not elapsed,
	// so even a success must not close the breaker yet.
	CircuitOpen
	// CircuitHalfOpen means the cooldown has elapsed: the node is still not
	// routable, but a probe may now attempt recovery.
	CircuitHalfOpen
)

// CircuitStateOf reports the breaker state at the given time.
func (e *NodeEntry) CircuitState(now time.Time) CircuitState {
	openedAt := e.CircuitOpenSince.Load()
	if openedAt == 0 {
		return CircuitClosed
	}
	cooldown := time.Duration(e.CircuitCooldownNs.Load())
	if cooldown > 0 && now.Sub(time.Unix(0, openedAt)) < cooldown {
		return CircuitOpen
	}
	return CircuitHalfOpen
}

// IsHalfOpen reports whether the cooldown has elapsed, meaning a probe result
// may close the breaker. Probe scheduling uses this to bypass the normal
// interval gate: without it, a node in a 30s cooldown would not be probed for
// up to an hour, making the cooldown effectively unobservable.
func (e *NodeEntry) IsHalfOpen() bool {
	return e.CircuitState(time.Now()) == CircuitHalfOpen
}

// IsCircuitOpen returns true if the node is currently circuit-broken.
// It stays true through the half-open phase: a half-open node is still not
// routable, it is merely eligible to be probed.
func (e *NodeEntry) IsCircuitOpen() bool {
	return e.CircuitOpenSince.Load() != 0
}

// HasLatency returns true if the node has at least one latency record.
func (e *NodeEntry) HasLatency() bool {
	return e.LatencyTable != nil && e.LatencyTable.Size() > 0
}

// HasOutbound returns true if the node has a valid outbound instance.
func (e *NodeEntry) HasOutbound() bool {
	return e.Outbound.Load() != nil
}

// IsHealthy returns true when the node can be treated as healthy for
// routing/statistics: outbound is ready and circuit is not open.
func (e *NodeEntry) IsHealthy() bool {
	if e == nil {
		return false
	}
	return !e.IsCircuitOpen() && e.HasOutbound()
}

// GetEgressIP returns the node's egress IP, or the zero Addr if unknown.
func (e *NodeEntry) GetEgressIP() netip.Addr {
	ptr := e.egressIP.Load()
	if ptr == nil {
		return netip.Addr{}
	}
	return *ptr
}

// SetEgressIP stores the node's egress IP.
func (e *NodeEntry) SetEgressIP(ip netip.Addr) {
	e.egressIP.Store(&ip)
}

// GetEgressRegion returns the node's stored region from probe metadata,
// or empty string if unknown.
func (e *NodeEntry) GetEgressRegion() string {
	ptr := e.egressRegion.Load()
	if ptr == nil {
		return ""
	}
	return *ptr
}

// SetEgressRegion stores the node's explicit probe region.
// Empty input clears the stored value.
func (e *NodeEntry) SetEgressRegion(region string) {
	region = strings.ToLower(strings.TrimSpace(region))
	if region == "" {
		e.egressRegion.Store(nil)
		return
	}
	e.egressRegion.Store(&region)
}

// GetRegion resolves a node region using explicit probe metadata first,
// then GeoIP fallback from egress IP.
func (e *NodeEntry) GetRegion(geoLookup func(netip.Addr) string) string {
	if region := e.GetEgressRegion(); region != "" {
		return region
	}
	if geoLookup == nil {
		return ""
	}
	egressIP := e.GetEgressIP()
	if !egressIP.IsValid() {
		return ""
	}
	return geoLookup(egressIP)
}

// SetLastError sets the node's error string (thread-safe).
func (e *NodeEntry) SetLastError(msg string) {
	e.mu.Lock()
	e.LastError = msg
	e.mu.Unlock()
}

// GetLastError returns the node's error string (thread-safe).
func (e *NodeEntry) GetLastError() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.LastError
}
