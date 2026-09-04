package config

import "time"

// RuntimeConfig holds all hot-updatable global settings.
// These are persisted in the database and served via GET /system/config.
type RuntimeConfig struct {
	// Request log
	RequestLogEnabled                  bool `json:"request_log_enabled"`
	RequestLogTotalMaxMB               int  `json:"request_log_total_max_mb"`
	ReverseProxyLogDetailEnabled       bool `json:"reverse_proxy_log_detail_enabled"`
	ReverseProxyLogReqHeadersMaxBytes  int  `json:"reverse_proxy_log_req_headers_max_bytes"`
	ReverseProxyLogReqBodyMaxBytes     int  `json:"reverse_proxy_log_req_body_max_bytes"`
	ReverseProxyLogRespHeadersMaxBytes int  `json:"reverse_proxy_log_resp_headers_max_bytes"`
	ReverseProxyLogRespBodyMaxBytes    int  `json:"reverse_proxy_log_resp_body_max_bytes"`

	// Health check
	MaxConsecutiveFailures          int      `json:"max_consecutive_failures"`
	MaxLatencyTestInterval          Duration `json:"max_latency_test_interval"`
	MaxAuthorityLatencyTestInterval Duration `json:"max_authority_latency_test_interval"`
	MaxEgressTestInterval           Duration `json:"max_egress_test_interval"`

	// Health score: success-ratio EWMA per node.
	// HealthEwmaWindow is the effective span of the score (alpha = 1/window);
	// below HealthEwmaMinSamples observations a larger alpha is used so a
	// fresh node converges fast.
	HealthEwmaWindow     int `json:"health_ewma_window"`
	HealthEwmaMinSamples int `json:"health_ewma_min_samples"`
	// HealthPenaltyMs is added to a node's routing score per unit of
	// unhealthiness, so a score of 0.5 costs half of this. Additive rather
	// than multiplicative so that an all-healthy fleet scores exactly as it
	// did before health was considered.
	HealthPenaltyMs int `json:"health_penalty_ms"`
	// Nodes at or below HealthFilterThresholdPercent health are rejected as
	// P2C candidates once they have at least HealthMinSamplesForFilter
	// observations. Filtering is best-effort: if every candidate is filtered
	// out the original pick is used, so routing never fails because of health.
	HealthFilterThresholdPercent int `json:"health_filter_threshold_percent"`
	HealthMinSamplesForFilter    int `json:"health_min_samples_for_filter"`
	// CircuitCooldown is the minimum time a node stays isolated once its
	// breaker opens: a success before it elapses feeds the health score but
	// does not rejoin routing, which is what clamps how fast a flapping node
	// can oscillate. 0 disables the cooldown.
	CircuitCooldown Duration `json:"circuit_cooldown"`
	// CircuitMaxCooldown caps the exponential backoff applied each time a
	// half-open probe fails (30s -> 60s -> 120s...).
	CircuitMaxCooldown Duration `json:"circuit_max_cooldown"`
	// HealthRecoveryFloorPercent is the health score a node is lifted to when
	// its breaker closes. It sits above the filter threshold so the node
	// re-enters routing instead of being filtered straight back out.
	HealthRecoveryFloorPercent int `json:"health_recovery_floor_percent"`
	// HealthTransferFailureWeightPercent scales how much a mid-body failure
	// counts against the health score, as a percentage of a connect failure.
	// The node was reached in that case, so the fault may not be its own.
	// 100 weights both the same; 0 ignores transfer failures.
	HealthTransferFailureWeightPercent int `json:"health_transfer_failure_weight_percent"`
	// Request-level failover: retry a request on another node when the request
	// provably never reached the first one. Never retried once any byte of the
	// request has been written, so non-idempotent requests cannot be duplicated.
	// FailoverMaxAttempts counts the first attempt, so 1 disables retrying.
	FailoverEnabled     bool `json:"failover_enabled"`
	FailoverMaxAttempts int  `json:"failover_max_attempts"`
	// FailoverAttemptBudget bounds one attempt; reaching it abandons that
	// attempt and moves to the next node. FailoverTotalBudget bounds them all.
	FailoverAttemptBudget Duration `json:"failover_attempt_budget"`
	FailoverTotalBudget   Duration `json:"failover_total_budget"`

	// Probe
	LatencyTestURL     string   `json:"latency_test_url"`
	LatencyAuthorities []string `json:"latency_authorities"`

	// P2C
	P2CLatencyWindow   Duration `json:"p2c_latency_window"`
	LatencyDecayWindow Duration `json:"latency_decay_window"`

	// Persistence
	CacheFlushInterval       Duration `json:"cache_flush_interval"`
	CacheFlushDirtyThreshold int      `json:"cache_flush_dirty_threshold"`
}

// NewDefaultRuntimeConfig returns a RuntimeConfig populated with the default
// values specified in DESIGN.md §运行时全局设置项.
func NewDefaultRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		RequestLogEnabled:                  true,
		RequestLogTotalMaxMB:               200,
		ReverseProxyLogDetailEnabled:       false,
		ReverseProxyLogReqHeadersMaxBytes:  4096,
		ReverseProxyLogReqBodyMaxBytes:     1024,
		ReverseProxyLogRespHeadersMaxBytes: 1024,
		ReverseProxyLogRespBodyMaxBytes:    1024,

		MaxConsecutiveFailures:             3,
		HealthEwmaWindow:                   20,
		HealthEwmaMinSamples:               5,
		HealthPenaltyMs:                    2000,
		HealthFilterThresholdPercent:       40,
		HealthMinSamplesForFilter:          8,
		CircuitCooldown:                    Duration(30 * time.Second),
		CircuitMaxCooldown:                 Duration(30 * time.Minute),
		HealthRecoveryFloorPercent:         60,
		HealthTransferFailureWeightPercent: 50,
		FailoverEnabled:                    true,
		FailoverMaxAttempts:                2,
		// The attempt budget must not be shorter than ResponseHeaderTimeout, or
		// a slow-but-healthy origin would be abandoned before it could answer.
		FailoverAttemptBudget:           Duration(60 * time.Second),
		FailoverTotalBudget:             Duration(90 * time.Second),
		MaxLatencyTestInterval:          Duration(1 * time.Hour),
		MaxAuthorityLatencyTestInterval: Duration(3 * time.Hour),
		MaxEgressTestInterval:           Duration(24 * time.Hour),

		LatencyTestURL:     "https://www.gstatic.com/generate_204",
		LatencyAuthorities: []string{"gstatic.com", "google.com", "cloudflare.com", "github.com"},

		P2CLatencyWindow:   Duration(10 * time.Minute),
		LatencyDecayWindow: Duration(10 * time.Minute),

		CacheFlushInterval:       Duration(5 * time.Minute),
		CacheFlushDirtyThreshold: 1000,
	}
}

// ApplyCompatibilityDefaults fills runtime config fields that may be missing in
// configs persisted by older versions. It returns the updated config and
// whether any field was backfilled.
func ApplyCompatibilityDefaults(cfg *RuntimeConfig, envCfg *EnvConfig) (*RuntimeConfig, bool) {
	if cfg == nil {
		return NewDefaultRuntimeConfig(), true
	}

	out := *cfg
	changed := false

	if out.RequestLogTotalMaxMB <= 0 {
		out.RequestLogTotalMaxMB = defaultRequestLogTotalMaxMB(envCfg)
		changed = true
	}

	return &out, changed
}

func defaultRequestLogTotalMaxMB(envCfg *EnvConfig) int {
	if envCfg != nil && envCfg.RequestLogDBMaxMB > 0 && envCfg.RequestLogDBRetainCount > 0 {
		return envCfg.RequestLogDBMaxMB * envCfg.RequestLogDBRetainCount
	}
	return 200
}
