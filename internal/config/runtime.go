package config

import "time"

// RuntimeConfigSchemaVersion marks the shape of a persisted RuntimeConfig.
// Bump it whenever fields are added, and list them in
// applyRuntimeConfigMigrations under the new version.
//
// A config written before this mechanism existed has no schema_version key and
// reads back as 0, which is exactly what a config written by an old release
// looks like — so 0 means "predates versioning" rather than "version 0".
const RuntimeConfigSchemaVersion = 1

// RuntimeConfig holds all hot-updatable global settings.
// These are persisted in the database and served via GET /system/config.
type RuntimeConfig struct {
	// SchemaVersion records which set of fields this config was written with.
	// Fields added by a later release are filled in on load; see
	// applyRuntimeConfigMigrations. Not patchable: it describes the stored
	// document, it is not a setting.
	SchemaVersion int `json:"schema_version"`

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
		SchemaVersion: RuntimeConfigSchemaVersion,

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

	// MaxConsecutiveFailures predates the schema-versioning below, so it is
	// still backfilled unconditionally. Zero means the breaker never fires (the
	// check is `> 0 &&`), and for a field this old there is no way left to tell
	// a missing key from an operator's deliberate zero.
	if out.MaxConsecutiveFailures <= 0 {
		// Zero means the breaker never fires: the check is `> 0 &&`.
		out.MaxConsecutiveFailures = 3
		changed = true
	}

	// Fields added by a release are filled in once, on the upgrade that
	// introduces them — see applyRuntimeConfigMigrations.
	if applyRuntimeConfigMigrations(&out) {
		changed = true
	}

	return &out, changed
}

// applyRuntimeConfigMigrations fills in fields that did not exist yet when the
// persisted config was written, then stamps it with the current schema version
// so the fill-in happens exactly once per upgrade.
//
// Only zero values are filled, so anything the operator has already touched
// keeps its value. That leaves one unavoidable trade-off: an operator who set a
// new field to 0 before this mechanism existed has that 0 replaced once, on the
// upgrade that introduces it. The alternative is what shipped before — a
// missing key reads back as 0 and silently switches the feature off, which no
// amount of documentation talked anyone out of.
func applyRuntimeConfigMigrations(cfg *RuntimeConfig) bool {
	if cfg.SchemaVersion >= RuntimeConfigSchemaVersion {
		return false
	}

	defaults := NewDefaultRuntimeConfig()

	if cfg.SchemaVersion < 1 {
		// v1: node health scoring and request-level failover.
		//
		// An old release cannot have written these fields, so a zero here means
		// "never set" rather than "set to zero" — unlike MaxConsecutiveFailures
		// above, which predates this scheme and can be any operator value.
		if cfg.HealthEwmaWindow <= 0 {
			cfg.HealthEwmaWindow = defaults.HealthEwmaWindow
		}
		if cfg.HealthEwmaMinSamples <= 0 {
			cfg.HealthEwmaMinSamples = defaults.HealthEwmaMinSamples
		}
		if cfg.HealthPenaltyMs == 0 {
			cfg.HealthPenaltyMs = defaults.HealthPenaltyMs
		}
		if cfg.HealthFilterThresholdPercent == 0 {
			cfg.HealthFilterThresholdPercent = defaults.HealthFilterThresholdPercent
		}
		if cfg.HealthMinSamplesForFilter <= 0 {
			cfg.HealthMinSamplesForFilter = defaults.HealthMinSamplesForFilter
		}
		if cfg.CircuitCooldown == 0 {
			cfg.CircuitCooldown = defaults.CircuitCooldown
		}
		if cfg.CircuitMaxCooldown == 0 {
			cfg.CircuitMaxCooldown = defaults.CircuitMaxCooldown
		}
		if cfg.HealthRecoveryFloorPercent == 0 {
			cfg.HealthRecoveryFloorPercent = defaults.HealthRecoveryFloorPercent
		}
		if cfg.HealthTransferFailureWeightPercent == 0 {
			cfg.HealthTransferFailureWeightPercent = defaults.HealthTransferFailureWeightPercent
		}
		// Failover is keyed off MaxAttempts rather than Enabled: Enabled is a
		// bool, so a config that predates the feature and one that deliberately
		// disables it are indistinguishable. MaxAttempts is validated to be at
		// least 1, so a zero can only mean "never configured".
		if cfg.FailoverMaxAttempts <= 0 {
			cfg.FailoverEnabled = defaults.FailoverEnabled
			cfg.FailoverMaxAttempts = defaults.FailoverMaxAttempts
			cfg.FailoverAttemptBudget = defaults.FailoverAttemptBudget
			cfg.FailoverTotalBudget = defaults.FailoverTotalBudget
		}
	}

	cfg.SchemaVersion = RuntimeConfigSchemaVersion
	return true
}

func defaultRequestLogTotalMaxMB(envCfg *EnvConfig) int {
	if envCfg != nil && envCfg.RequestLogDBMaxMB > 0 && envCfg.RequestLogDBRetainCount > 0 {
		return envCfg.RequestLogDBMaxMB * envCfg.RequestLogDBRetainCount
	}
	return 200
}
