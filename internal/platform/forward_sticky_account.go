package platform

import "strings"

// ForwardStickyAccount controls how a forward-proxy request derives its sticky
// account when the client did not provide one through credentials.
//
// Values are documented as pseudo tokens ($host / $domain) so they read like
// the header names used by reverse-proxy account extraction.
type ForwardStickyAccount string

const (
	// ForwardStickyAccountOff keeps stock behavior: an empty account means
	// per-request random routing (no stickiness).
	ForwardStickyAccountOff ForwardStickyAccount = "OFF"
	// ForwardStickyAccountHost sticks on the exact target host
	// (port stripped, lowercased).
	ForwardStickyAccountHost ForwardStickyAccount = "HOST"
	// ForwardStickyAccountDomain sticks on the target registrable domain,
	// computed with ICANN rules only (the Public Suffix List private section
	// is ignored), so all subdomains of one site share the same egress IP.
	ForwardStickyAccountDomain ForwardStickyAccount = "DOMAIN"
)

// NormalizeForwardStickyAccount canonicalizes user input.
//
// Accepted (case-insensitive, optional "$" prefix): "", OFF, HOST, DOMAIN.
// Empty input maps to ForwardStickyAccountOff. The second return value reports
// whether the raw value is recognized.
func NormalizeForwardStickyAccount(raw string) (ForwardStickyAccount, bool) {
	value := strings.TrimSpace(raw)
	value = strings.TrimPrefix(value, "$")
	switch ForwardStickyAccount(strings.ToUpper(value)) {
	case "", ForwardStickyAccountOff:
		return ForwardStickyAccountOff, true
	case ForwardStickyAccountHost:
		return ForwardStickyAccountHost, true
	case ForwardStickyAccountDomain:
		return ForwardStickyAccountDomain, true
	default:
		return ForwardStickyAccountOff, false
	}
}
