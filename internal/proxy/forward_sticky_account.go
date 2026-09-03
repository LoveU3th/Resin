package proxy

import (
	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/platform"
)

// resolveForwardStickyAccount derives the sticky account from the forward-proxy
// target when the client did not supply one in its credentials.
//
//   - OFF (default): keep stock behavior — an empty account routes randomly.
//   - HOST: account = target host (port stripped, lowercased).
//   - DOMAIN: account = target registrable domain (ICANN rules only), so
//     subdomains of one site stay together.
//
// An account explicitly carried by the credentials always wins, so per-client
// identity keeps working when configured.
func resolveForwardStickyAccount(
	source platform.ForwardStickyAccount,
	account string,
	target string,
) string {
	if account != "" || source == platform.ForwardStickyAccountOff {
		return account
	}
	switch source {
	case platform.ForwardStickyAccountHost:
		if host := normalizeMatchHost(target); host != "" {
			return host
		}
	case platform.ForwardStickyAccountDomain:
		if domain := netutil.ExtractICANNDomain(target); domain != "" {
			return domain
		}
	}
	return account
}
