package proxy

import (
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// shouldMarkStickyRebind reports whether an attempt's outcome says the node
// backing the account's sticky lease could not be reached.
//
// Only connect-stage failures count. A transfer-stage failure means the node
// was reached and the origin may simply be slow or have reset the connection,
// and a "slow" attribution says the same thing from the other side — moving a
// lease over either would relocate an account's egress IP for something the
// node may not even be responsible for. Retrying a request and relocating an
// account are different decisions, which is why the same verdict drives the
// first everywhere but the second only here.
//
// The account is the sticky key. It is empty for traffic that routes without a
// lease, and in that case there is nothing to move.
func shouldMarkStickyRebind(account string, res routing.RouteResult, verdict attemptVerdict) bool {
	if account == "" || res.PlatformID == "" {
		return false
	}
	return verdict.retryable && verdict.stage == node.PassiveStageConnect
}

// markStickyRebind flags the account's lease for migration when this attempt
// failed because the node could not be reached. It is deliberately independent
// of health recording: a lease must still be moved off an unreachable node when
// passive health feedback is switched off.
func markStickyRebind(
	router *routing.Router,
	account string,
	res routing.RouteResult,
	verdict attemptVerdict,
) {
	if router == nil || !shouldMarkStickyRebind(account, res, verdict) {
		return
	}
	router.MarkStickyNodeFailure(res.PlatformID, account, res.NodeHash)
}
