package proxy

import (
	"testing"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/routing"
)

// Relocating an account is a heavier decision than retrying a request: a retry
// costs one request, a relocation changes the egress IP every subsequent
// request shows. So only the verdict that blames the node outright may arm it.
func TestShouldMarkStickyRebind(t *testing.T) {
	res := routing.RouteResult{PlatformID: "plat-1", NodeHash: node.Hash{0x01}}

	tests := []struct {
		name    string
		account string
		res     routing.RouteResult
		verdict attemptVerdict
		want    bool
	}{
		{
			name:    "dial failed, node unreachable",
			account: "example.com",
			res:     res,
			verdict: attemptVerdict{retryable: true, stage: node.PassiveStageConnect},
			want:    true,
		},
		{
			name:    "connection accepted but nothing was written",
			account: "example.com",
			res:     res,
			verdict: attemptVerdict{retryable: true, deadConn: true, stage: node.PassiveStageConnect},
			want:    true,
		},
		{
			name:    "pooled connection was dead",
			account: "example.com",
			res:     res,
			verdict: attemptVerdict{deadConn: true, stage: node.PassiveStageTransfer},
			want:    false,
		},
		{
			name:    "attempt abandoned, origin too slow",
			account: "example.com",
			res:     res,
			verdict: attemptVerdict{stage: node.PassiveStageTransfer, slow: true},
			want:    false,
		},
		{
			name:    "request went out and then failed",
			account: "example.com",
			res:     res,
			verdict: attemptVerdict{stage: node.PassiveStageTransfer},
			want:    false,
		},
		{
			name:    "attempt succeeded",
			account: "example.com",
			res:     res,
			verdict: attemptVerdict{},
			want:    false,
		},
		{
			name:    "no sticky account, so there is no lease to move",
			account: "",
			res:     res,
			verdict: attemptVerdict{retryable: true, stage: node.PassiveStageConnect},
			want:    false,
		},
		{
			name:    "no platform to move a lease on",
			account: "example.com",
			res:     routing.RouteResult{NodeHash: node.Hash{0x01}},
			verdict: attemptVerdict{retryable: true, stage: node.PassiveStageConnect},
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldMarkStickyRebind(tc.account, tc.res, tc.verdict); got != tc.want {
				t.Fatalf("shouldMarkStickyRebind = %v, want %v", got, tc.want)
			}
		})
	}
}
