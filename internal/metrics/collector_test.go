package metrics

import "testing"

func TestCollector_RecordLatency_BoundaryAndOverflowBuckets(t *testing.T) {
	c := NewCollector(100, 3000)

	// Boundary value: overflow_ms itself should go to overflow bucket.
	c.RecordRequest("", true, 3000, false, true, true)
	// Strictly greater than overflow_ms should go to overflow bucket.
	c.RecordRequest("", true, 3001, false, true, true)
	// 100ms should be in second bucket [100,200), not first.
	c.RecordRequest("", true, 100, false, true, true)

	snap := c.Snapshot()
	regularBins := (3000 + 100 - 1) / 100
	if len(snap.LatencyBuckets) != regularBins+1 {
		t.Fatalf("bucket count: got %d, want %d", len(snap.LatencyBuckets), regularBins+1)
	}

	if snap.LatencyBuckets[0] != 0 {
		t.Fatalf("first bucket count: got %d, want %d", snap.LatencyBuckets[0], 0)
	}
	if snap.LatencyBuckets[1] != 1 {
		t.Fatalf("second bucket count: got %d, want %d", snap.LatencyBuckets[1], 1)
	}
	if snap.LatencyBuckets[regularBins-1] != 0 {
		t.Fatalf("last regular bucket count: got %d, want %d", snap.LatencyBuckets[regularBins-1], 0)
	}
	if snap.LatencyBuckets[regularBins] != 2 {
		t.Fatalf("overflow bucket count: got %d, want %d", snap.LatencyBuckets[regularBins], 2)
	}
}

func TestCollector_SwapConnectionWindowMax_TracksPeakAndResetsBaseline(t *testing.T) {
	c := NewCollector(100, 3000)

	// inbound: 0 -> 1 -> 2 -> 1, outbound: 0 -> 1 -> 0
	c.RecordConnection(ConnInbound, 1)
	c.RecordConnection(ConnInbound, 1)
	c.RecordConnection(ConnInbound, -1)
	c.RecordConnection(ConnOutbound, 1)
	c.RecordConnection(ConnOutbound, -1)

	inboundMax, outboundMax := c.SwapConnectionWindowMax()
	if inboundMax != 2 || outboundMax != 1 {
		t.Fatalf("first window max mismatch: inbound=%d outbound=%d", inboundMax, outboundMax)
	}

	// No new events: next window max should reflect current active levels.
	inboundMax, outboundMax = c.SwapConnectionWindowMax()
	if inboundMax != 1 || outboundMax != 0 {
		t.Fatalf("second window max mismatch: inbound=%d outbound=%d", inboundMax, outboundMax)
	}
}

// The gap between success rate and first-hop success rate is the whole point:
// failover makes the final rate look healthy while nodes are quietly failing.
func TestRecordRequest_FirstHopTrackedSeparately(t *testing.T) {
	c := NewCollector(50, 5000)

	// Served by the first node tried.
	c.RecordRequest("p", true, 10, false, true, true)
	// Served only after another node took over.
	c.RecordRequest("p", true, 10, false, false, true)
	// Failed outright.
	c.RecordRequest("p", false, 10, false, false, true)

	snap := c.Snapshot()
	if snap.Requests != 3 {
		t.Fatalf("requests: got %d, want 3", snap.Requests)
	}
	if snap.SuccessRequests != 2 {
		t.Fatalf("success: got %d, want 2", snap.SuccessRequests)
	}
	if snap.FirstHopSuccess != 1 {
		t.Fatalf("firstHop: got %d, want 1", snap.FirstHopSuccess)
	}

	platforms := c.PlatformSnapshots()
	p, ok := platforms["p"]
	if !ok {
		t.Fatal("expected a platform snapshot")
	}
	if p.FirstHopSuccess != 1 {
		t.Fatalf("platform firstHop: got %d, want 1", p.FirstHopSuccess)
	}
}

// First-hop success can never exceed the request count, or the implied rate
// would be nonsense.
func TestAddRequestCounts_ClampsFirstHopToTotal(t *testing.T) {
	b := NewBucketAggregator(300)
	b.AddRequestCounts("p", 10, 12, 99, 99)

	_, total, success, node, firstHop := b.RequestCounts("p")
	if total != 10 {
		t.Fatalf("total: got %d, want 10", total)
	}
	if success != 10 {
		t.Fatalf("success should be clamped to total: got %d, want 10", success)
	}
	if node != 10 {
		t.Fatalf("node requests should be clamped to total: got %d, want 10", node)
	}
	if firstHop != 10 {
		t.Fatalf("firstHop should be clamped to node requests: got %d, want 10", firstHop)
	}
}

// Negative deltas can appear when counters reset; they must not become negative
// bucket values.
func TestAddRequestCounts_RejectsNegative(t *testing.T) {
	b := NewBucketAggregator(300)
	b.AddRequestCounts("p", 5, -1, -1, -1)

	_, _, success, node, firstHop := b.RequestCounts("p")
	if success != 0 {
		t.Fatalf("success: got %d, want 0", success)
	}
	if node != 0 {
		t.Fatalf("node requests: got %d, want 0", node)
	}
	if firstHop != 0 {
		t.Fatalf("firstHop: got %d, want 0", firstHop)
	}
}

// Bypassed traffic never touches a node. Counting it as a first-hop success
// would lift the rate and hide real node failures.
func TestRecordRequest_BypassExcludedFromFirstHop(t *testing.T) {
	c := NewCollector(50, 5000)

	// Nine node requests, only five served on the first hop.
	for i := 0; i < 5; i++ {
		c.RecordRequest("p", true, 10, false, true, true)
	}
	for i := 0; i < 4; i++ {
		c.RecordRequest("p", true, 10, false, false, true)
	}
	// Ten bypassed requests: successful, but telling us nothing about nodes.
	for i := 0; i < 10; i++ {
		c.RecordRequest("p", true, 10, false, false, false)
	}

	snap := c.Snapshot()
	if snap.Requests != 19 {
		t.Fatalf("requests: got %d, want 19 (bypass still counts as traffic)", snap.Requests)
	}
	if snap.NodeRequests != 9 {
		t.Fatalf("node requests: got %d, want 9", snap.NodeRequests)
	}
	if snap.FirstHopSuccess != 5 {
		t.Fatalf("firstHop: got %d, want 5", snap.FirstHopSuccess)
	}
	// Without the exclusion the rate would read 15/19 instead of 5/9.
	if rate := float64(snap.FirstHopSuccess) / float64(snap.NodeRequests); rate > 0.6 {
		t.Fatalf("bypass inflated the first-hop rate: %v", rate)
	}
}
