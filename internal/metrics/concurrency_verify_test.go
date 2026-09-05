package metrics

// Concurrency verification for the bucket aggregator.
//
// The question for RequestCounts is whether a single locked read really is
// indivisible against AddRequestCounts and MaybeFlush/ForceFlush. The invariant
// that matters is conservation: every counter added is either still in the
// current bucket or has been handed out by exactly one flush, and no snapshot
// may ever observe firstHop > nodeTotal (the ordering the single lock exists to
// protect).

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCV_BucketRequestCountsConservation runs writers, readers and flushers
// concurrently and checks both the snapshot invariant and the conservation of
// every counter across flushes.
func TestCV_BucketRequestCountsConservation(t *testing.T) {
	// A 1s bucket means MaybeFlush actually fires during the run, which is the
	// interleaving a long-lived process sees in production.
	agg := NewBucketAggregator(1)

	const (
		writers   = 16
		perWriter = 2000
		readers   = 4
		flushers  = 2
	)
	wantTotal := int64(writers * perWriter)
	// Every write records a success; nodeTotal covers the 3/4 of requests that
	// went through a node; firstHop the 2/4 served by the first node tried.
	wantSuccess := int64(writers * perWriter)
	wantNodeTotal := int64(writers * perWriter * 3 / 4)
	wantFirstHop := int64(writers * perWriter / 2)

	// Flushed counters are aggregated by the flush goroutines.
	var flushMu sync.Mutex
	var flushedTotal, flushedSuccess, flushedNodeTotal, flushedFirstHop int64
	var snapshots atomic.Int64
	var invariantViolations atomic.Int64

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var writersWG sync.WaitGroup

	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer writersWG.Done()
			for i := 0; i < perWriter; i++ {
				switch i % 4 {
				case 0, 1: // served by the first node tried
					agg.AddRequestCounts("plat", 1, 1, 1, 1)
				case 2: // went through a node but not the first one
					agg.AddRequestCounts("plat", 1, 1, 1, 0)
				default: // bypassed: no node involved
					agg.AddRequestCounts("plat", 1, 1, 0, 0)
				}
			}
		}()
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, total, success, nodeTotal, firstHop := agg.RequestCounts("")
				if firstHop > nodeTotal || nodeTotal > total || success > total {
					invariantViolations.Add(1)
					t.Errorf("torn read: total=%d success=%d nodeTotal=%d firstHop=%d",
						total, success, nodeTotal, firstHop)
					return
				}
				// The same check for the platform scope.
				_, pTotal, pSuccess, pNodeTotal, pFirstHop := agg.RequestCounts("plat")
				if pFirstHop > pNodeTotal || pNodeTotal > pTotal || pSuccess > pTotal {
					invariantViolations.Add(1)
					t.Errorf("torn platform read: total=%d success=%d nodeTotal=%d firstHop=%d",
						pTotal, pSuccess, pNodeTotal, pFirstHop)
					return
				}
				snapshots.Add(1)
			}
		}()
	}

	for f := 0; f < flushers; f++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					// Drain whatever is left so conservation can be checked.
					if data := agg.ForceFlush(); data != nil {
						flushMu.Lock()
						if g, ok := data.Requests[""]; ok {
							flushedTotal += g.Total
							flushedSuccess += g.Success
							flushedNodeTotal += g.NodeTotal
							flushedFirstHop += g.FirstHop
						}
						flushMu.Unlock()
					}
					return
				default:
				}
				if data := agg.MaybeFlush(time.Now()); data != nil {
					flushMu.Lock()
					if g, ok := data.Requests[""]; ok {
						flushedTotal += g.Total
						flushedSuccess += g.Success
						flushedNodeTotal += g.NodeTotal
						flushedFirstHop += g.FirstHop
					}
					flushMu.Unlock()
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Wait for the writers, then let the flushers drain.
	writersWG.Wait()
	close(stop)
	wg.Wait()

	_, total, success, nodeTotal, firstHop := agg.RequestCounts("")
	flushMu.Lock()
	gotTotal := flushedTotal + total
	gotSuccess := flushedSuccess + success
	gotNodeTotal := flushedNodeTotal + nodeTotal
	gotFirstHop := flushedFirstHop + firstHop
	flushMu.Unlock()

	t.Logf("snapshots=%d invariant violations=%d", snapshots.Load(), invariantViolations.Load())
	if got := invariantViolations.Load(); got != 0 {
		t.Fatalf("BLOCKING: %d snapshots saw a counter combination that cannot exist", got)
	}
	if gotTotal != wantTotal {
		t.Fatalf("total: added %d, accounted %d (flushed=%d current=%d)",
			wantTotal, gotTotal, flushedTotal, total)
	}
	if gotSuccess != wantSuccess {
		t.Fatalf("success: added %d, accounted %d", wantSuccess, gotSuccess)
	}
	if gotNodeTotal != wantNodeTotal {
		t.Fatalf("nodeTotal: added %d, accounted %d", wantNodeTotal, gotNodeTotal)
	}
	if gotFirstHop != wantFirstHop {
		t.Fatalf("firstHop: added %d, accounted %d", wantFirstHop, gotFirstHop)
	}
}

// Concurrent traffic and probe accumulation must not lose deltas either.
func TestCV_BucketTrafficAndProbesAreNotLost(t *testing.T) {
	agg := NewBucketAggregator(3600) // wide bucket: no flush during the run

	// Kept modest on purpose: SnapshotLeaseLifetimeSamples copies the whole
	// slice, so snapshots during accumulation are quadratic in the sample count.
	const goroutines = 32
	const perGoroutine = 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				agg.AddTraffic(2, 3)
				agg.AddProbeCount(1)
				agg.AddLeaseLifetime("plat", int64(i))
				_, _, _ = agg.SnapshotTraffic()
				_, _ = agg.SnapshotProbes()
				_, _ = agg.SnapshotLeaseLifetimeSamples("plat")
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * perGoroutine)
	if _, ingress, egress := agg.SnapshotTraffic(); ingress != 2*want || egress != 3*want {
		t.Fatalf("traffic: got ingress=%d egress=%d, want %d / %d", ingress, egress, 2*want, 3*want)
	}
	if _, probes := agg.SnapshotProbes(); probes != want {
		t.Fatalf("probes: got %d, want %d", probes, want)
	}
	if _, samples := agg.SnapshotLeaseLifetimeSamples("plat"); int64(len(samples)) != want {
		t.Fatalf("lease lifetimes: got %d samples, want %d", len(samples), want)
	}
}

// A flush hands the accumulated data out and must not keep writing into it:
// the flush data is consumed by another goroutine while writers keep adding.
func TestCV_FlushDataIsNotAliasedByLaterWrites(t *testing.T) {
	agg := NewBucketAggregator(3600)
	agg.AddRequestCounts("plat", 10, 10, 10, 10)

	data := agg.ForceFlush()
	if data == nil {
		t.Fatal("expected flush data")
	}
	global, ok := data.Requests[""]
	if !ok || global.Total != 10 {
		t.Fatalf("flushed global counters: got %+v, want Total=10", global)
	}

	// More traffic after the flush must not change the handed-out struct.
	agg.AddRequestCounts("plat", 5, 5, 5, 5)
	if global.Total != 10 {
		t.Fatalf("flushed data aliased by a later write: Total=%d", global.Total)
	}
	if _, total, _, _, _ := agg.RequestCounts(""); total != 5 {
		t.Fatalf("current bucket: got total=%d, want 5", total)
	}
}
