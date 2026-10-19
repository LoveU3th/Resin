package routing

import (
	"strconv"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

// The symptom this guards: in a pool where most nodes are failing, the two
// random picks and the handful of extra filter draws keep landing on rejected
// nodes even though acceptable ones exist — so the request gets a node the
// filter already said no to. A bounded scan of the view must find the healthy
// minority instead.
func TestRandomRoute_FindsHealthyNodeInMostlyUnhealthyPool(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-mostly-bad", "Plat", nil, nil)
	pool.addPlatform(plat)

	var badHashes []node.Hash
	for i := 0; i < 30; i++ {
		h, e := newUnhealthyEntry(t, `{"id":"mostly-bad-`+strconv.Itoa(i)+`"}`,
			"198.51.100."+strconv.Itoa(i+1), 20)
		pool.addEntry(h, e)
		badHashes = append(badHashes, h)
	}
	// Proven healthy: enough observations that the unknown-charge taper is gone
	// and the score strictly beats a rejected node.
	var goodHashes []node.Hash
	for i := 0; i < 2; i++ {
		h, e := newProvenEntry(t, `{"id":"mostly-good-`+strconv.Itoa(i)+`"}`,
			"198.51.200."+strconv.Itoa(i+1), healthTestWeights().MinSamplesForFilter)
		pool.addEntry(h, e)
		goodHashes = append(goodHashes, h)
	}
	pool.rebuildPlatformView(plat)

	if size := plat.View().Size(); size != len(badHashes)+len(goodHashes) {
		t.Fatalf("precondition: view size %d, want %d", size, len(badHashes)+len(goodHashes))
	}

	stats := NewIPLoadStats()
	const iterations = 200
	badPicks := 0
	for i := 0; i < iterations; i++ {
		got, err := randomRoute(plat, stats, pool, "example.com", nil, 10*time.Minute, healthTestWeights(), nil)
		if err != nil {
			t.Fatalf("randomRoute: %v", err)
		}
		for _, bad := range badHashes {
			if got == bad {
				badPicks++
				break
			}
		}
	}
	if badPicks != 0 {
		t.Fatalf("a rejected node was picked %d/%d times even though %d acceptable nodes exist",
			badPicks, iterations, len(goodHashes))
	}
}

// The scan skips the exclude list: a retry that has already failed on the only
// acceptable node must not be handed it again. With nothing acceptable left the
// request still routes — health must never fail a routable pool.
func TestRandomRoute_HealthyScanRespectsExclusions(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-scan-exclude", "Plat", nil, nil)
	pool.addPlatform(plat)

	for i := 0; i < 10; i++ {
		h, e := newUnhealthyEntry(t, `{"id":"scan-excl-bad-`+strconv.Itoa(i)+`"}`,
			"198.51.100."+strconv.Itoa(i+1), 20)
		pool.addEntry(h, e)
	}
	goodH, goodEntry := newProvenEntry(t, `{"id":"scan-excl-good"}`,
		"198.51.200.1", healthTestWeights().MinSamplesForFilter)
	pool.addEntry(goodH, goodEntry)
	pool.rebuildPlatformView(plat)

	stats := NewIPLoadStats()
	for i := 0; i < 50; i++ {
		got, err := randomRoute(plat, stats, pool, "example.com", nil, 10*time.Minute,
			healthTestWeights(), []node.Hash{goodH})
		if err != nil {
			t.Fatalf("randomRoute: %v", err)
		}
		if got == goodH {
			t.Fatal("the excluded node was handed out by the healthy scan")
		}
		if got == node.Zero {
			t.Fatal("routing returned the zero hash")
		}
	}
}

// With every node rejected the scan comes back empty and the ordinary
// best-effort comparison runs: a routable pool must still yield a node rather
// than an error.
func TestRandomRoute_HealthyScanFallsBackWhenNothingIsAcceptable(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-scan-none", "Plat", nil, nil)
	pool.addPlatform(plat)

	for i := 0; i < 12; i++ {
		h, e := newUnhealthyEntry(t, `{"id":"scan-none-`+strconv.Itoa(i)+`"}`,
			"198.51.100."+strconv.Itoa(i+1), 20)
		pool.addEntry(h, e)
	}
	pool.rebuildPlatformView(plat)

	stats := NewIPLoadStats()
	got, err := randomRoute(plat, stats, pool, "example.com", nil, 10*time.Minute, healthTestWeights(), nil)
	if err != nil {
		t.Fatalf("routing must not fail when nothing is acceptable: %v", err)
	}
	if got == node.Zero {
		t.Fatal("routing returned the zero hash")
	}
}

// Health filtering switched off means the scan never runs, so a pool that
// tolerates every node behaves exactly as it did before the scan existed.
func TestRandomRoute_HealthyScanDisabledWithHealthOff(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-scan-off", "Plat", nil, nil)
	pool.addPlatform(plat)

	var hashes []node.Hash
	for i := 0; i < 8; i++ {
		h, e := newUnhealthyEntry(t, `{"id":"scan-off-`+strconv.Itoa(i)+`"}`,
			"198.51.100."+strconv.Itoa(i+1), 20)
		pool.addEntry(h, e)
		hashes = append(hashes, h)
	}
	pool.rebuildPlatformView(plat)

	stats := NewIPLoadStats()
	for i := 0; i < 50; i++ {
		got, err := randomRoute(plat, stats, pool, "example.com", nil, 10*time.Minute, HealthWeights{}, nil)
		if err != nil {
			t.Fatalf("randomRoute: %v", err)
		}
		found := false
		for _, h := range hashes {
			if got == h {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("routing returned %v, which is not in the view", got)
		}
	}
}
