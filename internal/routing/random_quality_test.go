package routing

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
)

// A target the pair has not both probed must not erase what one side did
// measure: the per-node authority average is the fallback.
func TestCompareLatencies_FallsBackToAuthorityAverage(t *testing.T) {
	pool := newRouterTestPool()

	h1, e1 := newRoutableEntry(t, `{"id":"auth-a"}`, "203.0.113.60")
	h2, e2 := newRoutableEntry(t, `{"id":"auth-b"}`, "203.0.113.61")
	e1.LatencyTable.Update("probe.example", 40*time.Millisecond, 10*time.Minute)
	waitForDomainLatency(t, e1, "probe.example")
	// h2 has no probe.example sample, and neither node has the target domain.

	pool.addEntry(h1, e1)
	pool.addEntry(h2, e2)

	lat1, lat2 := compareLatencies(
		h1, h2, pool,
		"never-probed.example",
		[]string{"probe.example"},
		10*time.Minute,
	)
	if lat1 != 40*time.Millisecond {
		t.Fatalf("measured side lost its authority average: got %v, want 40ms", lat1)
	}
	if lat2 != 0 {
		t.Fatalf("unmeasured side must stay unknown, got %v", lat2)
	}
}

// "No measurement" used to score zero, which beat every measured node and made
// the latency policies meaningless. It must now cost a cold-start estimate.
func TestCalculateScore_UnknownLatencyIsNotFree(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat", "Plat", nil, nil)
	plat.AllocationPolicy = platform.AllocationPolicyBalanced
	pool.addPlatform(plat)

	measured, measuredEntry := newProvenEntry(t, `{"id":"measured"}`, "203.0.113.62", 8)
	unknown, unknownEntry := newProvenEntry(t, `{"id":"unknown"}`, "203.0.113.63", 8)
	pool.addEntry(measured, measuredEntry)
	pool.addEntry(unknown, unknownEntry)

	stats := NewIPLoadStats()
	measuredScore := calculateScore(measured, 100*time.Millisecond, plat, stats, pool, HealthWeights{})
	unknownScore := calculateScore(unknown, 0, plat, stats, pool, HealthWeights{})

	if unknownScore == 0 {
		t.Fatal("an unmeasured latency must not score zero")
	}
	if measuredScore >= unknownScore {
		t.Fatalf("measured node must beat the unmeasured one: %v vs %v", measuredScore, unknownScore)
	}
	if want := float64(coldStartLatency); unknownScore != want {
		t.Fatalf("cold-start score: got %v, want %v", unknownScore, want)
	}

	// PreferIdleIP is about spreading leases; latency, known or not, does not
	// enter the score there.
	plat.AllocationPolicy = platform.AllocationPolicyPreferIdleIP
	if got := calculateScore(unknown, 0, plat, stats, pool, HealthWeights{}); got != 0 {
		t.Fatalf("PreferIdleIP must score by lease count only, got %v", got)
	}
}

// End to end: a node with a real measurement for the target must take far more
// than its random share of the traffic.
func TestRandomRoute_PrefersMeasuredLatencyOverUnknown(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat", "Plat", nil, nil)
	pool.addPlatform(plat)

	measured, measuredEntry := newProvenEntry(t, `{"id":"lat-known"}`, "203.0.113.70", 8)
	measuredEntry.LatencyTable.Update("example.com", 30*time.Millisecond, 10*time.Minute)
	waitForDomainLatency(t, measuredEntry, "example.com")
	pool.addEntry(measured, measuredEntry)
	for i := 0; i < 4; i++ {
		h, e := newProvenEntry(t, `{"id":"lat-unknown-`+string(rune('a'+i))+`"}`, "203.0.113.7"+string(rune('1'+i)), 8)
		pool.addEntry(h, e)
	}
	pool.rebuildPlatformView(plat)

	stats := NewIPLoadStats()
	const iterations = 200
	picks := 0
	for i := 0; i < iterations; i++ {
		got, err := randomRoute(plat, stats, pool, "example.com", nil, 10*time.Minute, healthTestWeights(), nil)
		if err != nil {
			t.Fatalf("randomRoute: %v", err)
		}
		if got == measured {
			picks++
		}
	}
	// A pure random share would be 20%; being in the field of two is 40%.
	// Anything below 30% means latency is still not participating.
	if picks < iterations*3/10 {
		t.Fatalf("measured node picked %d/%d times, want >= %d", picks, iterations, iterations*3/10)
	}
}

// The draw width must follow how much of the pool the health filter accepts.
func TestChoiceWidth_ScalesWithAcceptableShare(t *testing.T) {
	cases := []struct {
		name    string
		good    int
		bad     int
		weights HealthWeights
		want    int
	}{
		{name: "healthy fleet keeps two-choice", good: 6, bad: 0, weights: healthTestWeights(), want: 2},
		{name: "half acceptable widens to three", good: 5, bad: 5, weights: healthTestWeights(), want: 3},
		{name: "poor pool widens to four", good: 2, bad: 8, weights: healthTestWeights(), want: 4},
		{name: "health off keeps two-choice", good: 2, bad: 8, weights: HealthWeights{}, want: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newRouterTestPool()
			plat := platform.NewPlatform("plat-"+tc.name, "Plat", nil, nil)
			pool.addPlatform(plat)

			for i := 0; i < tc.good; i++ {
				h, e := newProvenEntry(t, `{"id":"w-good-`+tc.name+string(rune('a'+i))+`"}`, "198.51.100."+string(rune('1'+i)), 8)
				pool.addEntry(h, e)
			}
			for i := 0; i < tc.bad; i++ {
				h, e := newUnhealthyEntry(t, `{"id":"w-bad-`+tc.name+string(rune('a'+i))+`"}`, "203.0.113."+string(rune('1'+i)), 20)
				pool.addEntry(h, e)
			}
			pool.rebuildPlatformView(plat)

			rng := rand.New(rand.NewPCG(1, 2))
			if got := choiceWidth(plat.View(), pool, tc.weights, rng); got != tc.want {
				t.Fatalf("choice width: got %d, want %d", got, tc.want)
			}
		})
	}
}

// Whichever width is chosen, routing must still return a routable node and
// never the zero hash.
func TestRandomRoute_WideDrawStillRoutesInPoorPool(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-poor", "Plat-Poor", nil, nil)
	pool.addPlatform(plat)

	h, e := newProvenEntry(t, `{"id":"poor-good"}`, "198.51.100.90", 8)
	pool.addEntry(h, e)
	for i := 0; i < 9; i++ {
		bh, be := newUnhealthyEntry(t, `{"id":"poor-bad-`+string(rune('a'+i))+`"}`, "203.0.113."+string(rune('1'+i)), 20)
		pool.addEntry(bh, be)
	}
	pool.rebuildPlatformView(plat)

	stats := NewIPLoadStats()
	for i := 0; i < 50; i++ {
		got, err := randomRoute(plat, stats, pool, "example.com", nil, 10*time.Minute, healthTestWeights(), nil)
		if err != nil {
			t.Fatalf("routing must not fail in a poor pool: %v", err)
		}
		if got == node.Zero {
			t.Fatal("routing returned the zero hash")
		}
	}
}
