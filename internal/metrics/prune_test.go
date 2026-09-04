package metrics

import (
	"path/filepath"
	"testing"
	"time"
)

func TestPruneOlderThan_RemovesOnlyExpired(t *testing.T) {
	dir := t.TempDir()
	repo, err := NewMetricsRepo(filepath.Join(dir, "metrics.db"))
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer repo.Close()

	hour := int64(3600)
	old := time.Now().Add(-72 * time.Hour).Unix()
	recent := time.Now().Add(-1 * time.Hour).Unix()
	// Quantise to whole buckets, matching how the manager writes them.
	old = (old / hour) * hour
	recent = (recent / hour) * hour

	write := func(bucket int64) {
		if err := repo.WriteBucket(&BucketFlushData{
			BucketStartUnix: bucket,
			Requests:        map[string]requestAccum{"": {Total: 1, Success: 1, FirstHop: 1}},
		}); err != nil {
			t.Fatalf("write bucket %d: %v", bucket, err)
		}
	}
	write(old)
	write(recent)

	removed, err := repo.PruneOlderThan(recent)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if removed == 0 {
		t.Fatal("expected the expired bucket to be removed")
	}

	rows, err := repo.QueryRequests(old-10*hour, time.Now().Unix(), "")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for _, row := range rows {
		if row.BucketStartUnix == old {
			t.Fatalf("expired bucket %d survived pruning", old)
		}
	}
	var foundRecent bool
	for _, row := range rows {
		if row.BucketStartUnix == recent {
			foundRecent = true
		}
	}
	if !foundRecent {
		t.Fatal("the recent bucket must be kept")
	}
}

// Retention disabled must leave everything alone.
func TestPruneOlderThan_DisabledIsNoop(t *testing.T) {
	dir := t.TempDir()
	repo, err := NewMetricsRepo(filepath.Join(dir, "metrics.db"))
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer repo.Close()

	if err := repo.WriteBucket(&BucketFlushData{
		BucketStartUnix: time.Now().Unix(),
		Requests:        map[string]requestAccum{"": {Total: 1, Success: 1}},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	removed, err := repo.PruneOlderThan(0)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if removed != 0 {
		t.Fatalf("removed=%d, want 0 when retention is disabled", removed)
	}
}

// The first-hop metric is only useful if it can be read back. Writing it and
// never exposing it would leave the whole feature dead on arrival.
func TestQueryRequests_ReturnsFirstHopCounters(t *testing.T) {
	dir := t.TempDir()
	repo, err := NewMetricsRepo(filepath.Join(dir, "metrics.db"))
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer repo.Close()

	bucket := (time.Now().Unix() / 3600) * 3600
	if err := repo.WriteBucket(&BucketFlushData{
		BucketStartUnix: bucket,
		Requests: map[string]requestAccum{
			"":  {Total: 10, Success: 9, NodeTotal: 8, FirstHop: 6},
			"p": {Total: 4, Success: 4, NodeTotal: 4, FirstHop: 3},
		},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	global, err := repo.QueryRequests(bucket, bucket, "")
	if err != nil {
		t.Fatalf("query global: %v", err)
	}
	if len(global) != 1 {
		t.Fatalf("global rows: got %d, want 1", len(global))
	}
	if got := global[0].NodeRequests; got != 8 {
		t.Fatalf("node_requests: got %d, want 8", got)
	}
	if got := global[0].FirstHopSuccess; got != 6 {
		t.Fatalf("first_hop_success: got %d, want 6", got)
	}

	platform, err := repo.QueryRequests(bucket, bucket, "p")
	if err != nil {
		t.Fatalf("query platform: %v", err)
	}
	if len(platform) != 1 {
		t.Fatalf("platform rows: got %d, want 1", len(platform))
	}
	if got := platform[0].FirstHopSuccess; got != 3 {
		t.Fatalf("platform first_hop_success: got %d, want 3", got)
	}
}

// Pruning must cover every bucket table, or the ones it misses grow forever.
func TestPruneOlderThan_CoversAllBucketTables(t *testing.T) {
	dir := t.TempDir()
	repo, err := NewMetricsRepo(filepath.Join(dir, "metrics.db"))
	if err != nil {
		t.Fatalf("open repo: %v", err)
	}
	defer repo.Close()

	old := (time.Now().Add(-72*time.Hour).Unix() / 3600) * 3600
	if err := repo.WriteBucket(&BucketFlushData{
		BucketStartUnix: old,
		Requests:        map[string]requestAccum{"": {Total: 1, Success: 1, NodeTotal: 1, FirstHop: 1}},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := repo.WriteNodePoolSnapshot(old, 5, 5, 5); err != nil {
		t.Fatalf("write node pool: %v", err)
	}

	if _, err := repo.PruneOlderThan(old + 3600); err != nil {
		t.Fatalf("prune: %v", err)
	}

	rows, err := repo.QueryNodePool(old-10*3600, time.Now().Unix())
	if err != nil {
		t.Fatalf("query node pool: %v", err)
	}
	for _, row := range rows {
		if row.BucketStartUnix == old {
			t.Fatalf("metric_node_pool_bucket was not pruned")
		}
	}
}
