package metrics

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/Resinat/Resin/internal/state"
)

// oldMetricsDDL is the v1.0.11 (pre-upgrade) metrics schema: metric_request_bucket
// has no node_requests / first_hop_success.
const oldMetricsDDL = `
CREATE TABLE IF NOT EXISTS metric_traffic_bucket (
	bucket_start_unix INTEGER PRIMARY KEY,
	ingress_bytes     INTEGER NOT NULL DEFAULT 0,
	egress_bytes      INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS metric_request_bucket (
	bucket_start_unix  INTEGER NOT NULL,
	platform_id        TEXT,
	total_requests     INTEGER NOT NULL DEFAULT 0,
	success_requests   INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_metric_request_bucket_dim
	ON metric_request_bucket(bucket_start_unix, platform_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_metric_request_bucket_global
	ON metric_request_bucket(bucket_start_unix)
	WHERE platform_id IS NULL;
CREATE TABLE IF NOT EXISTS metric_probe_bucket (
	bucket_start_unix INTEGER PRIMARY KEY,
	total_count       INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS metric_node_pool_bucket (
	bucket_start_unix INTEGER PRIMARY KEY,
	total_nodes       INTEGER NOT NULL DEFAULT 0,
	healthy_nodes     INTEGER NOT NULL DEFAULT 0,
	egress_ip_count   INTEGER NOT NULL DEFAULT 0
);
`

func columnsOf(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		t.Fatalf("table_info %s: %v", table, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var def sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &def, &pk); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, name)
	}
	return out
}

// 场景1：老库升级 —— 用旧版 DDL 造库，新代码打开
func TestUpgrade_OldMetricsDB_AddsColumnsKeepsRows(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metrics.db")

	old, err := state.OpenDB(path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	if _, err := old.Exec(oldMetricsDDL); err != nil {
		t.Fatalf("seed ddl: %v", err)
	}
	if _, err := old.Exec(`INSERT INTO metric_request_bucket (bucket_start_unix, platform_id, total_requests, success_requests) VALUES (1000, NULL, 7, 5)`); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if _, err := old.Exec(`INSERT INTO metric_request_bucket (bucket_start_unix, platform_id, total_requests, success_requests) VALUES (1000, 'plat-a', 3, 2)`); err != nil {
		t.Fatalf("seed insert dim: %v", err)
	}
	if _, err := old.Exec(`INSERT INTO metric_traffic_bucket VALUES (1000, 111, 222)`); err != nil {
		t.Fatalf("seed traffic: %v", err)
	}
	old.Close()

	r, err := NewMetricsRepo(path)
	if err != nil {
		t.Fatalf("BLOCKING: 旧 metrics.db 无法打开: %v", err)
	}
	defer r.Close()

	cols := columnsOf(t, r.db, "metric_request_bucket")
	t.Logf("升级后列: %v", cols)
	for _, want := range []string{"node_requests", "first_hop_success"} {
		found := false
		for _, c := range cols {
			if c == want {
				found = true
			}
		}
		if !found {
			t.Errorf("新列 %s 未被添加", want)
		}
	}

	rows, err := r.QueryRequests(0, 5000, "")
	if err != nil {
		t.Fatalf("查询旧数据失败: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("旧行丢失: got %d want 1", len(rows))
	}
	if rows[0].TotalRequests != 7 || rows[0].SuccessRequests != 5 {
		t.Errorf("旧数据被破坏: %+v", rows[0])
	}
	if rows[0].NodeRequests != 0 || rows[0].FirstHopSuccess != 0 {
		t.Errorf("旧行新列应为 0 值: %+v", rows[0])
	}
	dim, err := r.QueryRequests(0, 5000, "plat-a")
	if err != nil || len(dim) != 1 || dim[0].TotalRequests != 3 {
		t.Errorf("维度行读回失败: %v %+v", err, dim)
	}
}

// 场景2：全新库建表
func TestUpgrade_FreshMetricsDB_HasNewColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.db")
	r, err := NewMetricsRepo(path)
	if err != nil {
		t.Fatalf("新建库失败: %v", err)
	}
	defer r.Close()
	cols := columnsOf(t, r.db, "metric_request_bucket")
	t.Logf("新建库列: %v", cols)
	if len(cols) != 6 {
		t.Errorf("新建库列数异常: %v", cols)
	}
}

// 场景5：写入 -> 重开 -> 读回
func TestUpgrade_MetricsWriteReopenReadBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.db")
	r, err := NewMetricsRepo(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	data := &BucketFlushData{
		BucketStartUnix: 2000,
		Traffic:         trafficAccum{IngressBytes: 1, EgressBytes: 2},
		Requests: map[string]requestAccum{
			"":   {Total: 10, Success: 8, NodeTotal: 9, FirstHop: 6},
			"p1": {Total: 4, Success: 4, NodeTotal: 4, FirstHop: 3},
		},
	}
	if err := r.WriteBucket(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r2, err := NewMetricsRepo(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r2.Close()
	got, err := r2.QueryRequests(0, 5000, "")
	if err != nil || len(got) != 1 {
		t.Fatalf("读回失败: %v %+v", err, got)
	}
	if got[0].NodeRequests != 9 || got[0].FirstHopSuccess != 6 {
		t.Errorf("BLOCKING: 新字段读回为零值/错值: %+v", got[0])
	}
	dim, _ := r2.QueryRequests(0, 5000, "p1")
	if len(dim) != 1 || dim[0].NodeRequests != 4 || dim[0].FirstHopSuccess != 3 {
		t.Errorf("维度新字段读回错误: %+v", dim)
	}
}

func mustOpen(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := state.OpenDB(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// 边界：表不存在时 hasTableColumn 的行为
func TestUpgrade_EnsureColumnOnMissingTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.db")
	db, err := state.OpenDB(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE t (a INTEGER)`)
	if err != nil {
		t.Fatal(err)
	}
	err = state.EnsureTableColumn(db, "no_such_table", "x", "x INTEGER NOT NULL DEFAULT 0")
	t.Logf("表不存在时 EnsureTableColumn 返回: %v", err)
	if err == nil {
		t.Errorf("表不存在时静默返回 nil（PRAGMA table_info 对不存在的表返回空行集，无法区分“表缺列”与“表不存在”）")
	}
}

// 场景6：降级 —— 旧代码在新库上跑（旧 DDL + 旧 INSERT/SELECT）
func TestDowngrade_OldCodeAgainstNewDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.db")
	r, err := NewMetricsRepo(path)
	if err != nil {
		t.Fatal(err)
	}
	data := &BucketFlushData{
		BucketStartUnix: 3000,
		Requests:        map[string]requestAccum{"": {Total: 1, Success: 1, NodeTotal: 1, FirstHop: 1}},
	}
	if err := r.WriteBucket(data); err != nil {
		t.Fatal(err)
	}
	r.Close()

	// 模拟旧代码：执行旧 DDL（IF NOT EXISTS 不会改表），再用旧 SQL 写入/读取
	db, err := state.OpenDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(oldMetricsDDL); err != nil {
		t.Fatalf("旧 DDL 执行失败: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO metric_request_bucket (bucket_start_unix, platform_id, total_requests, success_requests)
		VALUES (3000, NULL, 99, 98) ON CONFLICT(bucket_start_unix) WHERE platform_id IS NULL
		DO UPDATE SET total_requests = excluded.total_requests, success_requests = excluded.success_requests`); err != nil {
		t.Errorf("旧代码写入新库失败: %v", err)
	}
	var total, succ int64
	if err := db.QueryRow(`SELECT total_requests, success_requests FROM metric_request_bucket WHERE bucket_start_unix = 3000 AND platform_id IS NULL`).Scan(&total, &succ); err != nil {
		t.Errorf("旧代码读取新库失败: %v", err)
	}
	t.Logf("旧代码读回: total=%d success=%d", total, succ)
}
