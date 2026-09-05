package requestlog

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/state"
)

// oldCreateDDL is the v1.0.11 (pre-upgrade) request log schema: request_logs has
// neither failover_attempts nor failover_nodes.
const oldCreateDDL = `
CREATE TABLE IF NOT EXISTS request_logs (
	id                    TEXT PRIMARY KEY,
	ts_ns                 INTEGER NOT NULL,
	proxy_type            INTEGER NOT NULL,
	client_ip             TEXT NOT NULL DEFAULT '',
	platform_id           TEXT NOT NULL DEFAULT '',
	platform_name         TEXT NOT NULL DEFAULT '',
	account               TEXT NOT NULL DEFAULT '',
	target_host           TEXT NOT NULL DEFAULT '',
	target_url            TEXT NOT NULL DEFAULT '',
	node_hash             TEXT NOT NULL DEFAULT '',
	node_tag              TEXT NOT NULL DEFAULT '',
	egress_ip             TEXT NOT NULL DEFAULT '',
	duration_ns           INTEGER NOT NULL DEFAULT 0,
	first_byte_duration_ns INTEGER NOT NULL DEFAULT 0,
	net_ok                INTEGER NOT NULL DEFAULT 0,
	http_method           TEXT NOT NULL DEFAULT '',
	http_status           INTEGER NOT NULL DEFAULT 0,
	resin_error           TEXT NOT NULL DEFAULT '',
	upstream_stage        TEXT NOT NULL DEFAULT '',
	upstream_err_kind     TEXT NOT NULL DEFAULT '',
	upstream_errno        TEXT NOT NULL DEFAULT '',
	upstream_err_msg      TEXT NOT NULL DEFAULT '',
	ingress_bytes         INTEGER NOT NULL DEFAULT 0,
	egress_bytes          INTEGER NOT NULL DEFAULT 0,
	payload_present       INTEGER NOT NULL DEFAULT 0,
	req_headers_len       INTEGER NOT NULL DEFAULT 0,
	req_body_len          INTEGER NOT NULL DEFAULT 0,
	resp_headers_len      INTEGER NOT NULL DEFAULT 0,
	resp_body_len         INTEGER NOT NULL DEFAULT 0,
	req_headers_truncated  INTEGER NOT NULL DEFAULT 0,
	req_body_truncated     INTEGER NOT NULL DEFAULT 0,
	resp_headers_truncated INTEGER NOT NULL DEFAULT 0,
	resp_body_truncated    INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS request_log_payloads (
	log_id        TEXT PRIMARY KEY REFERENCES request_logs(id) ON DELETE CASCADE,
	req_headers   BLOB, req_body BLOB, resp_headers BLOB, resp_body BLOB
);
CREATE INDEX IF NOT EXISTS idx_request_logs_ts_ns ON request_logs(ts_ns);
`

func seedOldRequestLogDB(t *testing.T, path, id string, tsNs int64) {
	t.Helper()
	db, err := state.OpenDB(path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	if _, err := db.Exec(oldCreateDDL); err != nil {
		t.Fatalf("seed ddl: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO request_logs (id, ts_ns, proxy_type, target_host, node_hash) VALUES (?,?,?,?,?)`,
		id, tsNs, 1, "old.example.com", "deadbeef"); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}
}

func reqLogColumns(t *testing.T, path string) []string {
	t.Helper()
	db, err := state.OpenDB(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	rows, err := db.Query("PRAGMA table_info(request_logs)")
	if err != nil {
		t.Fatalf("pragma: %v", err)
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

func hasCol(cols []string, want string) bool {
	for _, c := range cols {
		if c == want {
			return true
		}
	}
	return false
}

// 场景1：老库升级 —— 打开一个 v1.0.11 的滚动库
func TestUpgrade_OldRequestLogDB(t *testing.T) {
	dir := t.TempDir()
	seedOldRequestLogDB(t, filepath.Join(dir, "request_logs-1000.db"), "old-row-1", 1111)

	r := NewRepo(dir, 512*1024*1024, 5)
	if err := r.Open(); err != nil {
		t.Fatalf("BLOCKING: 旧 requestlog 库无法打开: %v", err)
	}
	defer r.Close()

	cols := reqLogColumns(t, filepath.Join(dir, "request_logs-1000.db"))
	t.Logf("升级后列: %v", cols)
	for _, want := range []string{"failover_attempts", "failover_nodes", "first_byte_duration_ns"} {
		if !hasCol(cols, want) {
			t.Errorf("新列 %s 未添加", want)
		}
	}

	got, err := r.GetByID("old-row-1")
	if err != nil {
		t.Fatalf("读取旧行失败: %v", err)
	}
	if got == nil {
		t.Fatalf("BLOCKING: 旧行丢失")
	}
	if got.TargetHost != "old.example.com" || got.TsNs != 1111 {
		t.Errorf("旧数据被破坏: %+v", got)
	}
	if got.FailoverAttempts != 0 || got.FailoverNodes != "" {
		t.Errorf("旧行新列应为零值: %+v", got)
	}
}

// 场景3：多代老文件 —— Open() 是否迁移所有保留文件，List 能否读到
func TestUpgrade_MultipleGenerationsMigrated(t *testing.T) {
	dir := t.TempDir()
	seedOldRequestLogDB(t, filepath.Join(dir, "request_logs-1000.db"), "gen1", 1000)
	seedOldRequestLogDB(t, filepath.Join(dir, "request_logs-2000.db"), "gen2", 2000)
	seedOldRequestLogDB(t, filepath.Join(dir, "request_logs-3000.db"), "gen3", 3000)

	r := NewRepo(dir, 512*1024*1024, 5)
	if err := r.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	for _, f := range []string{"request_logs-1000.db", "request_logs-2000.db", "request_logs-3000.db"} {
		cols := reqLogColumns(t, filepath.Join(dir, f))
		if !hasCol(cols, "failover_attempts") || !hasCol(cols, "failover_nodes") {
			t.Errorf("保留文件 %s 未被迁移: %v", f, cols)
		}
	}
	rows, _, _, err := r.List(ListFilter{Limit: 50})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("BLOCKING: 跨代查询只返回 %d 行，期望 3（旧日志丢失）", len(rows))
	}
}

// 场景3b：轮转出来的新库是否带新列
func TestUpgrade_RotatedNewFileHasNewColumns(t *testing.T) {
	dir := t.TempDir()
	seedOldRequestLogDB(t, filepath.Join(dir, "request_logs-1000.db"), "old-row", 1000)

	r := NewRepo(dir, 512*1024*1024, 5)
	if err := r.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	// 强制轮转：新建一代
	if err := r.rotateDB(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	t.Logf("轮转后 activePath=%s", r.activePath)
	cols := reqLogColumns(t, r.activePath)
	if !hasCol(cols, "failover_attempts") || !hasCol(cols, "failover_nodes") {
		t.Errorf("轮转新建的库缺少新列: %v", cols)
	}
	// 老一代文件也应已被 migrateRetainedDBs 迁移
	colsOld := reqLogColumns(t, filepath.Join(dir, "request_logs-1000.db"))
	if !hasCol(colsOld, "failover_attempts") || !hasCol(colsOld, "failover_nodes") {
		t.Errorf("旧代文件未被迁移: %v", colsOld)
	}

	if _, err := r.InsertBatch([]proxy.RequestLogEntry{{
		ID:                  "new-row",
		StartedAtNs:         2000,
		ProxyType:           1,
		FailoverAttempts:    3,
		FailoverNodes:       "aa,bb,cc",
		DurationNs:          10,
		FirstByteDurationNs: 5,
	}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	rows, _, _, err := r.List(ListFilter{Limit: 50})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("轮转后应能读到 2 行（旧+新），实际 %d", len(rows))
	}
	for _, row := range rows {
		t.Logf("  id=%s ts=%d attempts=%d nodes=%q", row.ID, row.TsNs, row.FailoverAttempts, row.FailoverNodes)
	}
}

// 场景5：写入 -> 关闭 -> 重开 -> 读回
func TestUpgrade_RequestLogWriteReopenReadBack(t *testing.T) {
	dir := t.TempDir()
	r := NewRepo(dir, 512*1024*1024, 5)
	if err := r.Open(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.InsertBatch([]proxy.RequestLogEntry{{
		ID:               "fo-1",
		StartedAtNs:      4242,
		ProxyType:        2,
		FailoverAttempts: 4,
		FailoverNodes:    "n1,n2,n3",
		ReqHeaders:       []byte("H"),
		ReqHeadersLen:    1,
	}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r2 := NewRepo(dir, 512*1024*1024, 5)
	if err := r2.Open(); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer r2.Close()
	got, err := r2.GetByID("fo-1")
	if err != nil || got == nil {
		t.Fatalf("读回失败: %v", err)
	}
	if got.FailoverAttempts != 4 || got.FailoverNodes != "n1,n2,n3" {
		t.Errorf("BLOCKING: failover 字段读回为零值/错值: attempts=%d nodes=%q", got.FailoverAttempts, got.FailoverNodes)
	}
	p, err := r2.GetPayloads("fo-1")
	if err != nil || p == nil || string(p.ReqHeaders) != "H" {
		t.Errorf("payload 读回失败: %v %+v", err, p)
	}
}

// 风险：保留库未被迁移时，List 会静默跳过（只打日志）
func TestUpgrade_UnmigratedRetainedFileIsSilentlySkipped(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "request_logs-1000.db")
	seedOldRequestLogDB(t, oldPath, "ghost", 999)

	r := NewRepo(dir, 512*1024*1024, 5)
	if err := r.Open(); err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// 再造一个“升级前的老库”，模拟它在 Open() 之后才出现 / 迁移失败的情况。
	seedOldRequestLogDB(t, filepath.Join(dir, "request_logs-4000.db"), "ghost2", 4000)

	rows, _, _, err := r.List(ListFilter{Limit: 50})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	t.Logf("List 返回 %d 行（未迁移文件可能已被跳过）", len(rows))
	for _, row := range rows {
		t.Logf("  id=%s ts=%d", row.ID, row.TsNs)
	}

	// 直接证明：只读打开未迁移的老库，查询会失败
	ro, err := r.openReadOnly(filepath.Join(dir, "request_logs-4000.db"))
	if err != nil {
		t.Fatalf("open ro: %v", err)
	}
	defer ro.Close()
	if _, err := r.queryLogs(ro, ListFilter{Limit: 10}, 10); err != nil {
		t.Logf("未迁移库查询报错（List 只会打日志并跳过该库）: %v", err)
	} else {
		t.Logf("未迁移库查询未报错")
	}
}

// 场景6：降级 —— 旧代码读写新库
func TestDowngrade_OldCodeAgainstNewRequestLogDB(t *testing.T) {
	dir := t.TempDir()
	r := NewRepo(dir, 512*1024*1024, 5)
	if err := r.Open(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.InsertBatch([]proxy.RequestLogEntry{{ID: "down-1", StartedAtNs: 555, ProxyType: 1, FailoverAttempts: 2, FailoverNodes: "x"}}); err != nil {
		t.Fatal(err)
	}
	r.Close()

	db, err := state.OpenDB(r.activePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// 旧 DDL（IF NOT EXISTS 不会动已有表）
	if _, err := db.Exec(oldCreateDDL); err != nil {
		t.Errorf("旧 DDL 执行失败: %v", err)
	}
	// 旧 INSERT（不含 failover 列）
	if _, err := db.Exec(`INSERT OR IGNORE INTO request_logs (
		id, ts_ns, proxy_type, client_ip, platform_id, platform_name, account,
		target_host, target_url, node_hash, node_tag, egress_ip,
		duration_ns, first_byte_duration_ns, net_ok, http_method, http_status,
		resin_error, upstream_stage, upstream_err_kind, upstream_errno, upstream_err_msg,
		ingress_bytes, egress_bytes, payload_present,
		req_headers_len, req_body_len, resp_headers_len, resp_body_len,
		req_headers_truncated, req_body_truncated, resp_headers_truncated, resp_body_truncated
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"old-code-row", 666, 1, "", "", "", "", "", "", "", "", "", 0, 0, 1, "GET", 200,
		"", "", "", "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0); err != nil {
		t.Errorf("旧代码写入新库失败: %v", err)
	}
	var got string
	if err := db.QueryRow(`SELECT id FROM request_logs WHERE id = ?`, "old-code-row").Scan(&got); err != nil {
		t.Errorf("旧代码读取新库失败: %v", err)
	}
	t.Logf("旧代码可读回: %s", got)
}

// 边界：request_logs 表不存在时 ensureRequestLogSchema 的行为
func TestUpgrade_EnsureRequestLogColumnOnMissingTable(t *testing.T) {
	dir := t.TempDir()
	db, err := state.OpenDB(filepath.Join(dir, "empty.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = ensureRequestLogSchema(db)
	t.Logf("空库上 ensureRequestLogSchema 返回: %v", err)
	if err == nil {
		t.Errorf("表不存在时静默成功（PRAGMA table_info 对不存在的表返回空行集）")
	}
	fmt.Fprintln(os.Stderr, "")
}
