package state

import (
	"path/filepath"
	"sync"
	"testing"
)

// Adding a column is check-then-act: PRAGMA table_info, then ALTER TABLE.
// Another opener can slip between the two, so a bare "duplicate column name"
// error must not be fatal — a rolling restart where the old and new process
// briefly share the file would otherwise fail to start.
func TestEnsureTableColumn_ConcurrentAddDoesNotFail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.db")
	db, err := OpenDB(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	const concurrency = 8
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine uses its own handle, as separate openers would.
			handle, oerr := OpenDB(path)
			if oerr != nil {
				errs <- oerr
				return
			}
			defer handle.Close()
			errs <- EnsureTableColumn(handle, "t", "b", "b INTEGER NOT NULL DEFAULT 0")
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent add column must succeed, got: %v", err)
		}
	}

	// The column must exist exactly once and be usable.
	var sum int64
	if err := db.QueryRow(`SELECT COUNT(b) FROM t`).Scan(&sum); err != nil {
		t.Fatalf("column not usable after concurrent add: %v", err)
	}
}
