package state

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

// Recheck of the check-then-act race, at the granularity that actually bites
// production: separate *processes* opening the same pre-existing database file,
// as happens when an old and a new binary briefly coexist during a rolling
// restart.

const recheckChildEnv = "RESIN_RECHECK_ADD_CHILD"

func TestRecheck_EnsureTableColumnAcrossProcesses(t *testing.T) {
	if os.Getenv(recheckChildEnv) == "1" {
		runRecheckAddChild()
		return
	}

	path := filepath.Join(t.TempDir(), "shared.db")
	db, err := OpenDB(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	const children = 6
	var wg sync.WaitGroup
	failures := make(chan string, children)
	for i := 0; i < children; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=TestRecheck_EnsureTableColumnAcrossProcesses")
			cmd.Env = append(os.Environ(), recheckChildEnv+"=1", "RESIN_RECHECK_DB="+path)
			out, err := cmd.CombinedOutput()
			if err != nil {
				failures <- fmt.Sprintf("%v: %s", err, out)
			}
		}()
	}
	wg.Wait()
	close(failures)

	for f := range failures {
		t.Fatalf("a concurrent process failed to open the shared db: %s", f)
	}

	// The column must be present and usable exactly once.
	verify, err := OpenDB(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer verify.Close()
	var count int64
	if err := verify.QueryRow(`SELECT COUNT(b) FROM t`).Scan(&count); err != nil {
		t.Fatalf("column not usable after concurrent add: %v", err)
	}
}

// runRecheckAddChild never returns: it is the body of the child process.
func runRecheckAddChild() {
	path := os.Getenv("RESIN_RECHECK_DB")
	db, err := OpenDB(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Spin on the lock the way two real processes would: SQLite serializes
	// writers, so losers retry inside busy_timeout rather than erroring.
	if err := ensureRecheckColumns(db); err != nil {
		fmt.Fprintf(os.Stderr, "child migrate: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func ensureRecheckColumns(db *sql.DB) error {
	if err := EnsureTableColumn(db, "t", "b", "b INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	return EnsureTableColumn(db, "t", "c", "c TEXT NOT NULL DEFAULT ''")
}
