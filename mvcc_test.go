package mvcc

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// openTestDB creates a fresh DB in a temp directory.
func openTestDB(t *testing.T) (*DB, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "mvcc-test-*")
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	return db, dir
}

func closeTestDB(t *testing.T, db *DB, dir string) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
}

// allVisible returns all key/value pairs visible to a fresh read-only txn.
func allVisible(t *testing.T, db *DB) map[string][]byte {
	t.Helper()
	txn, err := db.BeginReadOnly()
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Commit()
	m, err := txn.Scan("", "")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestBasicPutGet(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.Put("k1", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := txn.Put("k2", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	got := allVisible(t, db)
	if len(got) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(got))
	}
	if string(got["k1"]) != "v1" || string(got["k2"]) != "v2" {
		t.Fatalf("unexpected values: %v", got)
	}
}

func TestDelete(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	txn, _ := db.Begin()
	txn.Put("k1", []byte("v1"))
	txn.Commit()

	txn2, _ := db.Begin()
	v, ok, _ := txn2.Get("k1")
	if !ok || string(v) != "v1" {
		t.Fatalf("expected v1, got %q ok=%v", v, ok)
	}
	txn2.Delete("k1")
	if _, ok, _ := txn2.Get("k1"); ok {
		t.Fatal("expected k1 to be deleted in own txn")
	}
	txn2.Commit()

	got := allVisible(t, db)
	if len(got) != 0 {
		t.Fatalf("expected 0 keys after delete, got %d", len(got))
	}
}

func TestRollback(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	txn, _ := db.Begin()
	txn.Put("k1", []byte("v1"))
	txn.Put("k2", []byte("v2"))
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}

	got := allVisible(t, db)
	if len(got) != 0 {
		t.Fatalf("expected 0 keys after rollback, got %d", len(got))
	}

	// Double rollback returns error.
	if err := txn.Rollback(); err != ErrTxnDone {
		t.Fatalf("expected ErrTxnDone, got %v", err)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	// T1: start, see empty.
	t1, _ := db.Begin()
	if _, ok, _ := t1.Get("k1"); ok {
		t.Fatal("k1 should not exist yet")
	}

	// T2: write k1=v1, commit.
	t2, _ := db.Begin()
	t2.Put("k1", []byte("v1"))
	t2.Commit()

	// T1 must still see empty (its snapshot predates T2's commit).
	if v, ok, _ := t1.Get("k1"); ok {
		t.Fatalf("snapshot broken: T1 saw %q", v)
	}

	// T1 writes k1=v2. Under first-committer-wins this must conflict because
	// T2 committed a write to k1 after T1's snapshot.
	t1.Put("k1", []byte("v2"))
	if err := t1.Commit(); err != ErrConflict {
		t.Fatalf("expected ErrConflict (first-committer-wins), got %v", err)
	}

	// T3: must see v1 (T2's commit).
	t3, _ := db.Begin()
	if v, ok, _ := t3.Get("k1"); !ok || string(v) != "v1" {
		t.Fatalf("expected v1, got %q ok=%v", v, ok)
	}
	t3.Commit()
}

func TestWriteConflict(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	// T1 and T2 both write k1.
	t1, _ := db.Begin()
	t2, _ := db.Begin()

	t1.Put("k1", []byte("t1"))
	t2.Put("k1", []byte("t2"))

	if err := t1.Commit(); err != nil {
		t.Fatal(err)
	}
	// T2 must fail with conflict.
	if err := t2.Commit(); err != ErrConflict {
		t.Fatalf("expected ErrConflict, got %v", err)
	}

	// Only T1's write is visible.
	got := allVisible(t, db)
	if len(got) != 1 || string(got["k1"]) != "t1" {
		t.Fatalf("expected only t1=v1, got %v", got)
	}
}

func TestWriteConflictDisjointKeys(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	t1, _ := db.Begin()
	t2, _ := db.Begin()

	t1.Put("k1", []byte("v1"))
	t2.Put("k2", []byte("v2"))

	if err := t1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := t2.Commit(); err != nil {
		t.Fatalf("disjoint keys should not conflict: %v", err)
	}

	got := allVisible(t, db)
	if len(got) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(got))
	}
}

func TestGC(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	// Create multiple versions of k1.
	for i := 0; i < 5; i++ {
		txn, _ := db.Begin()
		txn.Put("k1", []byte(fmt.Sprintf("v%d", i)))
		txn.Commit()
	}

	stats := db.Stats()
	if stats.Versions != 5 {
		t.Fatalf("expected 5 versions before GC, got %d", stats.Versions)
	}

	// No active txns: GC should reclaim all dead versions.
	n, err := db.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("expected 4 reclaimed, got %d", n)
	}

	stats = db.Stats()
	if stats.Versions != 1 {
		t.Fatalf("expected 1 version after GC, got %d", stats.Versions)
	}

	// The latest value is still visible.
	got := allVisible(t, db)
	if string(got["k1"]) != "v4" {
		t.Fatalf("expected v4, got %q", got["k1"])
	}
}

func TestGCLongTransaction(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	// Create initial version.
	txn, _ := db.Begin()
	txn.Put("k1", []byte("v0"))
	txn.Commit()

	// Start a long-running read-only txn (pins snapshot at v0).
	longTxn, _ := db.BeginReadOnly()

	// Create more versions.
	for i := 1; i < 4; i++ {
		txn, _ := db.Begin()
		txn.Put("k1", []byte(fmt.Sprintf("v%d", i)))
		txn.Commit()
	}

	// GC must not reclaim versions visible to longTxn.
	n, err := db.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 reclaimed (long txn active), got %d", n)
	}

	// longTxn still sees v0.
	if v, ok, _ := longTxn.Get("k1"); !ok || string(v) != "v0" {
		t.Fatalf("long txn snapshot broken: expected v0, got %q", v)
	}
	longTxn.Commit()

	// After longTxn finishes, GC can reclaim.
	n, _ = db.RunGC()
	if n != 3 {
		t.Fatalf("expected 3 reclaimed after long txn, got %d", n)
	}
}

func TestReadOnlyNotBlocked(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	// Start a read-only txn.
	rotx, _ := db.BeginReadOnly()

	// Writers should be able to commit without waiting for the read-only txn.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			txn, _ := db.Begin()
			txn.Put("k", []byte(fmt.Sprintf("v%d", i)))
			txn.Commit()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("writers blocked by read-only txn")
	}

	// rotx still sees a stable snapshot (empty, since it started before any write).
	if _, ok, _ := rotx.Get("k"); ok {
		t.Fatal("read-only txn snapshot should be empty")
	}
	rotx.Commit()
}

func TestRangeScan(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	// Write keys a, b, c, d.
	for _, k := range []string{"a", "b", "c", "d"} {
		txn, _ := db.Begin()
		txn.Put(k, []byte("val-"+k))
		txn.Commit()
	}

	// Scan [b, d) -> b, c.
	txn, _ := db.BeginReadOnly()
	m, err := txn.Scan("b", "d")
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 2 {
		t.Fatalf("expected 2 keys in [b,d), got %d", len(m))
	}
	if string(m["b"]) != "val-b" || string(m["c"]) != "val-c" {
		t.Fatalf("unexpected values: %v", m)
	}
	txn.Commit()

	// Scan [a, "") -> all.
	txn2, _ := db.BeginReadOnly()
	m2, _ := txn2.Scan("a", "")
	if len(m2) != 4 {
		t.Fatalf("expected 4 keys, got %d", len(m2))
	}
	txn2.Commit()
}

func TestRangeScanConcurrent(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	// Write initial keys.
	for i := 0; i < 50; i++ {
		txn, _ := db.Begin()
		k := fmt.Sprintf("key-%03d", i)
		txn.Put(k, []byte("initial"))
		txn.Commit()
	}

	// Start a read-only txn that scans.
	rotx, _ := db.BeginReadOnly()

	// Concurrent writer updates all keys.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			txn, _ := db.Begin()
			k := fmt.Sprintf("key-%03d", i)
			txn.Put(k, []byte("updated"))
			txn.Commit()
		}
	}()

	// The scan must return a self-consistent snapshot (all "initial" or all
	// "updated", never a mix).
	m, err := rotx.Scan("", "")
	if err != nil {
		t.Fatal(err)
	}
	initial := 0
	updated := 0
	for _, v := range m {
		switch string(v) {
		case "initial":
			initial++
		case "updated":
			updated++
		}
	}
	if initial != 0 && updated != 0 {
		t.Fatalf("mixed snapshot: %d initial, %d updated", initial, updated)
	}
	rotx.Commit()
	wg.Wait()
}

func TestConcurrentTransfers(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	// Initialize accounts.
	txn, _ := db.Begin()
	for i := 0; i < 10; i++ {
		txn.Put(fmt.Sprintf("account-%d", i), []byte("100"))
	}
	txn.Commit()

	// Concurrent transfers: each txn moves 1 from a random account to another.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				txn, err := db.Begin()
				if err != nil {
					return
				}
				from := rand.Intn(10)
				to := rand.Intn(10)
				if from == to {
					txn.Rollback()
					continue
				}
				fromKey := fmt.Sprintf("account-%d", from)
				toKey := fmt.Sprintf("account-%d", to)
				fromVal, _, _ := txn.Get(fromKey)
				toVal, _, _ := txn.Get(toKey)
				fromBal, _ := strconv.Atoi(string(fromVal))
				toBal, _ := strconv.Atoi(string(toVal))
				if fromBal < 1 {
					txn.Rollback()
					continue
				}
				txn.Put(fromKey, []byte(strconv.Itoa(fromBal-1)))
				txn.Put(toKey, []byte(strconv.Itoa(toBal+1)))
				if err := txn.Commit(); err != nil {
					// Conflict: retry is handled by the outer loop.
					continue
				}
			}
		}()
	}
	wg.Wait()

	// Total balance must be conserved.
	got := allVisible(t, db)
	total := 0
	for _, v := range got {
		bal, _ := strconv.Atoi(string(v))
		total += bal
	}
	if total != 1000 {
		t.Fatalf("total balance not conserved: %d", total)
	}
}

func TestStats(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	for i := 0; i < 3; i++ {
		txn, _ := db.Begin()
		txn.Put("k1", []byte(fmt.Sprintf("v%d", i)))
		txn.Commit()
	}

	stats := db.Stats()
	if stats.Keys != 1 {
		t.Fatalf("expected 1 key, got %d", stats.Keys)
	}
	if stats.Versions != 3 {
		t.Fatalf("expected 3 versions, got %d", stats.Versions)
	}
	if stats.LiveVersions != 1 {
		t.Fatalf("expected 1 live version, got %d", stats.LiveVersions)
	}
	if stats.CommittedTxns != 3 {
		t.Fatalf("expected 3 committed txns, got %d", stats.CommittedTxns)
	}
}

// TestCrashRecovery simulates a process kill and verifies that committed
// transactions are visible and uncommitted ones are not.
func TestCrashRecovery(t *testing.T) {
	if os.Getenv("MVCC_CRASH_CHILD") == "1" {
		runCrashChild()
		return
	}

	dir, err := os.MkdirTemp("", "mvcc-crash-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecovery")
	cmd.Env = append(os.Environ(), "MVCC_CRASH_CHILD=1", "MVCC_CRASH_DIR="+dir)
	if err := cmd.Run(); err != nil {
		// The child is expected to exit with an error (os.Exit).
		if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 42 {
			t.Fatalf("unexpected child error: %v", err)
		}
	}

	// Reopen and verify.
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	got := allVisible(t, db)
	// k1=committed should be visible (committed before crash).
	if v, ok := got["k1"]; !ok || string(v) != "committed" {
		t.Fatalf("expected k1=committed, got %q ok=%v", v, ok)
	}
	// k2 should not exist (uncommitted).
	if _, ok := got["k2"]; ok {
		t.Fatal("k2 should not be visible (uncommitted)")
	}
	// k3=committed should be visible.
	if v, ok := got["k3"]; !ok || string(v) != "committed" {
		t.Fatalf("expected k3=committed, got %q", v)
	}
}

func runCrashChild() {
	dir := os.Getenv("MVCC_CRASH_DIR")
	db, err := Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Committed write.
	txn, _ := db.Begin()
	txn.Put("k1", []byte("committed"))
	txn.Commit()

	// Uncommitted write (open transaction).
	txn2, _ := db.Begin()
	txn2.Put("k2", []byte("uncommitted"))
	// Intentionally do not commit or rollback.

	// Another committed write.
	txn3, _ := db.Begin()
	txn3.Put("k3", []byte("committed"))
	txn3.Commit()

	// Kill the process abruptly.
	os.Exit(42)
}

// TestCrashRecoveryDuringCommit simulates a kill right after a commit to
// verify the WAL redo path.
func TestCrashRecoveryDuringCommit(t *testing.T) {
	if os.Getenv("MVCC_CRASH_CHILD") == "1" {
		runCrashCommitChild()
		return
	}

	dir, err := os.MkdirTemp("", "mvcc-crash-commit-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecoveryDuringCommit")
	cmd.Env = append(os.Environ(), "MVCC_CRASH_CHILD=1", "MVCC_CRASH_DIR="+dir)
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 42 {
			t.Fatalf("unexpected child error: %v", err)
		}
	}

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	got := allVisible(t, db)
	if v, ok := got["k1"]; !ok || string(v) != "durable" {
		t.Fatalf("expected k1=durable after WAL redo, got %q ok=%v", v, ok)
	}
}

func runCrashCommitChild() {
	dir := os.Getenv("MVCC_CRASH_DIR")
	db, err := Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	txn, _ := db.Begin()
	txn.Put("k1", []byte("durable"))
	if err := txn.Commit(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// Kill immediately after commit (before Close/checkpoint).
	os.Exit(42)
}

// TestSnapshotIsolationWithDeletes verifies that a transaction's snapshot is
// stable even when another transaction deletes a key after the snapshot.
func TestSnapshotIsolationWithDeletes(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	txn, _ := db.Begin()
	txn.Put("k1", []byte("v1"))
	txn.Commit()

	// T1 starts and sees k1=v1.
	t1, _ := db.Begin()
	if v, ok, _ := t1.Get("k1"); !ok || string(v) != "v1" {
		t.Fatalf("expected v1, got %q ok=%v", v, ok)
	}

	// T2 deletes k1 and commits.
	t2, _ := db.Begin()
	t2.Delete("k1")
	t2.Commit()

	// T1 still sees k1=v1 (snapshot is stable).
	if v, ok, _ := t1.Get("k1"); !ok || string(v) != "v1" {
		t.Fatalf("snapshot broken: expected v1, got %q ok=%v", v, ok)
	}
	t1.Commit()

	// T3 sees k1 as deleted.
	t3, _ := db.Begin()
	if _, ok, _ := t3.Get("k1"); ok {
		t.Fatal("k1 should be deleted")
	}
	t3.Commit()
}

// TestMultipleReadOnlyTxns verifies that multiple read-only transactions
// each see their own snapshot.
func TestMultipleReadOnlyTxns(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	rotx1, _ := db.BeginReadOnly()

	txn, _ := db.Begin()
	txn.Put("k1", []byte("v1"))
	txn.Commit()

	rotx2, _ := db.BeginReadOnly()

	// rotx1 sees empty, rotx2 sees v1.
	if _, ok, _ := rotx1.Get("k1"); ok {
		t.Fatal("rotx1 should see empty snapshot")
	}
	if v, ok, _ := rotx2.Get("k1"); !ok || string(v) != "v1" {
		t.Fatalf("rotx2 should see v1, got %q ok=%v", v, ok)
	}

	// Another write.
	txn2, _ := db.Begin()
	txn2.Put("k1", []byte("v2"))
	txn2.Commit()

	// rotx1 and rotx2 still see their original snapshots.
	if _, ok, _ := rotx1.Get("k1"); ok {
		t.Fatal("rotx1 snapshot changed")
	}
	if v, ok, _ := rotx2.Get("k1"); !ok || string(v) != "v1" {
		t.Fatalf("rotx2 snapshot changed: got %q", v)
	}

	rotx1.Commit()
	rotx2.Commit()
}

// TestReadYourWrites verifies that a transaction sees its own pending writes.
func TestReadYourWrites(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	txn, _ := db.Begin()
	txn.Put("k1", []byte("v1"))
	if v, ok, _ := txn.Get("k1"); !ok || string(v) != "v1" {
		t.Fatalf("read-your-writes broken: expected v1, got %q ok=%v", v, ok)
	}

	// Overwrite.
	txn.Put("k1", []byte("v2"))
	if v, ok, _ := txn.Get("k1"); !ok || string(v) != "v2" {
		t.Fatalf("read-your-writes broken: expected v2, got %q ok=%v", v, ok)
	}

	// Delete.
	txn.Delete("k1")
	if _, ok, _ := txn.Get("k1"); ok {
		t.Fatal("read-your-writes broken: k1 should be deleted")
	}
	txn.Commit()
}

// TestGCWithConcurrentTxns verifies that GC correctly handles a mix of
// active transactions with different snapshots.
func TestGCWithConcurrentTxns(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	// Create initial version.
	txn, _ := db.Begin()
	txn.Put("k1", []byte("v0"))
	txn.Commit()

	// T1 starts (pins v0).
	t1, _ := db.BeginReadOnly()

	// Create v1.
	txn2, _ := db.Begin()
	txn2.Put("k1", []byte("v1"))
	txn2.Commit()

	// T2 starts (pins v1).
	t2, _ := db.BeginReadOnly()

	// Create v2.
	txn3, _ := db.Begin()
	txn3.Put("k1", []byte("v2"))
	txn3.Commit()

	// GC: threshold = min(t1.startTS, t2.startTS) = t1.startTS.
	// v0 has EndTS = commitTS of v1 > t1.startTS, so it's preserved.
	// v1 has EndTS = commitTS of v2 > t1.startTS, so it's preserved.
	n, err := db.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 reclaimed (both txns active), got %d", n)
	}

	// t1 sees v0, t2 sees v1.
	if v, ok, _ := t1.Get("k1"); !ok || string(v) != "v0" {
		t.Fatalf("t1 snapshot broken: expected v0, got %q", v)
	}
	if v, ok, _ := t2.Get("k1"); !ok || string(v) != "v1" {
		t.Fatalf("t2 snapshot broken: expected v1, got %q", v)
	}

	t1.Commit()

	// Now only t2 is active (pins v1). GC can reclaim v0 (EndTS <= t2.startTS).
	n, _ = db.RunGC()
	if n != 1 {
		t.Fatalf("expected 1 reclaimed (v0), got %d", n)
	}

	t2.Commit()

	// No active txns: GC can reclaim v1.
	n, _ = db.RunGC()
	if n != 1 {
		t.Fatalf("expected 1 reclaimed (v1), got %d", n)
	}

	// v2 is still visible.
	got := allVisible(t, db)
	if string(got["k1"]) != "v2" {
		t.Fatalf("expected v2, got %q", got["k1"])
	}
}

// TestDeterministicReplay verifies that replaying the same operation sequence
// produces identical visible data and GC results.
func TestDeterministicReplay(t *testing.T) {
	seed := int64(12345)
	ops := generateOps(seed, 100)

	run := func() (map[string][]byte, int) {
		dir, err := os.MkdirTemp("", "mvcc-det-*")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		db, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		executeOps(t, db, ops)
		gcCount, _ := db.RunGC()
		return allVisible(t, db), gcCount
	}

	r1, gc1 := run()
	r2, gc2 := run()

	if gc1 != gc2 {
		t.Fatalf("GC count mismatch: %d vs %d", gc1, gc2)
	}
	if len(r1) != len(r2) {
		t.Fatalf("key count mismatch: %d vs %d", len(r1), len(r2))
	}
	for k, v := range r1 {
		if string(r2[k]) != string(v) {
			t.Fatalf("value mismatch for %q: %q vs %q", k, v, r2[k])
		}
	}
}

type op struct {
	kind string // "put", "delete", "gc"
	key  string
	val  string
}

func generateOps(seed int64, n int) []op {
	rng := rand.New(rand.NewSource(seed))
	ops := make([]op, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%d", rng.Intn(20))
		switch rng.Intn(3) {
		case 0:
			ops = append(ops, op{kind: "put", key: k, val: fmt.Sprintf("val-%d", rng.Intn(100))})
		case 1:
			ops = append(ops, op{kind: "delete", key: k})
		case 2:
			ops = append(ops, op{kind: "gc"})
		}
	}
	return ops
}

func executeOps(t *testing.T, db *DB, ops []op) {
	t.Helper()
	for _, o := range ops {
		switch o.kind {
		case "put":
			txn, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			txn.Put(o.key, []byte(o.val))
			if err := txn.Commit(); err != nil {
				if err != ErrConflict {
					t.Fatal(err)
				}
			}
		case "delete":
			txn, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			txn.Delete(o.key)
			if err := txn.Commit(); err != nil {
				if err != ErrConflict {
					t.Fatal(err)
				}
			}
		case "gc":
			if _, err := db.RunGC(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestReopenPreservesData verifies that data survives Close + reopen.
func TestReopenPreservesData(t *testing.T) {
	dir, err := os.MkdirTemp("", "mvcc-reopen-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	txn, _ := db.Begin()
	txn.Put("k1", []byte("v1"))
	txn.Put("k2", []byte("v2"))
	txn.Commit()
	db.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := allVisible(t, db2)
	if len(got) != 2 || string(got["k1"]) != "v1" || string(got["k2"]) != "v2" {
		t.Fatalf("data not preserved: %v", got)
	}
	db2.Close()
}

// TestVersionChainOrdering verifies that versions are ordered by BeginTS
// descending and that the visible version is correct for each snapshot.
func TestVersionChainOrdering(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	for i := 0; i < 5; i++ {
		txn, _ := db.Begin()
		txn.Put("k1", []byte(fmt.Sprintf("v%d", i)))
		txn.Commit()
	}

	// Verify chain is ordered by BeginTS descending.
	cur := db.current.Load()
	chain := cur.get("k1")
	if chain == nil {
		t.Fatal("chain should exist")
	}
	for i := 1; i < len(chain.versions); i++ {
		if chain.versions[i-1].BeginTS <= chain.versions[i].BeginTS {
			t.Fatalf("versions not ordered by BeginTS descending: %d <= %d",
				chain.versions[i-1].BeginTS, chain.versions[i].BeginTS)
		}
	}

	// The latest version is visible to a new txn.
	txn, _ := db.BeginReadOnly()
	v := chain.visibleAt(txn.startTS)
	if v == nil || string(v.Value) != "v4" {
		t.Fatalf("expected v4, got %v", v)
	}
	txn.Commit()

	// Only the latest version is live.
	liveCount := 0
	for _, v := range chain.versions {
		if v.isLive() {
			liveCount++
		}
	}
	if liveCount != 1 {
		t.Fatalf("expected 1 live version, got %d", liveCount)
	}
}

// TestEmptyDBOperations verifies operations on an empty DB.
func TestEmptyDBOperations(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	txn, _ := db.Begin()
	if _, ok, _ := txn.Get("nonexistent"); ok {
		t.Fatal("nonexistent key should not exist")
	}
	m, _ := txn.Scan("", "")
	if len(m) != 0 {
		t.Fatal("empty scan should return empty map")
	}
	txn.Commit()

	stats := db.Stats()
	if stats.Keys != 0 || stats.Versions != 0 {
		t.Fatalf("expected empty stats, got keys=%d versions=%d", stats.Keys, stats.Versions)
	}
}

// TestReadOnlyTxnCannotWrite verifies that read-only txns reject writes.
func TestReadOnlyTxnCannotWrite(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	txn, _ := db.BeginReadOnly()
	if err := txn.Put("k1", []byte("v1")); err != ErrReadOnly {
		t.Fatalf("expected ErrReadOnly, got %v", err)
	}
	if err := txn.Delete("k1"); err != ErrReadOnly {
		t.Fatalf("expected ErrReadOnly, got %v", err)
	}
	txn.Commit()
}

// TestScanEmptyRange verifies scan with empty range returns nothing.
func TestScanEmptyRange(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	txn, _ := db.Begin()
	txn.Put("a", []byte("1"))
	txn.Put("z", []byte("2"))
	txn.Commit()

	txn2, _ := db.BeginReadOnly()
	m, _ := txn2.Scan("m", "n")
	if len(m) != 0 {
		t.Fatalf("expected empty range, got %d keys", len(m))
	}
	txn2.Commit()
}

// TestKeysInOrder verifies sortedKeys returns sorted keys.
func TestKeysInOrder(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	txn, _ := db.Begin()
	for _, k := range []string{"zebra", "apple", "mango", "banana"} {
		txn.Put(k, []byte("v"))
	}
	txn.Commit()

	cur := db.current.Load()
	keys := cur.sortedKeys()
	if !sort.StringsAreSorted(keys) {
		t.Fatalf("keys not sorted: %v", keys)
	}
}

// TestCrashRecoverySnapshotNoTruncate verifies that recovery does not
// duplicate versions when a snapshot was written but the WAL was not yet
// truncated before the kill.
func TestCrashRecoverySnapshotNoTruncate(t *testing.T) {
	if os.Getenv("MVCC_CRASH_CHILD") == "1" {
		runCrashSnapshotChild()
		return
	}

	dir, err := os.MkdirTemp("", "mvcc-crash-snap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecoverySnapshotNoTruncate")
	cmd.Env = append(os.Environ(), "MVCC_CRASH_CHILD=1", "MVCC_CRASH_DIR="+dir)
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 42 {
			t.Fatalf("unexpected child error: %v", err)
		}
	}

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	got := allVisible(t, db)
	if len(got) != 2 {
		t.Fatalf("expected 2 keys (no duplication), got %d: %v", len(got), got)
	}
	if string(got["k1"]) != "v1" || string(got["k2"]) != "v2" {
		t.Fatalf("unexpected values: %v", got)
	}

	// Verify no duplicate versions.
	stats := db.Stats()
	if stats.Versions != 2 {
		t.Fatalf("expected 2 versions (no duplication), got %d", stats.Versions)
	}
}

func runCrashSnapshotChild() {
	dir := os.Getenv("MVCC_CRASH_DIR")
	db, err := Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	txn, _ := db.Begin()
	txn.Put("k1", []byte("v1"))
	txn.Commit()
	txn2, _ := db.Begin()
	txn2.Put("k2", []byte("v2"))
	txn2.Commit()
	// Write snapshot but do NOT truncate the WAL.
	db.mu.Lock()
	err = db.writeSnapshotOnly()
	db.mu.Unlock()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(42)
}

// TestRollbackThenRewrite verifies that a rolled-back transaction's writes do
// not conflict with a subsequent transaction.
func TestRollbackThenRewrite(t *testing.T) {
	db, dir := openTestDB(t)
	defer closeTestDB(t, db, dir)

	// T1 writes k1 but rolls back.
	t1, _ := db.Begin()
	t1.Put("k1", []byte("t1"))
	t1.Rollback()

	// T2 writes k1 and commits (no conflict since T1 rolled back).
	t2, _ := db.Begin()
	t2.Put("k1", []byte("t2"))
	if err := t2.Commit(); err != nil {
		t.Fatalf("rollback should not cause conflict: %v", err)
	}

	got := allVisible(t, db)
	if len(got) != 1 || string(got["k1"]) != "t2" {
		t.Fatalf("expected k1=t2, got %v", got)
	}
}

// TestMain is used to run crash child processes.
func TestMain(m *testing.M) {
	// Ensure the test binary can be re-execed for crash tests.
	os.Exit(m.Run())
}
