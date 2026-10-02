package engine

import (
	"errors"
	"path/filepath"
	"testing"
)

func openTestEngine(t *testing.T, syncWrites bool) *Engine {
	t.Helper()
	e, err := Open(Options{Directory: t.TempDir(), SyncWrites: syncWrites})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestSnapshotIsolationAndDelete(t *testing.T) {
	e := openTestEngine(t, false)

	t1, err := e.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := t1.Put("a", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := t1.Commit(); err != nil {
		t.Fatal(err)
	}

	reader := mustBegin(t, e, true)
	writer := mustBegin(t, e, false)
	if err := writer.Put("a", []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	value, ok, err := reader.Get("a")
	if err != nil || !ok || string(value) != "one" {
		t.Fatalf("reader got (%q,%v,%v), want one", value, ok, err)
	}
	if err := reader.Commit(); err != nil {
		t.Fatal(err)
	}

	deleter := mustBegin(t, e, false)
	if err := deleter.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := deleter.Commit(); err != nil {
		t.Fatal(err)
	}
	newReader := mustBegin(t, e, true)
	if _, ok, err := newReader.Get("a"); err != nil || ok {
		t.Fatalf("deleted key got exists=%v err=%v", ok, err)
	}
}

func TestWriteWriteConflict(t *testing.T) {
	e := openTestEngine(t, false)
	initial := mustBegin(t, e, false)
	if err := initial.Put("k", []byte("v0")); err != nil {
		t.Fatal(err)
	}
	if err := initial.Commit(); err != nil {
		t.Fatal(err)
	}

	a := mustBegin(t, e, false)
	b := mustBegin(t, e, false)
	if err := a.Put("k", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put("k", []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := a.Commit(); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	err := b.Commit()
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("second commit err=%v, want ErrConflict", err)
	}
	if err := b.Rollback(); err != nil {
		t.Fatalf("rollback after conflict should be idempotent: %v", err)
	}

	retry := mustBegin(t, e, false)
	value, ok, err := retry.Get("k")
	if err != nil || !ok || string(value) != "a" {
		t.Fatalf("retry snapshot=%q,%v,%v", value, ok, err)
	}
	if err := retry.Put("k", []byte("retried")); err != nil {
		t.Fatal(err)
	}
	if err := retry.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackHasNoPartialVisibility(t *testing.T) {
	e := openTestEngine(t, false)
	base := mustBegin(t, e, false)
	_ = base.Put("a", []byte("a1"))
	_ = base.Put("b", []byte("b1"))
	if err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	txn := mustBegin(t, e, false)
	_ = txn.Put("a", []byte("bad"))
	_ = txn.Delete("b")
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}

	checker := mustBegin(t, e, true)
	if v, ok, _ := checker.Get("a"); !ok || string(v) != "a1" {
		t.Fatalf("a after rollback=%q,%v", v, ok)
	}
	if v, ok, _ := checker.Get("b"); !ok || string(v) != "b1" {
		t.Fatalf("b after rollback=%q,%v", v, ok)
	}
}

func TestRangeScanUsesOneSnapshot(t *testing.T) {
	e := openTestEngine(t, false)
	txn := mustBegin(t, e, false)
	_ = txn.Put("k1", []byte("1"))
	_ = txn.Put("k2", []byte("2"))
	_ = txn.Put("j1", []byte("0"))
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	reader := mustBegin(t, e, true)
	concurrent := mustBegin(t, e, false)
	_ = concurrent.Put("k1", []byte("new"))
	_ = concurrent.Put("k3", []byte("3"))
	if err := concurrent.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := reader.Scan(Range{Start: "k", End: "l"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Key != "k1" || string(got[0].Value) != "1" ||
		got[1].Key != "k2" || string(got[1].Value) != "2" {
		t.Fatalf("scan=%+v", got)
	}
}

func TestRecoveryCommittedVisibleUncommittedHidden(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Directory: dir, SyncWrites: false})
	if err != nil {
		t.Fatal(err)
	}
	committed := mustBegin(t, e, false)
	_ = committed.Put("committed", []byte("yes"))
	if err := committed.Commit(); err != nil {
		t.Fatal(err)
	}
	uncommitted := mustBegin(t, e, false)
	_ = uncommitted.Put("uncommitted", []byte("no"))
	// Simulate process death before commit: close discards only in-memory state.
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(Options{Directory: dir, SyncWrites: false})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	txn := mustBegin(t, e2, true)
	if v, ok, _ := txn.Get("committed"); !ok || string(v) != "yes" {
		t.Fatalf("committed recovery=%q,%v", v, ok)
	}
	if _, ok, _ := txn.Get("uncommitted"); ok {
		t.Fatal("uncommitted mutation is visible after recovery")
	}
}

func mustBegin(t *testing.T, e *Engine, readOnly bool) *Txn {
	t.Helper()
	txn, err := e.Begin(readOnly)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	return txn
}

func TestGCPreservesLongReaderSnapshot(t *testing.T) {
	e := openTestEngine(t, false)
	write := func(value string) {
		t.Helper()
		txn := mustBegin(t, e, false)
		if err := txn.Put("k", []byte(value)); err != nil {
			t.Fatal(err)
		}
		if err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	write("old")
	longReader := mustBegin(t, e, true)
	write("middle")
	write("new")

	stats, err := e.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	if stats.RemovedVersions != 0 {
		t.Fatalf("GC removed %d versions while long reader active", stats.RemovedVersions)
	}
	if value, ok, _ := longReader.Get("k"); !ok || string(value) != "old" {
		t.Fatalf("long reader=%q,%v", value, ok)
	}
	if err := longReader.Commit(); err != nil {
		t.Fatal(err)
	}

	stats, err = e.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	if stats.RemovedVersions != 2 {
		t.Fatalf("removed=%d, want 2", stats.RemovedVersions)
	}
	current := mustBegin(t, e, true)
	if value, ok, _ := current.Get("k"); !ok || string(value) != "new" {
		t.Fatalf("current after GC=%q,%v", value, ok)
	}
}

func TestGCRemovesExpiredTombstoneAndPersistsSnapshot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Directory: dir, SyncWrites: false})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"v1", "v2", "v3"} {
		txn := mustBegin(t, e, false)
		_ = txn.Put("k", []byte(value))
		if err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	deleter := mustBegin(t, e, false)
	_ = deleter.Delete("k")
	if err := deleter.Commit(); err != nil {
		t.Fatal(err)
	}
	stats, err := e.RunGC()
	if err != nil {
		t.Fatal(err)
	}
	if stats.RemovedVersions != 4 || stats.ChainsAfter != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(Options{Directory: dir, SyncWrites: false})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if e2.Stats().TotalVersions != 0 {
		t.Fatalf("versions after recovery=%d", e2.Stats().TotalVersions)
	}
}
