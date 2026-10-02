package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplayTruncatesTornFinalFrame(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Directory: dir, SyncWrites: false})
	if err != nil {
		t.Fatal(err)
	}
	txn := mustBegin(t, e, false)
	_ = txn.Put("a", []byte("committed"))
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(dir, walName)
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	originalSize := info.Size()
	file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1, 2, 3, 4, 5, 6, 7}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(Options{Directory: dir, SyncWrites: false})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	checker := mustBegin(t, e2, true)
	if v, ok, _ := checker.Get("a"); !ok || string(v) != "committed" {
		t.Fatalf("replay value=%q,%v", v, ok)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != originalSize {
		t.Fatalf("torn tail not truncated: %d != %d", info.Size(), originalSize)
	}
}

func TestReplayAfterCompactionRemovesTemporaryLogs(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Directory: dir, SyncWrites: false})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		txn := mustBegin(t, e, false)
		_ = txn.Put("k", []byte{byte('a' + i)})
		if err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.RunGC(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, walTempPrefix+"crashed"), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(Options{Directory: dir, SyncWrites: false})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	txn := mustBegin(t, e2, true)
	if v, ok, _ := txn.Get("k"); !ok || string(v) != "c" {
		t.Fatalf("compacted value=%q,%v", v, ok)
	}
	if err := e2.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), walTempPrefix) {
			t.Fatalf("stale temp file survived: %s", entry.Name())
		}
	}
}
