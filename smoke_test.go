package mvcc

import (
	"fmt"
	"os"
	"testing"
)

func TestSmoke(t *testing.T) {
	dir, err := os.MkdirTemp("", "mvcc-smoke-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Basic put/get
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.Put("key1", []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Read in a new txn
	txn2, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	v, ok, err := txn2.Get("key1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(v) != "value1" {
		t.Fatalf("expected value1, got %q ok=%v", v, ok)
	}
	if err := txn2.Commit(); err != nil {
		t.Fatal(err)
	}

	// Snapshot isolation: txn3 sees snapshot before txn4 commits
	txn3, _ := db.Begin()
	v3, _, _ := txn3.Get("key1")
	fmt.Printf("txn3 sees: %q\n", v3)

	txn4, _ := db.Begin()
	txn4.Put("key1", []byte("value2"))
	if err := txn4.Commit(); err != nil {
		t.Fatal(err)
	}

	v3b, ok, _ := txn3.Get("key1")
	if !ok || string(v3b) != "value1" {
		t.Fatalf("snapshot broken: expected value1, got %q", v3b)
	}
	txn3.Commit()

	// Verify txn4's write is visible to new txns
	txn5, _ := db.Begin()
	v5, _, _ := txn5.Get("key1")
	if string(v5) != "value2" {
		t.Fatalf("expected value2, got %q", v5)
	}
	txn5.Commit()

	db.Close()

	// Reopen and verify persistence
	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	txn6, _ := db2.Begin()
	v6, _, _ := txn6.Get("key1")
	if string(v6) != "value2" {
		t.Fatalf("after reopen: expected value2, got %q", v6)
	}
	txn6.Commit()
	db2.Close()

	fmt.Println("smoke test passed")
}
