package engine

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

type replayResult struct {
	pairs []KVPair
	stats Stats
	log   []byte
}

func TestSameSeedReplayProducesIdenticalDataAndGC(t *testing.T) {
	first := runSeededReplay(t, 740740)
	second := runSeededReplay(t, 740740)

	if len(first.pairs) != len(second.pairs) {
		t.Fatalf("pair count %d != %d", len(first.pairs), len(second.pairs))
	}
	for i := range first.pairs {
		if first.pairs[i].Key != second.pairs[i].Key ||
			string(first.pairs[i].Value) != string(second.pairs[i].Value) {
			t.Fatalf("pair %d differs: %+v != %+v", i, first.pairs[i], second.pairs[i])
		}
	}
	if first.stats.TotalVersions != second.stats.TotalVersions ||
		first.stats.RemovedVersions != second.stats.RemovedVersions ||
		first.stats.GCRuns != second.stats.GCRuns {
		t.Fatalf("stats differ: %+v != %+v", first.stats, second.stats)
	}
	if string(first.log) != string(second.log) {
		t.Fatalf("compacted WAL bytes differ")
	}
}

func runSeededReplay(t *testing.T, seed int64) replayResult {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(Options{Directory: dir, SyncWrites: false})
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(seed))
	var txn *Txn
	keyCount := 12
	for step := 0; step < 240; step++ {
		if txn == nil {
			txn = mustBegin(t, e, false)
		}
		key := fmt.Sprintf("key-%02d", rng.Intn(keyCount))
		switch rng.Intn(10) {
		case 0:
			if err := txn.Delete(key); err != nil {
				t.Fatal(err)
			}
		case 1:
			if err := txn.Rollback(); err != nil {
				t.Fatal(err)
			}
			txn = nil
		case 2:
			if err := txn.Commit(); err != nil {
				t.Fatal(err)
			}
			txn = nil
		default:
			value := fmt.Sprintf("step-%03d-key-%s", step, key)
			if err := txn.Put(key, []byte(value)); err != nil {
				t.Fatal(err)
			}
		}

		if step%31 == 0 {
			if _, err := e.RunGC(); err != nil {
				t.Fatal(err)
			}
		}
		if step%47 == 0 {
			if txn != nil {
				// Simulate process exit while a transaction is active. Its
				// in-memory handle is gone and replay must hide its writes.
				txn = nil
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			e, err = Open(Options{Directory: dir, SyncWrites: false})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if txn != nil {
		if err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := e.RunGC(); err != nil {
			t.Fatal(err)
		}
	}

	reader := mustBegin(t, e, true)
	pairs, err := reader.Scan(Range{})
	if err != nil {
		t.Fatal(err)
	}
	stats := e.Stats()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(filepath.Join(dir, walName))
	if err != nil {
		t.Fatal(err)
	}
	return replayResult{pairs: pairs, stats: stats, log: log}
}
