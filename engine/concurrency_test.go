package engine

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentWritersAndLongReadOnlySnapshot(t *testing.T) {
	e := openTestEngine(t, true)
	initial := mustBegin(t, e, false)
	for i := 0; i < 20; i++ {
		_ = initial.Put(fmt.Sprintf("key%02d", i), []byte("initial"))
	}
	if err := initial.Commit(); err != nil {
		t.Fatal(err)
	}

	longReader := mustBegin(t, e, true)
	stopGC := make(chan struct{})
	var gcWG sync.WaitGroup
	gcWG.Add(1)
	go func() {
		defer gcWG.Done()
		for {
			select {
			case <-stopGC:
				_, _ = e.RunGC()
				return
			default:
				_, _ = e.RunGC()
			}
		}
	}()

	const writers = 16
	const iterations = 20
	var wg sync.WaitGroup
	for worker := 0; worker < writers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				txn, err := e.Begin(false)
				if err != nil {
					t.Errorf("begin: %v", err)
					return
				}
				key := fmt.Sprintf("key%02d", (worker+iteration)%20)
				if err := txn.Put(key, []byte(fmt.Sprintf("w%d-i%d", worker, iteration))); err != nil {
					t.Errorf("put: %v", err)
					return
				}
				if err := txn.Commit(); err != nil && !errors.Is(err, ErrConflict) {
					t.Errorf("commit: %v", err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(stopGC)
	gcWG.Wait()

	got, err := longReader.Scan(Range{Start: "key00", End: "key20"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 20 {
		t.Fatalf("long snapshot has %d keys, want 20", len(got))
	}
	for _, pair := range got {
		if string(pair.Value) != "initial" {
			t.Fatalf("key %s changed in old snapshot: %q", pair.Key, pair.Value)
		}
	}
}

func TestConcurrentOverlappingCommitsExactlyOneWinner(t *testing.T) {
	e := openTestEngine(t, true)
	base := mustBegin(t, e, false)
	_ = base.Put("shared", []byte("base"))
	if err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	const count = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	commits := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			txn := mustBegin(t, e, false)
			if err := txn.Put("shared", []byte(fmt.Sprintf("v%d", i))); err != nil {
				t.Errorf("put: %v", err)
				return
			}
			<-start
			err := txn.Commit()
			commits <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(commits)

	successes := 0
	conflicts := 0
	for err := range commits {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected commit error: %v", err)
		}
	}
	if successes != 1 || conflicts != count-1 {
		t.Fatalf("success=%d conflicts=%d", successes, conflicts)
	}
	if e.Stats().TotalVersions != 2 {
		t.Fatalf("failed transactions left versions: %d", e.Stats().TotalVersions)
	}
}

func TestConcurrentCommitsRollbackAndScans(t *testing.T) {
	e := openTestEngine(t, true)
	base := mustBegin(t, e, false)
	for i := 0; i < 10; i++ {
		_ = base.Put(fmt.Sprintf("r%02d", i), []byte("base"))
	}
	if err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 15; i++ {
				txn, err := e.Begin(false)
				if err != nil {
					t.Errorf("begin: %v", err)
					return
				}
				for k := 0; k < 3; k++ {
					key := fmt.Sprintf("r%02d", (worker+i+k)%10)
					if err := txn.Put(key, []byte(fmt.Sprintf("w%d-%d", worker, i))); err != nil {
						t.Errorf("put: %v", err)
						return
					}
				}
				err = txn.Commit()
				if errors.Is(err, ErrConflict) {
					if err := txn.Rollback(); err != nil && !errors.Is(err, ErrTxnClosed) {
						t.Errorf("rollback: %v", err)
					}
				} else if err != nil {
					t.Errorf("commit: %v", err)
				}
			}
		}(worker)
	}
	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				txn := mustBegin(t, e, true)
				pairs, err := txn.Scan(Range{Start: "r00", End: "r10"})
				if err != nil {
					t.Errorf("scan: %v", err)
					return
				}
				if len(pairs) != 10 {
					t.Errorf("snapshot mixed keys: %d", len(pairs))
					return
				}
				if err := txn.Commit(); err != nil {
					t.Errorf("reader finish: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
