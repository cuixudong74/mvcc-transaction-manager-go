package mvcc

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// Stats exposes engine counters for observability and tests.
type Stats struct {
	Keys          int
	Versions      int
	LiveVersions  int
	ActiveTxns    int
	NextTS        uint64
	NextTxnID     uint64
	Reclaimed     uint64 // total versions reclaimed by GC
	CommittedTxns uint64
	AbortedTxns   uint64
}

// chainsMap is an immutable snapshot of all version chains. Every commit and
// GC creates a new chainsMap and atomically installs it, so read-only
// transactions can read a captured snapshot without taking any lock.
type chainsMap struct {
	chains map[string]*versionChain
}

func (m *chainsMap) get(key string) *versionChain {
	return m.chains[key]
}

func (m *chainsMap) sortedKeys() []string {
	keys := make([]string, 0, len(m.chains))
	for k := range m.chains {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// clone returns a shallow copy of the map. The versionChain values are
// immutable and therefore safe to share.
func (m *chainsMap) clone() *chainsMap {
	chains := make(map[string]*versionChain, len(m.chains))
	for k, v := range m.chains {
		chains[k] = v
	}
	return &chainsMap{chains: chains}
}

// DB is an embedded, multi-version concurrency controlled key-value store.
//
// All state lives in memory; the WAL and snapshot files on disk provide
// durability and crash recovery. The engine is safe for concurrent use.
type DB struct {
	mu        sync.Mutex
	path      string // directory containing the data files
	current   atomic.Pointer[chainsMap]
	nextTS    uint64
	nextTxnID uint64
	active    map[uint64]*Txn // txnID -> active transaction
	wal       *wal
	closed    bool

	// deterministic counters
	stats Stats
}

const (
	snapshotFile = "store.db"
	walFile      = "wal.log"
)

// Open opens (or creates) a database in the given directory. It replays the
// WAL on top of the last snapshot to recover the exact committed state.
func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	snap, err := loadSnapshot(filepath.Join(dir, snapshotFile))
	if err != nil {
		return nil, err
	}

	w, err := openWAL(filepath.Join(dir, walFile))
	if err != nil {
		return nil, err
	}

	db := &DB{
		path:      dir,
		nextTS:    snap.nextTS,
		nextTxnID: snap.nextTxnID,
		active:    make(map[uint64]*Txn),
		wal:       w,
	}
	db.current.Store(&chainsMap{chains: snap.chains})

	var maxCommitTS, maxTxnID uint64
	err = w.replay(
		snap.nextTS,
		func(txnID, startTS uint64) {
			// BEGIN records are informational during recovery; no active
			// transactions are resurrected.
		},
		func(txnID, commitTS uint64, writes []walWrite) {
			db.applyWritesRecovery(txnID, commitTS, writes)
			if commitTS > maxCommitTS {
				maxCommitTS = commitTS
			}
			if txnID > maxTxnID {
				maxTxnID = txnID
			}
			db.stats.CommittedTxns++
		},
		func(txnID uint64) {
			db.stats.AbortedTxns++
		},
		func(threshold uint64) {
			db.applyGCRecovery(threshold)
		},
	)
	if err != nil {
		w.Close()
		return nil, err
	}

	if db.nextTS < maxCommitTS+1 {
		db.nextTS = maxCommitTS + 1
	}
	if db.nextTxnID < maxTxnID+1 {
		db.nextTxnID = maxTxnID + 1
	}

	// Recovery is complete; checkpoint so the WAL can be truncated.
	if err := db.checkpointLocked(); err != nil {
		w.Close()
		return nil, err
	}

	return db, nil
}

// checkpointLocked writes a snapshot and truncates the WAL. Caller must hold
// db.mu.
func (db *DB) checkpointLocked() error {
	cur := db.current.Load()
	snap := &snapshot{
		chains:    cur.chains,
		nextTS:    db.nextTS,
		nextTxnID: db.nextTxnID,
	}
	if err := snap.save(filepath.Join(db.path, snapshotFile)); err != nil {
		return err
	}
	return db.wal.truncate()
}

// writeSnapshotOnly writes a snapshot without truncating the WAL. This is
// primarily useful for testing crash-recovery edge cases. Caller must hold
// db.mu.
func (db *DB) writeSnapshotOnly() error {
	cur := db.current.Load()
	snap := &snapshot{
		chains:    cur.chains,
		nextTS:    db.nextTS,
		nextTxnID: db.nextTxnID,
	}
	return snap.save(filepath.Join(db.path, snapshotFile))
}

// Close flushes a checkpoint to disk and releases all resources.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	if err := db.checkpointLocked(); err != nil {
		db.wal.Close()
		return err
	}
	return db.wal.Close()
}

// Begin starts a new read-write transaction with a snapshot taken at the
// current timestamp.
func (db *DB) Begin() (*Txn, error) {
	return db.begin(false)
}

// BeginReadOnly starts a read-only transaction. Read-only transactions never
// block writers and are never blocked by them; they still pin their snapshot
// so GC cannot reclaim versions they might read.
func (db *DB) BeginReadOnly() (*Txn, error) {
	return db.begin(true)
}

func (db *DB) begin(readOnly bool) (*Txn, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	txnID := db.nextTxnID
	db.nextTxnID++
	startTS := db.nextTS
	db.nextTS++
	t := &Txn{
		db:       db,
		txnID:    txnID,
		startTS:  startTS,
		readOnly: readOnly,
		snapshot: db.current.Load(),
		writes:   make(map[string]walWrite),
	}
	db.active[txnID] = t
	if err := db.wal.appendBegin(txnID, startTS); err != nil {
		delete(db.active, txnID)
		return nil, err
	}
	return t, nil
}

// applyWritesRecovery applies committed writes during recovery, when there is
// no concurrency. It mutates the chains in place for simplicity.
func (db *DB) applyWritesRecovery(txnID, commitTS uint64, writes []walWrite) {
	cur := db.current.Load()
	newChains := make(map[string]*versionChain, len(cur.chains)+len(writes))
	for k, v := range cur.chains {
		newChains[k] = v
	}
	for _, wr := range writes {
		chain := newChains[wr.Key]
		if chain == nil {
			chain = &versionChain{}
		}
		newChains[wr.Key] = chain.committed(Version{
			BeginTS:   commitTS,
			EndTS:     0,
			TxnID:     txnID,
			Tombstone: wr.Tombstone,
			Value:     wr.Value,
		}, commitTS)
	}
	db.current.Store(&chainsMap{chains: newChains})
}

// applyGCRecovery applies a GC record during recovery.
func (db *DB) applyGCRecovery(threshold uint64) {
	cur := db.current.Load()
	newChains := make(map[string]*versionChain, len(cur.chains))
	reclaimed := 0
	for _, k := range cur.sortedKeys() {
		chain := cur.chains[k]
		newChain := chain.reclaimed(threshold)
		reclaimed += len(chain.versions) - len(newChain.versions)
		if len(newChain.versions) > 0 {
			newChains[k] = newChain
		}
	}
	db.current.Store(&chainsMap{chains: newChains})
	db.stats.Reclaimed += uint64(reclaimed)
}

// gcThreshold returns the oldest snapshot timestamp that must be preserved,
// or 0 if there are no active transactions. Caller must hold db.mu.
func (db *DB) gcThreshold() uint64 {
	var minTS uint64
	first := true
	for _, t := range db.active {
		if first || t.startTS < minTS {
			minTS = t.startTS
			first = false
		}
	}
	if first {
		return 0
	}
	return minTS
}

// RunGC performs one round of garbage collection, reclaiming versions that
// are no longer visible to any active transaction. It returns the number of
// reclaimed versions.
func (db *DB) RunGC() (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, ErrClosed
	}
	threshold := db.gcThreshold()
	if threshold == 0 {
		// No active transactions: all versions with a finite EndTS are dead.
		threshold = db.nextTS
	}
	cur := db.current.Load()
	newChains := make(map[string]*versionChain, len(cur.chains))
	reclaimed := 0
	for _, k := range cur.sortedKeys() {
		chain := cur.chains[k]
		newChain := chain.reclaimed(threshold)
		reclaimed += len(chain.versions) - len(newChain.versions)
		if len(newChain.versions) > 0 {
			newChains[k] = newChain
		}
	}
	if reclaimed > 0 {
		if err := db.wal.appendGC(threshold); err != nil {
			return reclaimed, err
		}
		db.current.Store(&chainsMap{chains: newChains})
		db.stats.Reclaimed += uint64(reclaimed)
	}
	return reclaimed, nil
}

// Stats returns a snapshot of the engine counters.
func (db *DB) Stats() Stats {
	db.mu.Lock()
	defer db.mu.Unlock()
	cur := db.current.Load()
	db.stats.Keys = len(cur.chains)
	db.stats.ActiveTxns = len(db.active)
	db.stats.NextTS = db.nextTS
	db.stats.NextTxnID = db.nextTxnID
	versions := 0
	live := 0
	for _, chain := range cur.chains {
		versions += len(chain.versions)
		for _, v := range chain.versions {
			if v.isLive() {
				live++
			}
		}
	}
	db.stats.Versions = versions
	db.stats.LiveVersions = live
	return db.stats
}
