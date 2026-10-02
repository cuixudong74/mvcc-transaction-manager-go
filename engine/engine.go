package engine

import "sync"

// Options configures a local on-disk MVCC engine.
type Options struct {
	Directory  string
	SyncWrites bool
}

// Engine is a process-local, multi-version transactional key-value store.
type Engine struct {
	dir        string
	wal        *WAL
	syncWrites bool

	// commitMu serializes commit decisions and WAL rewrite/GC. A commit
	// timestamp is never observed in two different orders.
	commitMu sync.Mutex

	mu           sync.RWMutex
	nextTxID     uint64
	nextCommitTS uint64
	versions     map[string][]Version
	active       map[uint64]*Txn
	closed       bool
	fatal        bool

	commits         uint64
	rollbacks       uint64
	removedVersions uint64
	gcRuns          uint64
}

// Open opens or recovers an engine from directory.
func Open(opts Options) (*Engine, error) {
	wal, recovered, err := openWAL(opts.Directory, opts.SyncWrites)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		dir:          opts.Directory,
		wal:          wal,
		syncWrites:   opts.SyncWrites,
		nextTxID:     recovered.NextTxID,
		nextCommitTS: recovered.NextCommitTS,
		versions:     deepCopyVersionMap(recovered.Versions),
		active:       make(map[uint64]*Txn),
	}
	return e, nil
}

func (e *Engine) Close() error {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.available() {
		return ErrEngineClosed
	}
	e.closed = true
	return e.wal.Close()
}

func (e *Engine) markFatal() {
	e.mu.Lock()
	defer e.mu.Unlock()
	wal := e.wal
	e.fatal = true
	e.closed = true
	if wal != nil {
		_ = wal.Close()
	}
}

func (e *Engine) available() bool {
	return !e.closed && !e.fatal
}

// Begin starts a read-write or read-only transaction. The resulting snapshot
// contains exactly commits with timestamps not newer than Begin's instant.
func (e *Engine) Begin(readOnly bool) (*Txn, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.available() {
		return nil, ErrEngineClosed
	}

	id := e.nextTxID
	e.nextTxID++
	txn := &Txn{
		engine:   e,
		id:       id,
		readTS:   e.nextCommitTS - 1,
		readOnly: readOnly,
		active:   true,
		pending:  make(map[string]pendingMutation),
	}
	e.active[id] = txn
	return txn, nil
}

// Stats contains coarse engine counters.
type Stats struct {
	ActiveTxns      uint64
	CommittedTxns   uint64
	RolledBackTxns  uint64
	Keys            uint64
	TotalVersions   uint64
	GCRuns          uint64
	RemovedVersions uint64
}

func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats := Stats{
		ActiveTxns:      uint64(len(e.active)),
		CommittedTxns:   e.commits,
		RolledBackTxns:  e.rollbacks,
		Keys:            uint64(len(e.versions)),
		GCRuns:          e.gcRuns,
		RemovedVersions: e.removedVersions,
	}
	for _, chain := range e.versions {
		stats.TotalVersions += uint64(len(chain))
	}
	return stats
}

func deepCopyVersionMap(input map[string][]Version) map[string][]Version {
	output := make(map[string][]Version, len(input))
	for key, chain := range input {
		copied := make([]Version, len(chain))
		for i, version := range chain {
			copied[i] = Version{
				TxnID:    version.TxnID,
				CommitTS: version.CommitTS,
				Value:    append([]byte(nil), version.Value...),
				Deleted:  version.Deleted,
			}
		}
		output[key] = copied
	}
	return output
}
