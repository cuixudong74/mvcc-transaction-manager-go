package mvcc

import (
	"sort"
)

// Txn is a transaction running under snapshot isolation.
//
// A transaction sees a consistent snapshot of the database as of its start
// timestamp plus its own pending writes (read-your-writes).
type Txn struct {
	db       *DB
	txnID    uint64
	startTS  uint64
	readOnly bool
	done     bool

	// snapshot is the immutable view of the database captured at begin.
	snapshot *chainsMap

	// pending writes: key -> write (value nil with tombstone == true means delete)
	writes map[string]walWrite
}

// Get returns the value for key visible in this transaction's snapshot.
// ok is false if the key does not exist (or was deleted) in the snapshot.
func (t *Txn) Get(key string) (value []byte, ok bool, err error) {
	if t.done {
		return nil, false, ErrTxnDone
	}
	// Read-your-writes: pending writes take precedence.
	if wr, exists := t.writes[key]; exists {
		if wr.Tombstone {
			return nil, false, nil
		}
		return wr.Value, true, nil
	}
	chain := t.snapshot.get(key)
	if chain == nil {
		return nil, false, nil
	}
	v := chain.visibleAt(t.startTS)
	if v == nil || v.Tombstone {
		return nil, false, nil
	}
	return v.Value, true, nil
}

// Put sets key to value.
func (t *Txn) Put(key string, value []byte) error {
	if t.done {
		return ErrTxnDone
	}
	if t.readOnly {
		return ErrReadOnly
	}
	t.writes[key] = walWrite{Key: key, Value: value}
	return nil
}

// Delete removes key.
func (t *Txn) Delete(key string) error {
	if t.done {
		return ErrTxnDone
	}
	if t.readOnly {
		return ErrReadOnly
	}
	t.writes[key] = walWrite{Key: key, Tombstone: true}
	return nil
}

// Scan returns all key/value pairs in the range [start, end) visible in this
// transaction's snapshot. If end is empty, the range is unbounded to the
// right. The returned map is a self-consistent snapshot.
func (t *Txn) Scan(start, end string) (map[string][]byte, error) {
	if t.done {
		return nil, ErrTxnDone
	}
	result := make(map[string][]byte)
	for _, k := range t.snapshot.sortedKeys() {
		if k < start || (end != "" && k >= end) {
			continue
		}
		// Pending writes take precedence (read-your-writes).
		if wr, exists := t.writes[k]; exists {
			if !wr.Tombstone {
				result[k] = wr.Value
			}
			continue
		}
		chain := t.snapshot.get(k)
		if chain == nil {
			continue
		}
		v := chain.visibleAt(t.startTS)
		if v == nil || v.Tombstone {
			continue
		}
		result[k] = v.Value
	}
	// Include pending writes for keys not in the snapshot.
	for k, wr := range t.writes {
		if k < start || (end != "" && k >= end) {
			continue
		}
		if wr.Tombstone {
			continue
		}
		if _, exists := result[k]; !exists {
			result[k] = wr.Value
		}
	}
	return result, nil
}

// sortedWriteKeys returns the written keys in sorted order for deterministic
// WAL records.
func (t *Txn) sortedWriteKeys() []string {
	keys := make([]string, 0, len(t.writes))
	for k := range t.writes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Commit commits the transaction. It returns ErrConflict if a concurrent
// transaction committed a write to one of this transaction's keys after its
// snapshot (first-committer-wins).
func (t *Txn) Commit() error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if t.done {
		return ErrTxnDone
	}
	t.done = true
	delete(t.db.active, t.txnID)

	if t.readOnly || len(t.writes) == 0 {
		// Read-only / empty transactions do not produce a state change.
		return nil
	}

	// Conflict detection (first-committer-wins): if any written key was
	// modified by a transaction that committed after this transaction's
	// snapshot, this transaction must abort.
	cur := t.db.current.Load()
	for _, k := range t.sortedWriteKeys() {
		chain := cur.get(k)
		if chain == nil {
			continue
		}
		latest := chain.latest()
		if latest != nil && latest.BeginTS > t.startTS {
			t.db.stats.AbortedTxns++
			return ErrConflict
		}
	}

	// Assign the commit timestamp.
	commitTS := t.db.nextTS
	t.db.nextTS++

	// Build the WAL record with sorted writes for determinism.
	writes := make([]walWrite, 0, len(t.writes))
	for _, k := range t.sortedWriteKeys() {
		writes = append(writes, t.writes[k])
	}

	// The WAL fsync is the single commit point.
	if err := t.db.wal.appendCommit(t.txnID, commitTS, writes); err != nil {
		t.db.stats.AbortedTxns++
		return err
	}

	// Apply the writes to a new immutable snapshot.
	newChains := make(map[string]*versionChain, len(cur.chains)+len(writes))
	for k, v := range cur.chains {
		newChains[k] = v
	}
	for _, wr := range writes {
		chain := newChains[wr.Key]
		if chain == nil {
			chain = &versionChain{}
		}
		if wr.Tombstone {
			// A delete is a no-op if the key is already absent or already a
			// tombstone in the current committed state.
			if chain.latest() == nil || chain.latest().Tombstone {
				continue
			}
		}
		newChains[wr.Key] = chain.committed(Version{
			BeginTS:   commitTS,
			EndTS:     0,
			TxnID:     t.txnID,
			Tombstone: wr.Tombstone,
			Value:     wr.Value,
		}, commitTS)
	}
	t.db.current.Store(&chainsMap{chains: newChains})
	t.db.stats.CommittedTxns++
	return nil
}

// Rollback discards the transaction's pending writes.
func (t *Txn) Rollback() error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if t.done {
		return ErrTxnDone
	}
	t.done = true
	delete(t.db.active, t.txnID)
	if !t.readOnly {
		// Best-effort abort record; uncommitted transactions are ignored
		// during recovery anyway.
		_ = t.db.wal.appendAbort(t.txnID)
	}
	t.db.stats.AbortedTxns++
	return nil
}

// StartTS returns the snapshot timestamp of the transaction.
func (t *Txn) StartTS() uint64 { return t.startTS }

// ID returns the transaction id.
func (t *Txn) ID() uint64 { return t.txnID }
