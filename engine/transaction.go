package engine

import (
	"sort"
	"sync"
)

type pendingMutation struct {
	value   []byte
	deleted bool
}

// Txn is a snapshot-isolation transaction.
type Txn struct {
	engine     *Engine
	id         uint64
	readTS     uint64
	readOnly   bool
	active     bool
	publishing bool

	// t.mu serializes Close/Commit/Rollback for one transaction and protects
	// pending. Locking order is Engine.mu before Txn.mu when both are needed.
	mu      sync.Mutex
	pending map[string]pendingMutation
}

// KVPair is one result from a range scan.
type KVPair struct {
	Key   string
	Value []byte
}

// Range describes a lexicographic [Start, End) scan. Empty bounds are unlimited.
type Range struct {
	Start string
	End   string
	Limit int
}

func (t *Txn) Put(key string, value []byte) error {
	if t.readOnly {
		return ErrReadOnly
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.engine.mu.RLock()
	defer t.engine.mu.RUnlock()
	if !t.engine.available() {
		return ErrEngineClosed
	}
	if !t.active {
		return ErrTxnClosed
	}
	stored := make([]byte, len(value))
	copy(stored, value)
	t.pending[key] = pendingMutation{value: stored}
	return nil
}

func (t *Txn) Delete(key string) error {
	if t.readOnly {
		return ErrReadOnly
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.engine.mu.RLock()
	defer t.engine.mu.RUnlock()
	if !t.engine.available() {
		return ErrEngineClosed
	}
	if !t.active {
		return ErrTxnClosed
	}
	t.pending[key] = pendingMutation{deleted: true}
	return nil
}

// Commit atomically validates and durably publishes all writes in this
// transaction. A conflict leaves the pending writes unpublished; the caller may
// start a new transaction and retry the operation.
func (t *Txn) Commit() error {
	if t.readOnly {
		return t.finishWithoutWAL()
	}

	t.engine.commitMu.Lock()
	defer t.engine.commitMu.Unlock()

	t.engine.mu.Lock()
	t.mu.Lock()
	switch {
	case t.engine.closed:
		t.mu.Unlock()
		t.engine.mu.Unlock()
		return ErrEngineClosed
	case !t.active:
		t.mu.Unlock()
		t.engine.mu.Unlock()
		return ErrTxnClosed
	}

	for key := range t.pending {
		chain := t.engine.versions[key]
		if len(chain) > 0 && chain[0].CommitTS > t.readTS {
			t.active = false
			t.pending = make(map[string]pendingMutation)
			delete(t.engine.active, t.id)
			t.engine.rollbacks++
			t.mu.Unlock()
			t.engine.mu.Unlock()
			return ErrConflict
		}
	}

	commitTS := t.engine.nextCommitTS
	// Prepare committed values while state is locked, then perform file I/O only
	// after unlocking readers. No other commit can run concurrently.
	pending := make(map[string]pendingMutation, len(t.pending))
	for key, mutation := range t.pending {
		pending[key] = pendingMutation{
			value:   append([]byte(nil), mutation.value...),
			deleted: mutation.deleted,
		}
	}
	t.active = false
	t.publishing = true
	t.engine.mu.Unlock()
	t.mu.Unlock()

	// Uncommitted writes exist only in memory until this serialized commit
	// section. The commit record is the single visibility decision; a crash
	// before it leaves begin/mutation frames invisible during replay.
	if len(pending) > 0 {
		if err := t.engine.wal.appendBegin(t.id, t.readTS, false); err != nil {
			t.engine.markFatal()
			return err
		}
		keys := make([]string, 0, len(pending))
		for key := range pending {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			mutation := pending[key]
			if err := t.engine.wal.appendMutation(t.id, key, mutation.value, mutation.deleted); err != nil {
				t.engine.markFatal()
				return err
			}
		}
	}
	if err := t.engine.wal.appendCommit(t.id, commitTS); err != nil {
		t.engine.markFatal()
		return err
	}

	t.engine.mu.Lock()
	for key, mutation := range pending {
		version := Version{
			TxnID:    t.id,
			CommitTS: commitTS,
			Value:    append([]byte(nil), mutation.value...),
			Deleted:  mutation.deleted,
		}
		t.engine.versions[key] = append([]Version{version}, t.engine.versions[key]...)
	}
	t.engine.nextCommitTS = commitTS + 1
	t.engine.commits++
	delete(t.engine.active, t.id)
	t.engine.mu.Unlock()
	return nil
}

func (t *Txn) Rollback() error {
	if t.readOnly {
		return t.finishWithoutWAL()
	}

	t.engine.commitMu.Lock()
	defer t.engine.commitMu.Unlock()
	t.engine.mu.Lock()
	t.mu.Lock()
	switch {
	case t.engine.closed:
		t.mu.Unlock()
		t.engine.mu.Unlock()
		return ErrEngineClosed
	case !t.active:
		// Rollback is idempotent and is used in defer/error cleanup paths after
		// either explicit rollback or conflict-driven termination.
		t.mu.Unlock()
		t.engine.mu.Unlock()
		return nil
	}
	t.pending = make(map[string]pendingMutation)
	t.active = false
	delete(t.engine.active, t.id)
	t.engine.rollbacks++
	t.mu.Unlock()
	t.engine.mu.Unlock()
	return nil
}

func (t *Txn) finishWithoutWAL() error {
	t.engine.mu.Lock()
	t.mu.Lock()
	switch {
	case t.engine.closed:
		t.mu.Unlock()
		t.engine.mu.Unlock()
		return ErrEngineClosed
	case !t.active:
		t.mu.Unlock()
		t.engine.mu.Unlock()
		return ErrTxnClosed
	}
	t.active = false
	delete(t.engine.active, t.id)
	if !t.readOnly {
		t.engine.rollbacks++
	}
	t.mu.Unlock()
	t.engine.mu.Unlock()
	return nil
}

func (t *Txn) Get(key string) ([]byte, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.engine.mu.RLock()
	defer t.engine.mu.RUnlock()
	if !t.engine.available() {
		return nil, false, ErrEngineClosed
	}
	if !t.active {
		return nil, false, ErrTxnClosed
	}
	if mutation, exists := t.pending[key]; exists {
		if mutation.deleted {
			return nil, false, nil
		}
		return append([]byte(nil), mutation.value...), true, nil
	}
	value, exists := visibleValue(t.engine.versions[key], t.readTS)
	return value, exists, nil
}

// Scan returns a stable view of [Start, End) from this transaction's snapshot.
// Values written by the transaction itself are read consistently.
func (t *Txn) Scan(r Range) ([]KVPair, error) {
	if r.Start != "" && r.End != "" && r.Start >= r.End {
		return nil, ErrInvalidRange
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.engine.mu.RLock()
	defer t.engine.mu.RUnlock()
	if !t.engine.available() {
		return nil, ErrEngineClosed
	}
	if !t.active {
		return nil, ErrTxnClosed
	}

	keys := make(map[string]struct{}, len(t.engine.versions)+len(t.pending))
	for key := range t.engine.versions {
		if inRange(key, r.Start, r.End) {
			keys[key] = struct{}{}
		}
	}
	for key := range t.pending {
		if inRange(key, r.Start, r.End) {
			keys[key] = struct{}{}
		}
	}

	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)

	result := make([]KVPair, 0)
	for _, key := range ordered {
		if mutation, exists := t.pending[key]; exists {
			if mutation.deleted {
				continue
			}
			result = append(result, KVPair{Key: key, Value: append([]byte(nil), mutation.value...)})
		} else if value, exists := visibleValue(t.engine.versions[key], t.readTS); exists {
			result = append(result, KVPair{Key: key, Value: value})
		} else {
			continue
		}
		if r.Limit > 0 && len(result) >= r.Limit {
			break
		}
	}
	return result, nil
}

func inRange(key, start, end string) bool {
	if start != "" && key < start {
		return false
	}
	if end != "" && key >= end {
		return false
	}
	return true
}
