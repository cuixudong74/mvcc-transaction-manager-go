# MVCC Transaction Manager

An embedded, multi-version concurrency controlled (MVCC) key-value storage engine
written in Go. It provides snapshot isolation for read-write transactions,
first-committer-wins conflict detection, multi-version chain maintenance with
garbage collection, and crash recovery via a write-ahead log.

## Features

- **Snapshot isolation**: every transaction sees a consistent snapshot of the
  database as of its start timestamp. Reads never block writes and writes never
  block reads.
- **Write-write conflict detection**: two transactions modifying the same key
  with overlapping transaction intervals are resolved by the first-committer-wins
  policy; the losing transaction aborts without leaving partial writes.
- **Multi-version chains**: each key maintains a chain of immutable revisions
  with disjoint validity intervals `[BeginTS, EndTS)`.
- **Garbage collection**: versions no longer visible to any active transaction
  are reclaimed. Long-running transactions pin their snapshot so in-use versions
  are never deleted.
- **Crash recovery**: a write-ahead log (WAL) records committed transactions.
  On restart, the WAL is replayed on top of the last snapshot to recover the
  exact committed state. Torn writes are detected via CRC32 checksums and
  ignored.
- **Deterministic replay**: timestamps are assigned from a monotonic counter and
  GC iterates keys in sorted order, so replaying the same operation sequence
  produces identical visible data and GC results.
- **Read-only transactions**: read-only transactions capture an immutable
  snapshot pointer and read without taking any lock, so they are never blocked
  by concurrent writes or GC.
- **Range scans**: scans return a self-consistent snapshot of all keys in a
  range, never mixing states from different commits.

## Isolation Semantics

The engine implements **snapshot isolation (SI)**. Each transaction is assigned
a unique start timestamp from a global monotonic counter at `Begin`. A version
is visible to a transaction if and only if:

```
version.BeginTS <= txn.StartTS && (version.EndTS == 0 || txn.StartTS < version.EndTS)
```

where `EndTS == 0` denotes the latest revision (validity interval
`[BeginTS, +inf)`).

Because start timestamps are assigned from the same counter as commit timestamps,
a transaction sees exactly the versions that were committed before it began.
Concurrent commits that happen after the transaction's start are invisible,
guaranteeing a consistent snapshot.

Read-only transactions capture an immutable copy-on-write snapshot pointer at
begin time. This pointer is never modified, so reads proceed without locks and
are never blocked by writers or GC.

## Conflict Policy

The engine uses the **first-committer-wins (FCW)** policy for write-write
conflicts. At commit time, for each key in the transaction's write set, the
engine checks whether the key's latest committed version has a `BeginTS` greater
than the transaction's `StartTS`. If so, a concurrent transaction committed a
write to that key after the transaction's snapshot, and the transaction aborts
with `ErrConflict`.

The abort is atomic: no partial writes are left visible. The transaction's
pending writes are discarded, and the version chains are untouched.

Keys written by the same transaction do not conflict with each other.
Transactions with disjoint write sets commit independently.

## Garbage Collection Rules

A version is reclaimable when it is no longer visible to any active transaction.
The GC threshold is the minimum start timestamp among all active transactions
(or `+inf` if there are no active transactions). A version with `EndTS <=
threshold` is invisible to every active transaction and can be safely removed.

Key rules:

- The latest revision (`EndTS == 0`) of every key is never reclaimed.
- Long-running transactions pin their snapshot: versions visible to them
  (`EndTS > threshold`) are preserved.
- GC iterates keys in sorted order for deterministic results.
- GC decisions are logged to the WAL, so replaying the WAL reproduces the same
  GC outcome.
- Deleted keys (tombstones) follow the same rules: a tombstone with
  `EndTS <= threshold` is reclaimed, and if no versions remain, the key is
  removed entirely.

Run GC manually with `db.RunGC()`. The engine does not run GC automatically,
giving applications control over when reclamation occurs.

## Crash Recovery

The engine uses a write-ahead log (WAL) and periodic snapshots:

1. **Commit**: the transaction's writes are serialized into a single COMMIT
   record, fsynced to the WAL. This is the commit point: once fsynced, the
   transaction is guaranteed to be recoverable.
2. **Checkpoint**: a snapshot of all live version chains is written to a temp
   file, fsynced, and atomically renamed. The WAL is then truncated.
3. **Recovery**: on open, the snapshot is loaded and the WAL is replayed.
   Committed transactions are redone; aborted transactions are ignored. COMMIT
   records with a timestamp already reflected in the snapshot are skipped to
   avoid duplication. Torn WAL records (detected via CRC32) stop the replay.

The atomic rename ensures the snapshot is never partially written. The WAL
fsync ensures committed transactions survive a crash.

## API Usage

```go
package main

import (
    "fmt"
    "github.com/cuixudong74/mvcc-transaction-manager-go"
)

func main() {
    db, err := mvcc.Open("/path/to/data")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Read-write transaction
    txn, err := db.Begin()
    if err != nil {
        panic(err)
    }
    txn.Put("key", []byte("value"))
    if err := txn.Commit(); err != nil {
        // ErrConflict: retry the transaction
    }

    // Read-only transaction (never blocked by writers)
    rotx, err := db.BeginReadOnly()
    if err != nil {
        panic(err)
    }
    v, ok, err := rotx.Get("key")
    // ...
    rotx.Commit()

    // Range scan [start, end)
    m, err := rotx.Scan("a", "z")

    // Garbage collection
    reclaimed, err := db.RunGC()

    // Stats
    stats := db.Stats()
    fmt.Printf("keys=%d versions=%d active=%d\n",
        stats.Keys, stats.Versions, stats.ActiveTxns)
}
```

## Error Types

| Error | Description |
|-------|-------------|
| `ErrConflict` | Write-write conflict (first-committer-wins) |
| `ErrTxnDone` | Operation on an already-finished transaction |
| `ErrReadOnly` | Write attempted on a read-only transaction |
| `ErrCorrupt` | Corrupt data file or WAL record |
| `ErrClosed` | Operation on a closed database |

## Testing

```bash
go test -v ./...
```

The test suite covers:

- Basic put/get, delete, rollback
- Snapshot isolation (including with deletes)
- Write-write conflict (same key and disjoint keys)
- Read-your-writes
- Garbage collection (including with long-running and concurrent transactions)
- Crash recovery (process kill, kill during commit, snapshot-no-truncate)
- Deterministic replay
- Read-only transactions not blocked by writers
- Range scans (including under concurrent writes)
- Concurrent transfers (balance conservation)
- Stats

## File Layout

```
mvcc.go       - DB core, Open/Close/Begin/RunGC/Stats
txn.go        - Transaction operations (Get/Put/Delete/Scan/Commit/Rollback)
version.go    - Version and version chain (immutable, copy-on-write)
wal.go        - Write-ahead log with CRC32-framed records
snapshot.go   - Atomic snapshot save/load
errors.go     - Error types
mvcc_test.go  - Test suite
```
