package mvcc

import "errors"

// Errors returned by the transaction engine.
var (
	// ErrConflict is returned when a transaction cannot commit because a
	// concurrent transaction already committed a write to one of its keys
	// (first-committer-wins policy).
	ErrConflict = errors.New("mvcc: write-write conflict: key was modified by a concurrent transaction")

	// ErrTxnDone is returned when an operation is attempted on a transaction
	// that has already been committed or rolled back.
	ErrTxnDone = errors.New("mvcc: transaction already finished")

	// ErrReadOnly is returned when a write is attempted on a read-only
	// transaction.
	ErrReadOnly = errors.New("mvcc: transaction is read-only")

	// ErrCorrupt is returned when a data file or WAL record fails its
	// checksum or fails a structural validation.
	ErrCorrupt = errors.New("mvcc: corrupt data")

	// ErrClosed is returned when an operation is attempted on a closed DB.
	ErrClosed = errors.New("mvcc: database is closed")
)
