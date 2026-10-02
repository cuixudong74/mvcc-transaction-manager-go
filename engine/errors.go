package engine

import "errors"

var (
	ErrEngineClosed  = errors.New("engine: engine is closed")
	ErrTxnClosed     = errors.New("engine: transaction is already committed or rolled back")
	ErrReadOnly      = errors.New("engine: cannot write in a read-only transaction")
	ErrConflict      = errors.New("engine: write-write conflict under snapshot isolation")
	ErrInvalidRange  = errors.New("engine: invalid scan range")
	ErrInvalidRecord = errors.New("engine: invalid WAL record")
)
