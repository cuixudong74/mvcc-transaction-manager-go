package engine

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	walName             = "mvcc.db"
	walTempPrefix       = walName + ".tmp."
	maxWalRecordPayload = 256 * 1024 * 1024
)

type mutationSnapshot struct {
	Value   []byte `json:"value"`
	Deleted bool   `json:"deleted"`
}

type activeSnapshot struct {
	ReadTS   uint64                      `json:"read_ts"`
	ReadOnly bool                        `json:"read_only"`
	Pending  map[string]mutationSnapshot `json:"pending"`
}

type walSnapshot struct {
	NextTxID     uint64                    `json:"next_tx_id"`
	NextCommitTS uint64                    `json:"next_commit_ts"`
	Versions     map[string][]Version      `json:"versions"`
	Active       map[uint64]activeSnapshot `json:"active"`
}

type walRecord struct {
	Type     string       `json:"type"`
	TxnID    uint64       `json:"txn_id,omitempty"`
	ReadTS   uint64       `json:"read_ts,omitempty"`
	CommitTS uint64       `json:"commit_ts,omitempty"`
	ReadOnly bool         `json:"read_only,omitempty"`
	Key      string       `json:"key,omitempty"`
	Value    []byte       `json:"value,omitempty"`
	Deleted  bool         `json:"deleted,omitempty"`
	Snapshot *walSnapshot `json:"snapshot,omitempty"`
}

// WAL is an append-only, CRC-protected commit log.
type WAL struct {
	path       string
	file       *os.File
	syncWrites bool

	mu          sync.Mutex
	validOffset int64
	rewriteSeq  uint64
}

func openWAL(dir string, syncWrites bool) (*WAL, walSnapshot, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, walSnapshot{}, err
	}
	path := filepath.Join(dir, walName)
	snapshot, validOffset, err := replayWAL(path)
	if err != nil {
		return nil, walSnapshot{}, err
	}

	// Remove temporary files left by interrupted rewrites. A completed rewrite
	// has atomically replaced the real log and leaves no live temp file.
	tempEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, walSnapshot{}, err
	}
	for _, entry := range tempEntries {
		if strings.HasPrefix(entry.Name(), walTempPrefix) {
			if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
				return nil, walSnapshot{}, err
			}
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, walSnapshot{}, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, walSnapshot{}, err
	}
	if info.Size() > validOffset {
		if err := file.Truncate(validOffset); err != nil {
			_ = file.Close()
			return nil, walSnapshot{}, err
		}
	}
	if _, err := file.Seek(validOffset, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, walSnapshot{}, err
	}

	return &WAL{
		path:        path,
		file:        file,
		syncWrites:  syncWrites,
		validOffset: validOffset,
	}, snapshot, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Sync()
	if closeErr := w.file.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	w.file = nil
	return err
}

func (w *WAL) appendBegin(txnID, readTS uint64, readOnly bool) error {
	return w.append(walRecord{
		Type:     "begin",
		TxnID:    txnID,
		ReadTS:   readTS,
		ReadOnly: readOnly,
	})
}

func (w *WAL) appendMutation(txnID uint64, key string, value []byte, deleted bool) error {
	record := walRecord{
		Type:    "mutation",
		TxnID:   txnID,
		Key:     key,
		Value:   append([]byte(nil), value...),
		Deleted: deleted,
	}
	return w.append(record)
}

func (w *WAL) appendCommit(txnID, commitTS uint64) error {
	return w.append(walRecord{Type: "commit", TxnID: txnID, CommitTS: commitTS})
}

func (w *WAL) appendRollback(txnID uint64) error {
	return w.append(walRecord{Type: "rollback", TxnID: txnID})
}

// rewrite atomically replaces the log with one authoritative snapshot record.
func (w *WAL) rewrite(snapshot walSnapshot) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return ErrEngineClosed
	}

	dir := filepath.Dir(w.path)
	w.rewriteSeq++
	tempName := fmt.Sprintf("%s%d-%d-%d", walTempPrefix, os.Getpid(), time.Now().UnixNano(), w.rewriteSeq)
	tempPath := filepath.Join(dir, tempName)
	temp, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}

	payload, err := encodeRecord(walRecord{Type: "snapshot", Snapshot: &snapshot})
	if err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if _, err := temp.Write(payload); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := syncDir(dir); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, w.path); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	oldFile := w.file
	if err := syncDir(dir); err != nil {
		_ = oldFile.Close()
		w.file = nil
		return err
	}

	newFile, err := os.OpenFile(w.path, os.O_RDWR, 0o600)
	if err != nil {
		_ = oldFile.Close()
		w.file = nil
		return err
	}
	if err := newFile.Sync(); err != nil {
		_ = newFile.Close()
		_ = oldFile.Close()
		w.file = nil
		return err
	}
	w.file = newFile
	w.validOffset = int64(len(payload))
	_ = oldFile.Close()
	return nil
}

func (w *WAL) append(record walRecord) error {
	payload, err := encodeRecord(record)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return ErrEngineClosed
	}
	if _, err := w.file.Seek(w.validOffset, io.SeekStart); err != nil {
		return err
	}
	n, err := w.file.Write(payload)
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return err
	}
	if w.syncWrites {
		if err := w.file.Sync(); err != nil {
			return err
		}
	}
	w.validOffset += int64(len(payload))
	return nil
}

func encodeRecord(record walRecord) ([]byte, error) {
	body, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(body)))
	buffer.Write(length[:])

	crc := crc32.ChecksumIEEE(body)
	var checksum [4]byte
	binary.BigEndian.PutUint32(checksum[:], crc)
	buffer.Write(checksum[:])
	buffer.Write(body)
	return buffer.Bytes(), nil
}

func replayWAL(path string) (walSnapshot, int64, error) {
	snapshot := walSnapshot{
		NextTxID:     1,
		NextCommitTS: 1,
		Versions:     make(map[string][]Version),
		Active:       make(map[uint64]activeSnapshot),
	}

	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return snapshot, 0, nil
	}
	if err != nil {
		return snapshot, 0, err
	}
	defer file.Close()

	offset := int64(0)
	for {
		record, frameSize, err := readRecord(file)
		if err == io.EOF {
			return snapshot, offset, nil
		}
		if err != nil {
			// A failed process can leave a partially appended final frame. All
			// complete frames before it remain authoritative; the torn tail is
			// discarded by the caller.
			return snapshot, offset, nil
		}
		applyRecord(&snapshot, record)
		offset += frameSize
	}
}

func readRecord(file *os.File) (walRecord, int64, error) {
	var header [8]byte
	n, err := io.ReadFull(file, header[:])
	if err == io.EOF && n == 0 {
		return walRecord{}, 0, io.EOF
	}
	if err != nil {
		return walRecord{}, int64(n), err
	}

	length := binary.BigEndian.Uint32(header[:4])
	if length == 0 || length > maxWalRecordPayload {
		return walRecord{}, 8, ErrInvalidRecord
	}
	expectedCRC := binary.BigEndian.Uint32(header[4:8])
	body := make([]byte, length)
	if _, err := io.ReadFull(file, body); err != nil {
		return walRecord{}, int64(n) + int64(len(body)), err
	}
	if crc32.ChecksumIEEE(body) != expectedCRC {
		return walRecord{}, 8 + int64(length), ErrInvalidRecord
	}

	var record walRecord
	if err := json.Unmarshal(body, &record); err != nil {
		return walRecord{}, 8 + int64(length), err
	}
	if !validRecordType(record.Type) || !recordWellFormed(record) {
		return walRecord{}, 8 + int64(length), ErrInvalidRecord
	}
	return record, 8 + int64(length), nil
}

func validRecordType(kind string) bool {
	switch kind {
	case "begin", "mutation", "commit", "rollback", "snapshot":
		return true
	default:
		return false
	}
}

func recordWellFormed(record walRecord) bool {
	switch record.Type {
	case "begin", "mutation", "commit", "rollback":
		if record.TxnID == 0 {
			return false
		}
	case "snapshot":
		return record.Snapshot != nil
	}
	switch record.Type {
	case "mutation":
		return record.Key != ""
	case "commit":
		return record.CommitTS != 0
	}
	return true
}

func applyRecord(snapshot *walSnapshot, record walRecord) {
	switch record.Type {
	case "begin":
		if _, exists := snapshot.Active[record.TxnID]; !exists {
			snapshot.Active[record.TxnID] = activeSnapshot{
				ReadTS:   record.ReadTS,
				ReadOnly: record.ReadOnly,
				Pending:  make(map[string]mutationSnapshot),
			}
		}
		if record.TxnID >= snapshot.NextTxID {
			snapshot.NextTxID = record.TxnID + 1
		}
	case "mutation":
		// A mutation without a durable begin belongs to a torn old tail or a
		// transaction whose metadata was removed. It must never become visible.
		active, exists := snapshot.Active[record.TxnID]
		if !exists || active.ReadOnly {
			break
		}
		if active.Pending == nil {
			active.Pending = make(map[string]mutationSnapshot)
		}
		active.Pending[record.Key] = mutationSnapshot{
			Value:   append([]byte(nil), record.Value...),
			Deleted: record.Deleted,
		}
		snapshot.Active[record.TxnID] = active
	case "commit":
		active, exists := snapshot.Active[record.TxnID]
		if exists {
			for key, mutation := range active.Pending {
				version := Version{
					TxnID:    record.TxnID,
					CommitTS: record.CommitTS,
					Value:    append([]byte(nil), mutation.Value...),
					Deleted:  mutation.Deleted,
				}
				snapshot.Versions[key] = append([]Version{version}, snapshot.Versions[key]...)
			}
			delete(snapshot.Active, record.TxnID)
		}
		if record.CommitTS >= snapshot.NextCommitTS {
			snapshot.NextCommitTS = record.CommitTS + 1
		}
		if record.TxnID >= snapshot.NextTxID {
			snapshot.NextTxID = record.TxnID + 1
		}
	case "rollback":
		delete(snapshot.Active, record.TxnID)
		if record.TxnID >= snapshot.NextTxID {
			snapshot.NextTxID = record.TxnID + 1
		}
	case "snapshot":
		if record.Snapshot != nil {
			*snapshot = normalizeSnapshot(*record.Snapshot)
		}
	}
}

func normalizeSnapshot(input walSnapshot) walSnapshot {
	if input.NextTxID == 0 {
		input.NextTxID = 1
	}
	if input.NextCommitTS == 0 {
		input.NextCommitTS = 1
	}
	if input.Versions == nil {
		input.Versions = make(map[string][]Version)
	}
	if input.Active == nil {
		input.Active = make(map[uint64]activeSnapshot)
	}
	for id, active := range input.Active {
		if active.Pending == nil {
			active.Pending = make(map[string]mutationSnapshot)
			input.Active[id] = active
		}
	}
	return input
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
