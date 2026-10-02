package mvcc

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// WAL record types.
const (
	walRecordBegin      byte = 1
	walRecordCommit     byte = 2
	walRecordAbort      byte = 3
	walRecordGC         byte = 4
	walRecordCheckpoint byte = 5
)

const walMagic byte = 'W'

// walWrite is a single key/value mutation embedded in a COMMIT record.
type walWrite struct {
	Key       string
	Value     []byte
	Tombstone bool
}

// wal is the append-only write-ahead log.
//
// Record framing:
//
//	+--------+--------+------------------+------------------+
//	| magic  | type   | payload len (4B) | CRC32 (4B)       |
//	+--------+--------+------------------+------------------+
//	| payload (payload len bytes)                            |
//	+--------------------------------------------------------+
//
// Every record is written in a single write(2) call and fsynced before the
// operation is reported as durable, so a process kill can only leave a torn
// tail record, which replay detects via checksum and stops at.
type wal struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &wal{file: f, path: path}, nil
}

func (w *wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// sync fsyncs the WAL file. Caller must hold w.mu.
func (w *wal) sync() error {
	return w.file.Sync()
}

func appendUvarint(buf []byte, x uint64) []byte {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], x)
	return append(buf, tmp[:n]...)
}

func appendString(buf []byte, s string) []byte {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(len(s)))
	buf = append(buf, tmp[:]...)
	buf = append(buf, s...)
	return buf
}

func appendBytes(buf []byte, b []byte) []byte {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(len(b)))
	buf = append(buf, tmp[:]...)
	buf = append(buf, b...)
	return buf
}

func readUvarint(b []byte) (uint64, int, error) {
	x, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, 0, ErrCorrupt
	}
	return x, n, nil
}

func readString(b []byte) (string, int, error) {
	if len(b) < 2 {
		return "", 0, ErrCorrupt
	}
	n := int(binary.BigEndian.Uint16(b[:2]))
	if len(b) < 2+n {
		return "", 0, ErrCorrupt
	}
	return string(b[2 : 2+n]), 2 + n, nil
}

func readBytes(b []byte) ([]byte, int, error) {
	if len(b) < 4 {
		return nil, 0, ErrCorrupt
	}
	n := int(binary.BigEndian.Uint32(b[:4]))
	if len(b) < 4+n {
		return nil, 0, ErrCorrupt
	}
	out := make([]byte, n)
	copy(out, b[4:4+n])
	return out, 4 + n, nil
}

// encodeRecord frames a payload with a header and CRC.
func encodeRecord(recType byte, payload []byte) []byte {
	header := make([]byte, 6)
	header[0] = walMagic
	header[1] = recType
	binary.BigEndian.PutUint32(header[2:6], uint32(len(payload)))
	crc := crc32.ChecksumIEEE(payload)
	out := make([]byte, 0, 6+4+len(payload))
	out = append(out, header...)
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc)
	out = append(out, crcBuf[:]...)
	out = append(out, payload...)
	return out
}

func (w *wal) appendRecord(recType byte, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return ErrClosed
	}
	rec := encodeRecord(recType, payload)
	if _, err := w.file.Write(rec); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *wal) appendBegin(txnID, startTS uint64) error {
	payload := make([]byte, 0, 16)
	payload = appendUvarint(payload, txnID)
	payload = appendUvarint(payload, startTS)
	return w.appendRecord(walRecordBegin, payload)
}

func (w *wal) appendCommit(txnID, commitTS uint64, writes []walWrite) error {
	payload := make([]byte, 0, 32+len(writes)*16)
	payload = appendUvarint(payload, txnID)
	payload = appendUvarint(payload, commitTS)
	payload = appendUvarint(payload, uint64(len(writes)))
	for _, wr := range writes {
		payload = appendString(payload, wr.Key)
		flags := byte(0)
		if wr.Tombstone {
			flags = 1
		}
		payload = append(payload, flags)
		payload = appendBytes(payload, wr.Value)
	}
	return w.appendRecord(walRecordCommit, payload)
}

func (w *wal) appendAbort(txnID uint64) error {
	payload := make([]byte, 0, 8)
	payload = appendUvarint(payload, txnID)
	return w.appendRecord(walRecordAbort, payload)
}

func (w *wal) appendGC(threshold uint64) error {
	payload := make([]byte, 0, 8)
	payload = appendUvarint(payload, threshold)
	return w.appendRecord(walRecordGC, payload)
}

func (w *wal) appendCheckpoint() error {
	return w.appendRecord(walRecordCheckpoint, nil)
}

// replay scans the WAL from the beginning and invokes the callbacks for each
// valid record. It stops silently at the first corrupt record (torn tail).
//
// COMMIT records with commitTS < minCommitTS are skipped: they are already
// reflected in the loaded snapshot (this can happen if the process was killed
// between writing a new snapshot and truncating the WAL).
func (w *wal) replay(minCommitTS uint64, onBegin func(txnID, startTS uint64), onCommit func(txnID, commitTS uint64, writes []walWrite), onAbort func(txnID uint64), onGC func(threshold uint64)) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return ErrClosed
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	defer w.file.Seek(0, io.SeekEnd)

	header := make([]byte, 6)
	for {
		if _, err := io.ReadFull(w.file, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil // clean end or torn tail
			}
			return err
		}
		if header[0] != walMagic {
			return nil // torn tail: stop
		}
		recType := header[1]
		payloadLen := binary.BigEndian.Uint32(header[2:6])
		if payloadLen > 64*1024*1024 { // sanity bound
			return nil
		}
		crcBuf := make([]byte, 4)
		if _, err := io.ReadFull(w.file, crcBuf); err != nil {
			return nil // torn tail
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			return nil // torn tail
		}
		if binary.BigEndian.Uint32(crcBuf) != crc32.ChecksumIEEE(payload) {
			return nil // checksum mismatch: torn tail
		}
		switch recType {
		case walRecordBegin:
			txnID, n, err := readUvarint(payload)
			if err != nil {
				return nil
			}
			startTS, _, err := readUvarint(payload[n:])
			if err != nil {
				return nil
			}
			onBegin(txnID, startTS)
		case walRecordCommit:
			commitTS, err := peekCommitTS(payload)
			if err != nil {
				return nil
			}
			if commitTS < minCommitTS {
				continue // already reflected in the snapshot
			}
			if err := replayCommit(payload, onCommit); err != nil {
				return nil
			}
		case walRecordAbort:
			txnID, _, err := readUvarint(payload)
			if err != nil {
				return nil
			}
			onAbort(txnID)
		case walRecordGC:
			threshold, _, err := readUvarint(payload)
			if err != nil {
				return nil
			}
			onGC(threshold)
		case walRecordCheckpoint:
			// checkpoint markers are informational; truncation is driven by
			// the snapshot writer.
		}
	}
}

// peekCommitTS extracts the commit timestamp from a COMMIT payload without
// decoding the writes.
func peekCommitTS(payload []byte) (uint64, error) {
	_, n, err := readUvarint(payload)
	if err != nil {
		return 0, err
	}
	commitTS, _, err := readUvarint(payload[n:])
	if err != nil {
		return 0, err
	}
	return commitTS, nil
}

func replayCommit(payload []byte, onCommit func(txnID, commitTS uint64, writes []walWrite)) error {
	txnID, n, err := readUvarint(payload)
	if err != nil {
		return err
	}
	pos := n
	commitTS, n, err := readUvarint(payload[pos:])
	if err != nil {
		return err
	}
	pos += n
	numWrites, n, err := readUvarint(payload[pos:])
	if err != nil {
		return err
	}
	pos += n
	writes := make([]walWrite, 0, numWrites)
	for i := uint64(0); i < numWrites; i++ {
		key, n, err := readString(payload[pos:])
		if err != nil {
			return err
		}
		pos += n
		if pos >= len(payload) {
			return ErrCorrupt
		}
		flags := payload[pos]
		pos++
		value, n, err := readBytes(payload[pos:])
		if err != nil {
			return err
		}
		pos += n
		writes = append(writes, walWrite{Key: key, Value: value, Tombstone: flags&1 != 0})
	}
	onCommit(txnID, commitTS, writes)
	return nil
}

// truncate removes every record from the WAL. It is called only while the DB
// lock is held, so no commit can be in flight: the snapshot written just
// before truncation contains all committed state, and commits that happen
// after truncation append fresh records.
func (w *wal) truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return ErrClosed
	}
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return w.file.Sync()
}
