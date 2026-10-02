package mvcc

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	snapshotMagic   = "MVCC"
	snapshotVersion = 1
)

// snapshot is the on-disk representation of all live version chains. It is
// written atomically (temp file + fsync + rename) and represents a checkpoint
// up to which the WAL can be truncated.
type snapshot struct {
	keys     []string
	chains   map[string]*versionChain
	nextTS   uint64
	nextTxnID uint64
}

// encode serializes the snapshot. Keys are sorted so the output is
// deterministic.
func (s *snapshot) encode() []byte {
	var buf bytes.Buffer
	buf.WriteString(snapshotMagic)
	var ver [2]byte
	binary.BigEndian.PutUint16(ver[:], snapshotVersion)
	buf.Write(ver[:])

	writeUvarint := func(x uint64) {
		var tmp [10]byte
		n := binary.PutUvarint(tmp[:], x)
		buf.Write(tmp[:n])
	}
	writeString := func(str string) {
		var tmp [2]byte
		binary.BigEndian.PutUint16(tmp[:], uint16(len(str)))
		buf.Write(tmp[:])
		buf.WriteString(str)
	}
	writeBytes := func(b []byte) {
		var tmp [4]byte
		binary.BigEndian.PutUint32(tmp[:], uint32(len(b)))
		buf.Write(tmp[:])
		buf.Write(b)
	}

	writeUvarint(s.nextTS)
	writeUvarint(s.nextTxnID)
	writeUvarint(uint64(len(s.chains)))

	keys := make([]string, 0, len(s.chains))
	for k := range s.chains {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		writeString(k)
		chain := s.chains[k]
		writeUvarint(uint64(len(chain.versions)))
		for _, v := range chain.versions {
			writeUvarint(v.BeginTS)
			writeUvarint(v.EndTS)
			writeUvarint(v.TxnID)
			flags := byte(0)
			if v.Tombstone {
				flags |= 1
			}
			buf.WriteByte(flags)
			writeBytes(v.Value)
		}
	}
	return buf.Bytes()
}

func decodeSnapshot(data []byte) (*snapshot, error) {
	if len(data) < 6 || string(data[:4]) != snapshotMagic {
		return nil, ErrCorrupt
	}
	pos := 4
	ver := binary.BigEndian.Uint16(data[pos : pos+2])
	pos += 2
	if ver != snapshotVersion {
		return nil, ErrCorrupt
	}

	readUvarint := func() (uint64, error) {
		x, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return 0, ErrCorrupt
		}
		pos += n
		return x, nil
	}
	readString := func() (string, error) {
		if pos+2 > len(data) {
			return "", ErrCorrupt
		}
		n := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+n > len(data) {
			return "", ErrCorrupt
		}
		str := string(data[pos : pos+n])
		pos += n
		return str, nil
	}
	readBytes := func() ([]byte, error) {
		if pos+4 > len(data) {
			return nil, ErrCorrupt
		}
		n := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+n > len(data) {
			return nil, ErrCorrupt
		}
		out := make([]byte, n)
		copy(out, data[pos:pos+n])
		pos += n
		return out, nil
	}

	nextTS, err := readUvarint()
	if err != nil {
		return nil, err
	}
	nextTxnID, err := readUvarint()
	if err != nil {
		return nil, err
	}
	numKeys, err := readUvarint()
	if err != nil {
		return nil, err
	}
	chains := make(map[string]*versionChain, numKeys)
	keys := make([]string, 0, numKeys)
	for i := uint64(0); i < numKeys; i++ {
		k, err := readString()
		if err != nil {
			return nil, err
		}
		numVersions, err := readUvarint()
		if err != nil {
			return nil, err
		}
		chain := &versionChain{versions: make([]Version, 0, numVersions)}
		for j := uint64(0); j < numVersions; j++ {
			beginTS, err := readUvarint()
			if err != nil {
				return nil, err
			}
			endTS, err := readUvarint()
			if err != nil {
				return nil, err
			}
			txnID, err := readUvarint()
			if err != nil {
				return nil, err
			}
			if pos >= len(data) {
				return nil, ErrCorrupt
			}
			flags := data[pos]
			pos++
			value, err := readBytes()
			if err != nil {
				return nil, err
			}
			chain.versions = append(chain.versions, Version{
				BeginTS:   beginTS,
				EndTS:     endTS,
				TxnID:     txnID,
				Tombstone: flags&1 != 0,
				Value:     value,
			})
		}
		chains[k] = chain
		keys = append(keys, k)
	}
	if pos != len(data) {
		return nil, ErrCorrupt
	}
	return &snapshot{keys: keys, chains: chains, nextTS: nextTS, nextTxnID: nextTxnID}, nil
}

// save writes the snapshot to path atomically: temp file + fsync + rename +
// directory fsync.
func (s *snapshot) save(path string) error {
	data := s.encode()
	payload := make([]byte, 0, len(data)+4)
	payload = append(payload, data...)
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc32.ChecksumIEEE(data))
	payload = append(payload, crcBuf[:]...)

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	// fsync the directory so the rename itself is durable.
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// loadSnapshot reads a snapshot from path. If the file does not exist, an
// empty snapshot is returned.
func loadSnapshot(path string) (*snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &snapshot{chains: make(map[string]*versionChain)}, nil
		}
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	if len(data) < 4 {
		return nil, ErrCorrupt
	}
	// Last 4 bytes are the CRC.
	crcOff := len(data) - 4
	body := data[:crcOff]
	stored := binary.BigEndian.Uint32(data[crcOff:])
	if stored != crc32.ChecksumIEEE(body) {
		return nil, ErrCorrupt
	}
	return decodeSnapshot(body)
}
