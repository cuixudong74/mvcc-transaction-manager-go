package engine

// GCStats describes one version-chain compaction.
type GCStats struct {
	RemovedVersions uint64
	ChainsBefore    int
	ChainsAfter     int
	SafeReadTS      uint64
}

// RunGC rewrites the WAL snapshot and removes versions older than the oldest
// active transaction snapshot. It never blocks readers during the file copy and
// fsync phase; commits are serialized through the same commit gate.
func (e *Engine) RunGC() (GCStats, error) {
	e.commitMu.Lock()
	defer e.commitMu.Unlock()

	e.mu.RLock()
	if !e.available() {
		e.mu.RUnlock()
		return GCStats{}, ErrEngineClosed
	}

	safeReadTS := e.nextCommitTS - 1
	for _, txn := range e.active {
		if txn.readTS < safeReadTS {
			safeReadTS = txn.readTS
		}
	}

	snapshot := walSnapshot{
		NextTxID:     e.nextTxID,
		NextCommitTS: e.nextCommitTS,
		Versions:     make(map[string][]Version, len(e.versions)),
		Active:       make(map[uint64]activeSnapshot),
	}
	versionsBefore := 0
	chainsBefore := len(e.versions)
	for key, chain := range e.versions {
		versionsBefore += len(chain)
		compacted := deepCopyVersions(compactChain(chain, safeReadTS))
		if len(compacted) > 0 {
			snapshot.Versions[key] = compacted
		}
	}
	e.mu.RUnlock()

	if err := e.wal.rewrite(snapshot); err != nil {
		e.markFatal()
		return GCStats{}, err
	}

	e.mu.Lock()
	versionsAfter := 0
	compacted := make(map[string][]Version, len(snapshot.Versions))
	for key, chain := range snapshot.Versions {
		compacted[key] = deepCopyVersions(chain)
		versionsAfter += len(chain)
	}
	e.versions = compacted
	e.removedVersions += uint64(versionsBefore - versionsAfter)
	e.gcRuns++
	stats := GCStats{
		RemovedVersions: uint64(versionsBefore - versionsAfter),
		ChainsBefore:    chainsBefore,
		ChainsAfter:     len(snapshot.Versions),
		SafeReadTS:      safeReadTS,
	}
	e.mu.Unlock()
	return stats, nil
}

func deepCopyVersions(input []Version) []Version {
	output := make([]Version, len(input))
	for i, version := range input {
		output[i] = Version{
			TxnID:    version.TxnID,
			CommitTS: version.CommitTS,
			Value:    append([]byte(nil), version.Value...),
			Deleted:  version.Deleted,
		}
	}
	return output
}
