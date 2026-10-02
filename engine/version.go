package engine

// Version is one committed point in a key's immutable version chain.
//
// Chains are ordered from newest to oldest. Values are owned by the chain and
// must not be mutated after the version has been published.
type Version struct {
	TxnID    uint64
	CommitTS uint64
	Value    []byte
	Deleted  bool
}

// visibleValue returns the committed value visible to a snapshot at readTS.
//
// A version is visible only when its commit timestamp is not newer than the
// snapshot. Versions of transactions that were still running when the snapshot
// began therefore never become visible later to that transaction.
func visibleValue(chain []Version, readTS uint64) ([]byte, bool) {
	for _, version := range chain {
		if version.CommitTS <= readTS {
			if version.Deleted {
				return nil, false
			}
			// Copy so callers cannot mutate immutable MVCC history.
			value := make([]byte, len(version.Value))
			copy(value, version.Value)
			return value, true
		}
	}
	return nil, false
}

// compactChain removes versions strictly older than safeReadTS. A tombstone is
// retained while some retained snapshot may need it. Once every active snapshot
// is newer than the deletion, an absent key represents the same visible state.
func compactChain(chain []Version, safeReadTS uint64) []Version {
	if len(chain) == 0 {
		return chain
	}
	if chain[0].Deleted && chain[0].CommitTS <= safeReadTS {
		return nil
	}
	if len(chain) == 1 {
		return chain
	}

	kept := make([]Version, 0, len(chain))
	for index, version := range chain {
		if index == 0 || version.CommitTS >= safeReadTS {
			kept = append(kept, version)
		}
	}
	return kept
}
