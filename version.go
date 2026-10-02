package mvcc

// Version is a single immutable revision of a key.
//
// A version is visible to a snapshot with timestamp ts iff
//
//	version.BeginTS <= ts && (version.EndTS == 0 || ts < version.EndTS)
//
// where EndTS == 0 means the version is still the latest revision
// (its validity interval is [BeginTS, +inf)).
type Version struct {
	BeginTS   uint64 // commit timestamp of the transaction that created this version
	EndTS     uint64 // commit timestamp of the transaction that invalidated it; 0 == still live
	TxnID     uint64 // id of the transaction that created this version
	Tombstone bool   // true if this version represents a deletion
	Value     []byte // value bytes; nil for tombstones
}

// isLive reports whether the version is still the latest revision of its key.
func (v *Version) isLive() bool { return v.EndTS == 0 }

// visibleAt reports whether the version is visible to a snapshot taken at ts.
func (v *Version) visibleAt(ts uint64) bool {
	if v.BeginTS > ts {
		return false
	}
	if v.EndTS != 0 && ts >= v.EndTS {
		return false
	}
	return true
}

// versionChain is the list of revisions of a single key, ordered by
// BeginTS descending (newest first). Revisions have disjoint validity
// intervals.
//
// versionChain values are treated as immutable: every mutation returns a new
// chain, so a snapshot captured by a read-only transaction can be read without
// holding any lock while writers concurrently install new revisions.
type versionChain struct {
	versions []Version
}

// latest returns the newest version, or nil if the chain has no versions.
func (c *versionChain) latest() *Version {
	if len(c.versions) == 0 {
		return nil
	}
	return &c.versions[0]
}

// visibleAt returns the version visible to a snapshot at ts, or nil.
func (c *versionChain) visibleAt(ts uint64) *Version {
	for i := range c.versions {
		v := &c.versions[i]
		if v.BeginTS <= ts {
			if v.EndTS == 0 || ts < v.EndTS {
				return v
			}
			// versions are ordered by BeginTS descending; once BeginTS <= ts
			// but EndTS <= ts, older versions may still be visible, so keep
			// scanning.
		}
	}
	return nil
}

// committed returns a new chain with v added as the latest version and the
// previous latest invalidated at commitTS. The receiver is not modified.
func (c *versionChain) committed(v Version, commitTS uint64) *versionChain {
	newVersions := make([]Version, 0, len(c.versions)+1)
	newVersions = append(newVersions, v)
	for i, old := range c.versions {
		if i == 0 {
			old.EndTS = commitTS
		}
		newVersions = append(newVersions, old)
	}
	return &versionChain{versions: newVersions}
}

// reclaimed returns a new chain with versions whose EndTS <= threshold
// removed. The receiver is not modified. The live (EndTS == 0) version is
// never removed.
func (c *versionChain) reclaimed(threshold uint64) *versionChain {
	if threshold == 0 {
		return c
	}
	newVersions := make([]Version, 0, len(c.versions))
	for i := range c.versions {
		v := &c.versions[i]
		if v.EndTS != 0 && v.EndTS <= threshold {
			continue
		}
		newVersions = append(newVersions, *v)
	}
	return &versionChain{versions: newVersions}
}
