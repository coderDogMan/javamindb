# Changelog

All notable user-visible changes are documented here.

## 0.3.0 — Phase 3

LSM-foundation release.

### Added

- Ordered in-memory `MemTable` using unsigned lexicographic byte-key ordering.
- Immutable MemTable snapshots and explicit `MiniDB.flush()`.
- Immutable generation-numbered SSTables under `sstables/`.
- CRC32C-protected SSTable records using the existing format-v2 entry encoding.
- In-memory sparse index sampled every 16 SSTable records.
- Newest-table-wins point lookup with tombstone shadowing.
- `MiniDB.entries()` ordered live-state snapshots through the public `KeyValue` type.
- `docs/LSM_FOUNDATION.md`.

### Changed

- `sync()` now checkpoints the canonical data/WAL state and flushes the current MemTable.
- Startup reconstructs the MemTable only from data-file sequences newer than the newest persisted SSTable.
- `merge()` removes stale SSTables and publishes one fresh live-state SSTable.
- Project version is `0.3.0`.

### Compatibility

- The Phase 2 WAL and canonical data-file durability contract remains authoritative.
- Existing Phase 0/v1 migration and format-v2 compatibility behavior is preserved.
- SSTables are derived sorted state in v0.3; Phase 4 will add compaction policy and read-amplification controls.

## 0.2.0 — Phase 2

Durability and crash-recovery release.

### Added

- CRC32C-protected format-v2 records.
- Positive monotonic sequence numbers for append/replay ordering.
- `minidb.wal` write-ahead log with WAL-first fsync ordering.
- Deterministic replay of WAL entries newer than the canonical data file.
- Incomplete final WAL-record repair.
- Fault-injection hooks used by the regression suite.
- `docs/DURABILITY.md`.

### Changed

- Successful `put`/`delete` now cross a WAL fsync boundary before returning.
- `sync()` is now a checkpoint: force data, then reset WAL.
- `close()` checkpoints normally; after an ambiguous persistence error it preserves WAL for recovery.
- `merge()` emits format v2 and resets WAL after durable replacement.
- Phase 0 and v1 files migrate directly to format v2.
- Project version is `0.2.0`.

### Recovery behavior

- Partial data tails are truncated, then missing committed records can be restored from WAL.
- Partial WAL tails are truncated.
- CRC32C mismatch fails closed rather than being silently repaired.
- Complete duplicate WAL records are skipped by sequence number.

## 0.1.0 — Phase 1

First defined pre-1.0 storage-engine release.

### Added

- Versioned on-disk file format v1.
- Automatic migration of Phase 0 headerless data files.
- Exclusive database-directory locking.
- `MiniDB.sync()` as an explicit fsync boundary.
- Public storage exceptions for corruption, unsupported formats, and lock conflicts.
- API and file-format documentation.
- Tag-driven GitHub release workflow.

### Recovery behavior

- A physically incomplete final record is discarded by truncating to the last complete record.
- Structural corruption is not auto-repaired.
