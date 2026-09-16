# Changelog

All notable user-visible changes are documented here.

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
