# Changelog

All notable user-visible changes are documented here.

## 0.1.0 — Phase 1

First defined pre-1.0 storage-engine release.

### Added

- Versioned on-disk file format v1.
- Automatic migration of Phase 0 headerless data files.
- Exclusive database-directory locking.
- `MiniDB.sync()` as an explicit fsync boundary.
- Public storage exceptions for corruption, unsupported formats, and lock conflicts.
- API and file-format documentation.
- Regression coverage for API semantics, legacy migration, truncation repair, structural corruption, unsupported versions, locking, merge, reopen, and byte-array isolation.
- Tag-driven GitHub release workflow.

### Changed

- `size()` and all data operations now fail with `IllegalStateException` after close.
- Entry decoding fails closed on invalid key/value sizes, operation codes, or invalid delete records.
- `merge()` always emits the v1 file format and fsyncs the replacement file before swap.
- Project version changed from `0.1.0-SNAPSHOT` to `0.1.0`.

### Recovery behavior

- A physically incomplete final record is discarded by truncating the file to the last complete record.
- Structural corruption is not auto-repaired.
- Payload checksums and WAL-level crash guarantees are intentionally deferred to Phase 2.
