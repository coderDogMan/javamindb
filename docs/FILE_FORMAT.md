# JavaMinDB file format

This document defines the first versioned JavaMinDB on-disk format.

All integer fields use Java `ByteBuffer` default byte order: big-endian.

## File names

- `minidb.data` — canonical append-only data file
- `minidb.data.merge` — temporary file used during `merge()`
- `.javamindb.lock` — directory lock file; its presence alone does not mean the DB is locked

## Format v1 file header

The file starts with a fixed 16-byte header:

| Offset | Size | Field | Value |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `JMINIDB\0` |
| 8 | 4 | format version | `1` |
| 12 | 4 | file header size | `16` |

A file with JavaMinDB magic but an unsupported version is rejected with `UnsupportedFormatException`.

## Record layout

Immediately after the file header, records are concatenated with no padding:

| Size | Field |
| ---: | --- |
| 4 | key length |
| 4 | value length |
| 2 | operation |
| key length | key bytes |
| value length | value bytes |

Operations:

- `1` = PUT
- `2` = DELETE

A DELETE record must have `value length = 0`.

The current component limits are exposed as `MiniDB.MAX_KEY_SIZE` and `MiniDB.MAX_VALUE_SIZE`.

## Authoritative state

Records are applied in file order. The latest PUT for a key is authoritative unless followed by DELETE. The in-memory index stores offsets only for currently live keys.

`merge()` rewrites one PUT per live key, so record history is not part of the compatibility contract.

## Truncation behavior

If the file ends before a final record header or payload is physically complete, v0.1 treats that final record as an incomplete tail. `open()` truncates the file back to the beginning of that record and keeps the valid prefix.

This repair applies only to incomplete EOF tails. It does not repair malformed sizes, unknown operations, impossible DELETE records, or malformed file headers.

## Corruption boundary in v0.1

v0.1 validates structure but has no per-record payload checksum. Therefore it can detect malformed metadata and truncation but cannot reliably detect every bit flip that leaves lengths and operation fields valid.

Phase 2 is responsible for record checksums/CRC and stronger crash-recovery guarantees.

## Phase 0 compatibility

Phase 0 stored the same 10-byte record header and payloads but had no file header. When v0.1 opens a valid headerless Phase 0 file:

1. it scans/rebuilds the logical state,
2. repairs an incomplete final record if present,
3. rewrites the live state into a fresh format-v1 file.

This migration is one-way. Future breaking layouts must increment the format version and document an explicit migration path.
