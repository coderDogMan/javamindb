# JavaMinDB file formats

All integer fields use Java `ByteBuffer` big-endian byte order.

## Files

- `minidb.data` — canonical append-only data file
- `minidb.wal` — write-ahead log
- `minidb.data.merge` — temporary merge/migration file
- `sstables/sst-<generation>.sst` — immutable sorted-table files introduced in v0.3
- `.javamindb.lock` — directory lock file

## Data file header

The 16-byte header remains:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `JMINIDB\0` |
| 8 | 4 | format version |
| 12 | 4 | header size (`16`) |

Current format version: **2**.

Format v1 is accepted only as migration input. Phase 0 headerless files are also accepted as migration input.

## Format-v2 data record

| Size | Field |
| ---: | --- |
| 4 | key length |
| 4 | value length |
| 2 | operation |
| 8 | sequence number |
| 4 | CRC32C |
| key length | key bytes |
| value length | value bytes |

Operations:

- `1` = PUT
- `2` = DELETE

DELETE must have zero value length. Sequence numbers are positive and strictly increase in canonical data-file append order.

CRC32C covers, in order:

```text
key length || value length || operation || sequence || key bytes || value bytes
```

The checksum itself is not included.

## WAL format

`minidb.wal` starts with a fixed 16-byte header:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `JMWALDB\0` |
| 8 | 4 | WAL version (`1`) |
| 12 | 4 | header size (`16`) |

WAL records use the same format-v2 record encoding as the data file.

## SSTable format

Each Phase 3 SSTable starts with a fixed 32-byte header:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `JMINSST1` |
| 8 | 4 | SSTable version (`1`) |
| 12 | 4 | header size (`32`) |
| 16 | 8 | positive generation number |
| 24 | 8 | maximum record sequence in this table |

The header is followed by format-v2 records sorted strictly by unsigned lexicographic key order. Unlike the canonical append-only file, physical sequence numbers inside an SSTable do not need to be increasing because key order is the primary physical order.

SSTables are immutable once published. On open, JavaMinDB scans the table, validates every record CRC, validates strict key ordering, and builds an in-memory sparse index sampled every 16 records. The sparse index is derived state and is not stored on disk in v0.3.

## Recovery rules

- A physically incomplete final canonical data record is truncated to its starting offset.
- A physically incomplete final WAL record is truncated to its starting offset.
- CRC32C mismatch is corruption and fails closed.
- malformed sizes, operations, sequence numbers, file headers, WAL headers, or SSTable headers fail closed.
- complete WAL entries with sequence numbers newer than the data-file high-water sequence are replayed.
- WAL entries already represented by the data file are skipped, making recovery idempotent.
- SSTables are derived sorted state; canonical data + WAL remain the recovery authority in v0.3.
- an SSTable whose high-water sequence is newer than recovered canonical state is rejected as inconsistent.

## Migration

### Phase 0 -> v2

Headerless 10-byte-record files are scanned using the legacy layout. After successful scan, only current live state is rewritten as checksummed v2 records.

### v1 -> v2

v1 files have the versioned 16-byte header but use the old 10-byte record header with no sequence or CRC. They are scanned, then live state is rewritten to v2 with fresh positive sequences.

Migration is one-way.

## What CRC does and does not guarantee

CRC32C detects accidental record corruption with high probability. It is not cryptographic authentication. A valid checksum does not make an untrusted file safe or prove who wrote it.
