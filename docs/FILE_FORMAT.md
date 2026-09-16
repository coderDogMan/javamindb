# JavaMinDB file formats

All integer fields use Java `ByteBuffer` big-endian byte order.

## Files

- `minidb.data` — canonical append-only data file
- `minidb.wal` — write-ahead log
- `minidb.data.merge` — temporary merge/migration file
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

DELETE must have zero value length. Sequence numbers are positive and strictly increase in physical append order.

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

## Recovery rules

- A physically incomplete final data record is truncated to its starting offset.
- A physically incomplete final WAL record is truncated to its starting offset.
- CRC32C mismatch is corruption and fails closed.
- malformed sizes, operations, sequence numbers, file headers, or WAL headers fail closed.
- complete WAL entries with sequence numbers newer than the data-file high-water sequence are replayed.
- WAL entries already represented by the data file are skipped, making recovery idempotent.

## Migration

### Phase 0 -> v2

Headerless 10-byte-record files are scanned using the legacy layout. After successful scan, only current live state is rewritten as checksummed v2 records.

### v1 -> v2

v1 files have the versioned 16-byte header but use the old 10-byte record header with no sequence or CRC. They are scanned, then live state is rewritten to v2 with fresh positive sequences.

Migration is one-way.

## What CRC does and does not guarantee

CRC32C detects accidental record corruption with high probability. It is not cryptographic authentication. A valid checksum does not make an untrusted file safe or prove who wrote it.
