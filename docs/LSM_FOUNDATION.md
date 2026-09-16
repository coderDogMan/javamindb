# Phase 3 LSM foundation

JavaMinDB v0.3 introduces the first sorted-storage layer while retaining the Phase 2 append-only data file and WAL as the recovery authority.

## Write path

```text
put/delete
  -> WAL append + fsync
  -> canonical data append
  -> live-key index update
  -> ordered MemTable update
```

The MemTable is a `TreeMap` keyed by unsigned lexicographic byte order and retains only the newest mutation for each key.

## Flush

`flush()` freezes the current MemTable into an immutable snapshot and writes it as a new SSTable under `sstables/`. `sync()` performs the Phase 2 data/WAL checkpoint and then flushes the MemTable.

Each SSTable:

- is immutable after publication,
- stores keys in strict unsigned lexicographic order,
- stores the existing format-v2 CRC32C-protected records,
- records its generation and maximum sequence in a fixed header,
- builds an in-memory sparse index every 16 records when opened.

The sparse index chooses a nearby starting offset and point lookup scans forward until the target key is found or passed.

## Read path

```text
MemTable
   -> newest SSTable ... oldest SSTable
   -> canonical data-file index fallback
```

A tombstone found in a newer layer hides values in older layers.

## Restart behavior

SSTables record the largest sequence they contain. After canonical data/WAL recovery, JavaMinDB rebuilds the MemTable only from data-file records whose sequence is newer than the newest persisted SSTable sequence. This avoids rebuilding all sorted state on every restart.

## Ordered iteration

`entries()` merges SSTables and the MemTable by key and sequence, drops tombstones, and returns a stable key-ordered snapshot of live `KeyValue` entries.

## Merge interaction

`merge()` remains the full-state rewrite operation for the canonical data file. It also removes stale SSTables, rebuilds the current live state into the MemTable, and flushes one fresh SSTable so deleted keys cannot be resurrected by an older table.

## Intentional Phase 3 boundaries

Phase 3 does not yet implement leveled/size-tiered background compaction, Bloom filters, block caching, or an SSTable manifest. The append-only data file is still retained as the recovery authority. Phase 4 will focus on read amplification and compaction policy.
