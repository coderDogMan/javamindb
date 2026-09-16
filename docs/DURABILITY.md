# Durability and crash recovery

JavaMinDB v0.2 uses WAL-first ordering.

## Mutation protocol

For each successful PUT/DELETE:

```text
1. build record with monotonically increasing sequence
2. append record to minidb.wal
3. fsync minidb.wal
4. append record to minidb.data
5. update in-memory index
6. return
```

The durability claim is deliberately narrow: after a successful mutation returns, the WAL has been forced. The data file may not yet have been checkpointed.

## Checkpoint protocol

`sync()` performs:

```text
1. fsync minidb.data
2. truncate/reset minidb.wal to its header
3. fsync the WAL reset
```

If a crash occurs after step 1 but before step 2, recovery sees both copies and skips the WAL duplicate by sequence number.

## Open/recovery protocol

1. validate/migrate the data file,
2. scan data and rebuild the live index,
3. truncate only a physically incomplete final data record,
4. validate the WAL,
5. truncate only a physically incomplete final WAL record,
6. replay WAL records newer than the data-file high-water sequence,
7. fsync replayed data,
8. reset the WAL.

## Failure semantics

A persistence method can fail after the WAL durability boundary. Therefore an exception does not always mean "the mutation definitely did not happen." After such an error the instance refuses further data operations and must be closed/reopened.

This is intentional: reopening provides one deterministic place to reconcile WAL and data state.

## Fault-injection coverage

The test suite injects failures at these ordering boundaries:

- after WAL fsync / before data append,
- after data append / before index update,
- after data fsync / before WAL reset.

Tests also cover:

- truncated data tails recovered through WAL replay,
- truncated WAL tails,
- data-record checksum corruption,
- WAL-record checksum corruption,
- v1 and Phase 0 migration.

## Non-goals in v0.2

- transactions or multi-key atomicity
- group commit
- configurable sync modes
- cryptographic integrity
- multi-process readers/writers
- SSTables or MemTables

Those concerns remain outside the Phase 2 contract.
