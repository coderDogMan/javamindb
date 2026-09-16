# JavaMinDB v0.3 API contract

This document defines the public behavior of the v0.3 LSM-foundation engine.

## Opening

```java
MiniDB.open(Path directory)
MiniDB.open(String directory)
```

Opening creates the directory if needed and takes an exclusive file lock. It validates/migrates the canonical data file, repairs only physically incomplete final records, rebuilds the live-key index, validates/replays the WAL, opens persisted SSTables, and reconstructs the MemTable from canonical records newer than the newest SSTable sequence.

## Keys and values

The API is binary.

- key: non-null, non-empty, at most `MiniDB.MAX_KEY_SIZE`
- value: non-null, may be empty, at most `MiniDB.MAX_VALUE_SIZE`
- caller arrays are copied
- `get()` and `KeyValue` accessors return copies

Invalid input throws `IllegalArgumentException` before mutation.

## `put`

```java
void put(byte[] key, byte[] value) throws IOException
```

Write ordering is:

1. append a sequence-numbered, CRC32C-protected WAL record,
2. fsync the WAL,
3. append the same record to the canonical data file,
4. update the live-key index,
5. update the ordered MemTable.

When `put()` returns successfully, the mutation has crossed the WAL durability boundary.

## `delete`

```java
boolean delete(byte[] key) throws IOException
```

An existing key gets a WAL-protected DELETE tombstone and returns `true`. An absent key is a no-op and returns `false`. Tombstones in newer MemTables/SSTables shadow values in older SSTables.

## `get`

```java
byte[] get(byte[] key) throws IOException
```

The read path checks the current MemTable, then SSTables from newest to oldest, then the canonical data-file index as a compatibility/recovery fallback. Returns the latest live value or `null`.

## `size`

```java
long size()
```

Returns live-key count.

## `flush`

```java
void flush() throws IOException
```

Freezes the current ordered MemTable and writes one immutable SSTable. `flush()` does not replace the WAL durability contract; the canonical data/WAL path remains authoritative in v0.3.

## `sync`

```java
void sync() throws IOException
```

Forces the canonical data file, resets the WAL, and flushes the current MemTable into an SSTable. Successful writes do not require `sync()` to be recoverable because the WAL is forced before the write returns.

## `entries`

```java
List<KeyValue> entries() throws IOException
```

Returns a stable snapshot of currently live entries ordered by unsigned lexicographic key bytes. Newer sequence numbers override older SSTable records and tombstones are removed from the returned snapshot.

## `merge`

```java
void merge() throws IOException
```

Rewrites only live PUT records into a fresh format-v2 canonical data file, resets the WAL, removes stale SSTables, reconstructs the current live state, and publishes one fresh SSTable. Sequence numbers of surviving entries are preserved.

## I/O failures and ambiguous completion

If a persistence operation throws after a WAL record may have been forced, the mutation may already be durable. The current `MiniDB` instance enters a recovery-required state: further data operations fail with `IllegalStateException`. Close it and reopen the directory; canonical data/WAL recovery determines the authoritative durable result.

## `close`

`close()` is idempotent. Under normal conditions it checkpoints and flushes pending MemTable state before releasing SSTables, WAL/data files, and the directory lock. If a prior persistence error put the instance into recovery-required state, close preserves recovery information instead of clearing it.

## Error model

- `IllegalArgumentException`: invalid input
- `IllegalStateException`: closed instance or reopen required after a persistence failure
- `DatabaseLockedException`: directory already owned
- `CorruptDatabaseException`: checksum or structural corruption in canonical data, WAL, or SSTables
- `UnsupportedFormatException`: unsupported canonical data/WAL format
- other `IOException`: filesystem/I/O failure

## Threading/process model

A single instance is protected by a read/write lock. Only one JavaMinDB instance may own a database directory at a time.
