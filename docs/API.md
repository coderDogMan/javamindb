# JavaMinDB v0.2 API contract

This document defines the public behavior of the v0.2 append-only engine.

## Opening

```java
MiniDB.open(Path directory)
MiniDB.open(String directory)
```

Opening creates the directory if needed and takes an exclusive file lock. It validates/migrates the data file, repairs only physically incomplete final records, rebuilds the in-memory index, validates the WAL, replays WAL records newer than the data-file high-water sequence, checkpoints recovered data, and resets the WAL.

## Keys and values

The API is binary.

- key: non-null, non-empty, at most `MiniDB.MAX_KEY_SIZE`
- value: non-null, may be empty, at most `MiniDB.MAX_VALUE_SIZE`
- caller arrays are copied
- `get()` returns a copy

Invalid input throws `IllegalArgumentException` before mutation.

## `put`

```java
void put(byte[] key, byte[] value) throws IOException
```

Write ordering is:

1. append a sequence-numbered, CRC32C-protected WAL record,
2. fsync the WAL,
3. append the same record to the data file,
4. update the in-memory index.

When `put()` returns successfully, the mutation has crossed the WAL durability boundary.

## `delete`

```java
boolean delete(byte[] key) throws IOException
```

An existing key gets a WAL-protected DELETE tombstone and returns `true`. An absent key is a no-op and returns `false`.

## `get`

```java
byte[] get(byte[] key) throws IOException
```

Returns the latest live value or `null`.

## `size`

```java
long size()
```

Returns live-key count.

## `sync`

```java
void sync() throws IOException
```

Checkpoints the current state by forcing `minidb.data` and then resetting the WAL. Successful writes do not require `sync()` to be recoverable because the WAL is forced before the write returns.

## `merge`

```java
void merge() throws IOException
```

Rewrites only live PUT records into a fresh format-v2 data file, forces it, replaces the canonical data file, and resets the WAL. Sequence numbers of surviving entries are preserved.

## I/O failures and ambiguous completion

If a persistence operation throws after a WAL record may have been forced, the mutation may already be durable. The current `MiniDB` instance enters a recovery-required state: further data operations fail with `IllegalStateException`. Close it and reopen the directory; WAL recovery determines the authoritative durable result.

## `close`

`close()` is idempotent. Under normal conditions it checkpoints before releasing files and the directory lock. If a prior persistence error put the instance into recovery-required state, close preserves the WAL instead of clearing it so the next open can recover.

## Error model

- `IllegalArgumentException`: invalid input
- `IllegalStateException`: closed instance or reopen required after a persistence failure
- `DatabaseLockedException`: directory already owned
- `CorruptDatabaseException`: checksum or structural corruption
- `UnsupportedFormatException`: unsupported data/WAL format
- other `IOException`: filesystem/I/O failure

## Threading/process model

A single instance is protected by a read/write lock. Only one JavaMinDB instance may own a database directory at a time.
