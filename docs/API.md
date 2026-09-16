# JavaMinDB v0.1 API contract

This document defines the public behavior that callers can rely on for v0.1.

## Opening a database

```java
MiniDB.open(Path directory)
MiniDB.open(String directory)
```

Opening creates the directory when needed and takes an exclusive file lock in that directory. A second JavaMinDB instance cannot open the same directory until the first closes.

Opening also validates the data-file header, repairs a physically incomplete trailing record when safe, rebuilds the in-memory index, and migrates a valid Phase 0 headerless file to format v1.

## Keys and values

The storage API is binary. JavaMinDB does not apply text encodings.

- key: non-null, non-empty, at most `MiniDB.MAX_KEY_SIZE`
- value: non-null, may be empty, at most `MiniDB.MAX_VALUE_SIZE`
- caller-owned input arrays are copied before they become persistent/index state
- values returned by `get` are copies

Invalid arguments throw `IllegalArgumentException` before storage mutation.

## `put`

```java
void put(byte[] key, byte[] value) throws IOException
```

`put` appends a new PUT record. If the key already exists, the newer record becomes authoritative. It does not implicitly call `sync()`.

## `get`

```java
byte[] get(byte[] key) throws IOException
```

Returns the latest live value, or `null` when the key is absent. An empty stored value is returned as a zero-length byte array and is distinct from absence.

## `delete`

```java
boolean delete(byte[] key) throws IOException
```

When the key exists, `delete` appends a tombstone, removes the key from the live index, and returns `true`. Deleting an absent key is a no-op and returns `false`.

## `size`

```java
long size()
```

Returns the count of live keys. It does not report physical record count or file size.

## `sync`

```java
void sync() throws IOException
```

Requests an fsync of the current append-only data file. In v0.1 this is an explicit file persistence boundary only. It is not a transaction commit and does not provide WAL semantics.

## `merge`

```java
void merge() throws IOException
```

Rewrites only the latest live PUT records into a fresh format-v1 file and atomically replaces the old file when the platform supports atomic moves. The replacement file is synced before the swap.

## `close`

`close()` is idempotent. It releases both the data file and the exclusive directory lock. Data operations after close throw `IllegalStateException`.

## Threading and process model

A single `MiniDB` instance is thread-safe through a read/write lock. v0.1 intentionally permits only one open JavaMinDB instance per database directory, including within the same JVM.

## Error model

- `IllegalArgumentException`: invalid API input
- `IllegalStateException`: operation on a closed instance
- `DatabaseLockedException`: another instance owns the directory
- `CorruptDatabaseException`: recognized file structure is invalid
- `UnsupportedFormatException`: file uses a newer/unknown JavaMinDB format version
- other `IOException`: underlying filesystem or I/O failure

JavaMinDB does not reinterpret structural corruption as an empty database.
