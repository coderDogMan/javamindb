# JavaMinDB

JavaMinDB is a tiny embeddable append-only key-value store written in pure Java. It is designed to be small enough to study while still treating persistence, file compatibility, recovery boundaries, and maintenance as real engineering concerns.

> Status: **v0.1 / Phase 1 — stable append-only storage engine**. This release defines the first public API and on-disk format. It is not yet a production-grade LSM database.

## v0.1 capabilities

- Pure Java, JDK 17+
- Byte-array `put`, `get`, and `delete`
- Append-only persistent data file
- In-memory key-to-offset index rebuilt on startup
- Explicit `sync()` persistence boundary
- Manual `merge()` that rewrites only live entries
- Versioned v1 file header
- Automatic migration of the Phase 0 headerless format
- Deterministic repair of one incomplete trailing record
- Fail-closed handling for malformed entry metadata
- Rejection of unsupported future file-format versions
- Exclusive database-directory lock to prevent concurrent writers
- Thread-safe operations within one process

## Build and test

```bash
mvn clean verify
```

CI runs the test suite on Java 17 and Java 21.

## Quick start

```java
import io.github.coderdogman.javamindb.MiniDB;

import java.nio.charset.StandardCharsets;

byte[] key = "name".getBytes(StandardCharsets.UTF_8);
byte[] value = "Alice".getBytes(StandardCharsets.UTF_8);

try (MiniDB db = MiniDB.open("./data")) {
    db.put(key, value);
    db.sync();

    byte[] stored = db.get(key); // null when absent
    db.delete(key);              // true only if the key existed
}
```

## API contract

The v0.1 API intentionally stays small:

- keys must be non-null and non-empty
- values must be non-null; empty values are valid
- `put` overwrites logically by appending a newer record
- `get` returns a copy of the stored bytes, or `null` if absent
- `delete` returns `true` only when a live key existed
- `size` counts live keys, not physical records
- `close` is idempotent
- operations after close throw `IllegalStateException`
- storage failures use `IOException` and documented subclasses
- one database directory can be owned by only one JavaMinDB instance at a time

See [docs/API.md](docs/API.md) for the complete contract.

## Storage and recovery model

```text
PUT / DELETE
     |
     v
versioned append-only data file
     |
     +----> in-memory key -> file offset index

open()  -> validates format + rebuilds index
merge() -> rewrites only live PUT records
sync()  -> fsyncs the current data file
```

The v1 format has a fixed file header followed by length-delimited records. On open, JavaMinDB may discard an incomplete final record by truncating to the last fully readable record. Invalid lengths, invalid operation codes, malformed headers, and unsupported file versions fail closed.

v0.1 does **not** checksum payload bytes, provide transactions, or claim WAL-level crash durability. Those are Phase 2 concerns.

See [docs/FILE_FORMAT.md](docs/FILE_FORMAT.md).

## Compatibility policy

- The first stable on-disk format is **format version 1**.
- Phase 0 headerless files are recognized and rewritten to v1 after a successful scan.
- Unknown newer versions are never guessed; they raise `UnsupportedFormatException`.
- Any future breaking on-disk change must use a new format version and document its migration path.
- Pre-1.0 Java API compatibility may still evolve, but breaking changes require release notes.

## Project direction

JavaMinDB is intentionally not positioned as a RocksDB replacement. The goal is a readable, testable, pure-Java storage engine that can evolve toward WAL semantics, MemTables, SSTables, checksums, Bloom filters, compaction, and repeatable benchmarks without hiding the mechanics.

See [ROADMAP.md](ROADMAP.md) and [CHANGELOG.md](CHANGELOG.md).

## Contributing

Issues, bug reports, design discussions, tests, documentation, and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

See [SECURITY.md](SECURITY.md).

## License

MIT License. See [LICENSE](LICENSE).
