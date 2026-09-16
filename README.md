# JavaMinDB

JavaMinDB is a tiny embeddable log-structured key-value store written in pure Java. It is intentionally small enough to study while treating persistence, compatibility, corruption detection, and crash recovery as real storage-engine concerns.

> Status: **v0.2 / Phase 2 — WAL durability and crash recovery**. JavaMinDB now provides a CRC32C-protected write-ahead log and deterministic recovery for interrupted append/checkpoint paths. It is still a pre-1.0 educational/lightweight engine, not a RocksDB replacement.

## v0.2 capabilities

- Pure Java, JDK 17+
- Binary `put`, `get`, and `delete`
- Append-only canonical data file
- CRC32C per record
- Write-ahead log (`minidb.wal`)
- WAL-first write ordering: WAL append -> WAL fsync -> data append -> index update
- Deterministic replay of committed WAL records on `open()`
- Idempotent recovery by monotonic record sequence number
- Repair of physically incomplete final data/WAL records
- Fail-closed handling for checksum failures and structural corruption
- Explicit `sync()` checkpoint: fsync data -> reset WAL
- Manual `merge()` that rewrites only live state
- Automatic migration from Phase 0 headerless files and v0.1 format-v1 files
- Exclusive database-directory lock
- Thread-safe operations within one process
- Fault-injection tests for the major persistence ordering boundaries

## Build and test

```bash
mvn clean verify
```

CI runs on Java 17 and Java 21.

## Quick start

```java
import io.github.coderdogman.javamindb.MiniDB;

import java.nio.charset.StandardCharsets;

byte[] key = "name".getBytes(StandardCharsets.UTF_8);
byte[] value = "Alice".getBytes(StandardCharsets.UTF_8);

try (MiniDB db = MiniDB.open("./data")) {
    db.put(key, value); // WAL is forced before this returns successfully

    byte[] stored = db.get(key);
    db.sync();          // checkpoint current state into minidb.data and reset the WAL
}
```

## Durability model

```text
put/delete
    |
    v
append WAL record (CRC32C + sequence)
    |
    v
fsync WAL
    |
    v
append data record
    |
    v
update in-memory index

open()
    |
    +--> validate/repair final physical tails
    +--> rebuild data index
    +--> replay WAL entries whose sequence is newer than data
    +--> fsync recovered data
    +--> reset WAL
```

A successful mutation is WAL-durable. `sync()` is a checkpoint boundary, not a transaction API. If a persistence method throws after the WAL may already have been forced, the database requires close/reopen; recovery determines the durable state.

See [docs/DURABILITY.md](docs/DURABILITY.md), [docs/API.md](docs/API.md), and [docs/FILE_FORMAT.md](docs/FILE_FORMAT.md).

## Compatibility policy

- Phase 0 headerless files and v0.1 format-v1 files are accepted and rewritten to format v2 after a successful scan.
- Format v2 adds record sequence numbers and CRC32C.
- Unknown newer formats fail with `UnsupportedFormatException`.
- Breaking disk changes require a new format version plus a documented migration path.
- Java API compatibility may still evolve before 1.0; breaking changes require release notes.

## Project direction

Phase 3 moves from the single append-only file toward an actual LSM foundation: MemTable, immutable flush, SSTables, sparse indexing, and ordered iteration.

See [ROADMAP.md](ROADMAP.md) and [CHANGELOG.md](CHANGELOG.md).

## Contributing

Issues, bug reports, recovery edge cases, tests, documentation, and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

See [SECURITY.md](SECURITY.md).

## License

MIT License. See [LICENSE](LICENSE).
