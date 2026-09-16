# JavaMinDB

JavaMinDB is a tiny embeddable log-structured key-value store written in pure Java. It is intentionally small enough to study while treating persistence, compatibility, corruption detection, crash recovery, sorted storage, and maintenance as real storage-engine concerns.

> Status: **v0.3 / Phase 3 — LSM foundation**. JavaMinDB now combines the Phase 2 WAL/data-file durability path with an ordered MemTable, immutable SSTables, sparse indexing, and ordered live-state iteration. It remains a pre-1.0 educational/lightweight engine, not a RocksDB replacement.

## v0.3 capabilities

- Pure Java, JDK 17+
- Binary `put`, `get`, and `delete`
- CRC32C-protected write-ahead log and canonical data file
- Deterministic WAL replay after interrupted writes/checkpoints
- Ordered in-memory MemTable
- Immutable MemTable flush through `flush()`
- Generation-numbered SSTables under `sstables/`
- Sparse in-memory SSTable index sampled every 16 records
- Newest-layer-wins point lookup with tombstone shadowing
- Ordered live-state snapshots through `entries()`
- `sync()` checkpoint plus MemTable flush
- `merge()` full-state rewrite plus stale-SSTable replacement
- Automatic migration from Phase 0 headerless files and v0.1 format-v1 files
- Exclusive database-directory lock
- Java 17/21 CI and fault-injection recovery tests

## Build and test

```bash
mvn clean verify
```

## Quick start

```java
import io.github.coderdogman.javamindb.KeyValue;
import io.github.coderdogman.javamindb.MiniDB;

import java.nio.charset.StandardCharsets;

byte[] key = "name".getBytes(StandardCharsets.UTF_8);
byte[] value = "Alice".getBytes(StandardCharsets.UTF_8);

try (MiniDB db = MiniDB.open("./data")) {
    db.put(key, value); // WAL is forced before this returns successfully
    db.flush();         // freeze current MemTable into an immutable SSTable

    byte[] stored = db.get(key);

    for (KeyValue item : db.entries()) {
        System.out.println(new String(item.key(), StandardCharsets.UTF_8));
    }

    db.sync();          // checkpoint canonical data/WAL state and flush pending MemTable state
}
```

## Current storage architecture

```text
put/delete
    |
    v
CRC32C WAL + fsync
    |
    v
append-only canonical data file
    |
    +----> live-key offset index
    |
    +----> ordered MemTable
              |
              | flush()
              v
        immutable SSTables
        + sparse index

get()
    -> MemTable
    -> newest SSTable ... oldest SSTable
    -> canonical data-file fallback
```

The append-only data file and WAL remain the recovery authority in v0.3. SSTables are the first persistent sorted layer and establish the base for Phase 4 compaction and read-amplification work.

See [docs/LSM_FOUNDATION.md](docs/LSM_FOUNDATION.md), [docs/DURABILITY.md](docs/DURABILITY.md), [docs/API.md](docs/API.md), and [docs/FILE_FORMAT.md](docs/FILE_FORMAT.md).

## Compatibility policy

- Phase 0 headerless files and v0.1 format-v1 files are accepted and rewritten to format v2 after a successful scan.
- Format v2 carries monotonic sequence numbers and CRC32C.
- SSTables use their own versioned header and reuse format-v2 record encoding.
- Unknown newer canonical formats fail with `UnsupportedFormatException`.
- Breaking disk changes require a new format version plus a documented migration path.
- Java API compatibility may still evolve before 1.0; breaking changes require release notes.

## Project direction

Phase 4 focuses on Bloom filters, compaction policy, tombstone lifecycle, and reducing read amplification. Later phases add repeatable benchmarking, Maven Central publication, examples, and real-world adoption signals.

See [ROADMAP.md](ROADMAP.md) and [CHANGELOG.md](CHANGELOG.md).

## Contributing

Issues, bug reports, recovery edge cases, storage-format discussions, tests, documentation, and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

See [SECURITY.md](SECURITY.md).

## License

MIT License. See [LICENSE](LICENSE).
