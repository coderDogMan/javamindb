# JavaMinDB

JavaMinDB is a tiny embeddable log-structured key-value store written in pure Java. The project started as a small append-only storage prototype and is being revived as a readable, tested open-source storage engine for learning, prototyping, and lightweight JVM applications.

> Status: **Phase 0 — project revival**. The current engine is intentionally small and is not yet a production-ready LSM database.

## Current capabilities

- Append-only persistent data file
- `put`, `get`, and `delete`
- In-memory key-to-offset index rebuilt on startup
- Manual merge/compaction of stale entries
- Read/write locking for process-local concurrency
- Reopen/recovery of the in-memory index from the data file

## Requirements

- JDK 17+
- Maven 3.9+

## Build and test

```bash
mvn clean verify
```

## Quick start

```java
import io.github.coderdogman.javamindb.MiniDB;

import java.nio.charset.StandardCharsets;

try (MiniDB db = MiniDB.open("./data")) {
    db.put("name".getBytes(StandardCharsets.UTF_8),
           "Alice".getBytes(StandardCharsets.UTF_8));

    byte[] value = db.get("name".getBytes(StandardCharsets.UTF_8));
    System.out.println(new String(value, StandardCharsets.UTF_8));
}
```

## Storage model today

The current implementation is an append-only log plus an in-memory index:

```text
PUT / DELETE
     |
     v
append-only data file
     |
     +----> in-memory key -> file offset index

merge() rewrites only the currently live entries.
```

This is the starting point, not the final architecture. The roadmap evolves the project toward a real LSM-style engine with WAL semantics, MemTables, SSTables, checksums, Bloom filters, compaction, benchmarks, and crash-recovery tests.

## Project goals

1. Keep the implementation small enough to study.
2. Make storage behavior explicit and testable.
3. Prefer correctness and recoverability over benchmark claims.
4. Remain pure Java and easy to embed.
5. Build changes through issues, tests, reviews, and releases as a real OSS project.

## Non-goals for Phase 0

- Production-grade durability guarantees
- Multi-process writers
- Transactions
- Distributed storage
- Claims of outperforming mature engines such as RocksDB

## Roadmap

See [ROADMAP.md](ROADMAP.md).

## Contributing

Issues, bug reports, design discussions, tests, documentation, and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

See [SECURITY.md](SECURITY.md) for responsible reporting guidance.

## License

MIT License. See [LICENSE](LICENSE).
