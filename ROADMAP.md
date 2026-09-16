# Roadmap

JavaMinDB is developed in explicit phases so correctness and maintainability grow before feature count.

## Phase 0 — Project revival

Status: **complete**

- Standard Maven layout
- Java package namespace
- MIT license
- JUnit 5 regression tests
- GitHub Actions CI on Java 17 and 21
- Contributor and security documentation
- Restart index reconstruction fixed
- Platform-specific merge paths removed
- Clean OSS baseline established

Exit gate: `mvn clean verify` passes in CI and reopen/merge regression tests pass.

## Phase 1 — Stable append-only engine

Status: **complete in v0.1.0**

- Define public API semantics and error model
- Add exclusive directory locking
- Define and implement versioned on-disk format v1
- Migrate the Phase 0 headerless format
- Validate entry lengths and operation codes
- Repair physically incomplete trailing records deterministically
- Reject structural corruption and unsupported future versions
- Add explicit `sync()` boundary
- Document API, file format, compatibility policy, and release notes
- Add a tag-driven GitHub release workflow

Exit gate: the Phase 1 regression suite passes on supported JDKs and the repository is ready to tag `v0.1.0`.

## Phase 2 — Durability and crash recovery

- Define WAL and durability semantics
- Add per-record checksums / CRC
- Detect payload corruption, not only structural corruption
- Define sync/flush ordering across WAL and data files
- Deterministic crash recovery across fault points
- Fault-injection and torn-write tests
- Define format-v2 migration if the record layout changes

## Phase 3 — LSM foundation

- MemTable
- Immutable MemTable flush
- SSTable format
- Sparse index
- Ordered iteration primitives

## Phase 4 — Read and compaction efficiency

- Bloom filters
- Leveled or size-tiered compaction design
- Compaction correctness tests
- Tombstone lifecycle

## Phase 5 — Benchmarking and concurrency

- JMH benchmark module
- Repeatable datasets and workloads
- Read/write contention tests
- Memory and write-amplification measurements
- Comparisons reported with methodology and limitations

## Phase 6 — Distribution and adoption

- Maven Central publication
- Versioned release notes
- Examples and integration guides
- Contributor-friendly issues
- Real-world user feedback and compatibility policy

## Long-term direction

The project aims to remain a small, understandable pure-Java storage engine. Features that materially increase complexity must justify their educational or practical value.
