# Roadmap

JavaMinDB is being developed in explicit phases so correctness and maintainability grow before feature count.

## Phase 0 — Project revival

Status: in progress

- Standard Maven layout
- Java package namespace
- MIT license
- JUnit 5 regression tests
- GitHub Actions CI on supported JDKs
- Contributor and security documentation
- Fix restart index reconstruction
- Remove platform-specific merge paths
- Establish a clean baseline for future storage work

Exit gate: `mvn clean verify` passes on CI and reopen/merge regression tests pass.

## Phase 1 — Stable append-only engine

- Define public API semantics
- Define error model
- Harden entry decoding and size validation
- Add corruption/truncation tests
- Add file-format versioning strategy
- Publish first tagged pre-1.0 release

## Phase 2 — Durability and recovery

- WAL semantics and sync policy
- Checksums / CRC
- Detect partial writes
- Deterministic crash recovery
- Fault-injection tests

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
