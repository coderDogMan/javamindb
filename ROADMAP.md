# Roadmap

JavaMinDB is developed in explicit phases so correctness and maintainability grow before feature count.

## Phase 0 — Project revival

Status: **complete**

Maven layout, license, CI, tests, contributor/security docs, cross-platform paths, and restart correctness.

## Phase 1 — Stable append-only engine

Status: **complete in v0.1.0**

Public API semantics, directory locking, format v1, legacy migration, structural validation, deterministic incomplete-tail repair, explicit sync, and first release workflow.

## Phase 2 — Durability and crash recovery

Status: **complete in v0.2.0**

- WAL-first mutation protocol
- WAL fsync before data append
- per-record CRC32C
- monotonic record sequence numbers
- idempotent WAL replay
- data/WAL incomplete-tail handling
- format-v2 migration from Phase 0 and v1
- recovery-required state after ambiguous persistence errors
- fault injection at WAL/data/checkpoint boundaries
- durability, API, and file-format documentation

Exit gate: `mvn clean verify` passes on Java 17 and Java 21 with WAL recovery, CRC corruption, migration, and fault-injection regression tests.

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
- Repeatable datasets/workloads
- contention tests
- memory/write-amplification measurements
- comparisons with methodology and limitations

## Phase 6 — Distribution and adoption

- Maven Central publication
- versioned release notes
- examples/integration guides
- contributor-friendly issues
- real-world user feedback
- compatibility policy refinement

## Long-term direction

Remain a small, understandable pure-Java storage engine whose internals are readable enough to learn from and disciplined enough to exercise real storage-engine engineering.
