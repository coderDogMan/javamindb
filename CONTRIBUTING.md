# Contributing to JavaMinDB

Thanks for helping improve JavaMinDB.

## Development setup

Requirements:

- JDK 17+
- Maven 3.9+

Run the full verification suite before opening a pull request:

```bash
mvn clean verify
```

## Workflow

1. Search existing issues before starting substantial work.
2. For behavior changes, open or reference an issue describing the problem and expected behavior.
3. Keep pull requests focused and reasonably small.
4. Add regression tests for bug fixes and tests for new behavior.
5. Update documentation when user-visible behavior changes.
6. Avoid drive-by formatting changes unrelated to the PR.

## Storage-engine changes

Changes involving persistence, file formats, recovery, compaction, concurrency, or corruption handling should explain:

- the invariant being preserved;
- failure cases considered;
- compatibility impact on existing data files;
- tests that exercise restart or partial-failure behavior where relevant.

## Commit and PR guidance

Use descriptive commit messages. A PR description should include:

- what changed;
- why it changed;
- how it was tested;
- any compatibility or migration implications.

## Compatibility

Until the project reaches a stable file-format milestone, persistence compatibility may change between pre-1.0 releases. Any intentional incompatible change must be documented in the release notes.

## Code of conduct

Be respectful, technical, and constructive. Harassment, abuse, and discriminatory behavior are not acceptable in project spaces.
