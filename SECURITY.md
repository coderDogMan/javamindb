# Security Policy

JavaMinDB is pre-1.0 software and is not currently recommended for storing sensitive or irreplaceable production data.

## Reporting a vulnerability

Please do not publish exploit details in a public issue before the maintainer has had a reasonable opportunity to assess the report.

For now, use a minimal public GitHub issue asking for a private contact channel, without including sensitive exploit details. A dedicated security reporting channel will be added as the project matures.

## Scope

Security-relevant reports include, but are not limited to:

- data corruption that can be triggered by crafted input;
- unsafe file path handling;
- persistence bugs that violate documented durability guarantees;
- denial-of-service conditions caused by malformed on-disk data;
- dependency vulnerabilities affecting released artifacts.

## Supported versions

Until the first stable release, only the latest published pre-1.0 version is expected to receive fixes.
