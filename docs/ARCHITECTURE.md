# Architecture

## Goals

The mobile SDK provides one protocol implementation for Android and iOS without making either platform depend on a WebView or JavaScript runtime. Its public surface remains deliberately small:

- immutable configuration;
- explicit `track` calls with a closed property model;
- bounded `flush`, `reset`, and `shutdown` operations;
- redacted operational diagnostics.

Kotlin Multiplatform owns event construction, identity and session rules, batching, retries, persistence, and state transitions. Platform source sets provide the app-private storage root, secure randomness, and HTTP engine. Android is published as a Maven artifact; Apple builds are combined into a static XCFramework.

## Trust boundaries

The application owns event names and property values. The SDK validates their shape and size but does not infer whether a value is personal data. The write key is publishable and grants ingestion only; it must never grant administrative or read access.

The SDK accepts only an HTTPS endpoint ending in the exact `/v1/batch` path. Redirects are disabled so an application cannot accidentally forward its write key or payload to a different origin. Loopback HTTP is allowed for local development.

Diagnostics expose stable codes, counts, retryability, and HTTP status only. They do not expose event bodies, property values, write keys, or transport exceptions.

## State model

`track` validates and freezes an event before appending it to an app-private journal and the in-memory working queue. Queue count, byte size, journal size, and event age are bounded. One mutex serializes flushes; a small synchronized state section protects identity, persistence, and the queue from calls made on different threads.

Storage is isolated by source ID and environment. Android uses the app's no-backup directory; Apple uses Application Support with backup exclusion enabled, so analytics state is not restored onto another device. An atomic manifest selects the active installation generation, and each generation owns its own event journal. `reset` creates an empty journal for a new UUIDv7 installation, atomically switches the manifest, and only then removes the previous journal. A crash or write failure therefore selects either the complete old generation or the complete new generation, never a mixed identity and queue. Corrupt journal records are rejected through the same strict event parser and the valid remainder is compacted.

A flush removes data only after a terminal outcome:

- `202`: accepted;
- `413`: reduce the effective batch size, or drop one individually oversized event;
- `408`, `429`, `5xx`, or transport failure: retry with bounded jitter, then defer the intact queue until another explicit flush, event, or application launch;
- other status: reject without retry.

`reset` switches durable generations before completing the in-memory invalidation signal, clearing queued events, and rotating identifiers. An in-flight request is cancelled on a best-effort basis, and an older generation can never retry or remove new events. If the durable switch fails, reset fails without changing the active in-memory state. This matters when reset represents logout or consent withdrawal.

`shutdown` has one shared completion signal. Concurrent callers join the same close operation, and cancellation of the initiating caller still runs transport and owned-scope cleanup before propagating cancellation. Unaccepted events remain in durable storage for the next launch.

The canonical protocol schema and complete event-batch fixture set are commit-pinned in `protocol-fixtures`. Android host tests consume that independent snapshot, while macOS CI compiles a real Swift consumer against the assembled XCFramework so Objective-C export changes cannot silently invalidate the documented API.

## Current limitations

The pre-1.0 implementation intentionally excludes automatic capture, user profiles, remote configuration, background services, and platform lifecycle hooks. It assumes one live client per source/environment inside a process; multi-process writers are not coordinated.

Swift Package Manager needs an immutable binary URL and checksum for every version. The release workflow publishes a checksummed XCFramework first; a Swift façade and atomic package-index workflow will follow rather than committing an unverifiable placeholder manifest.

## Compatibility policy

The event envelope is versioned independently through `schema_version`. Public Kotlin API dumps are checked in CI. Until 1.0, release notes may describe source-breaking SDK changes. After 1.0, incompatible public API or protocol changes require a major version.
