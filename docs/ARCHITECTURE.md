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

`track` validates and freezes an event before adding it to the in-memory working queue and a bounded serial persistence writer. It does no filesystem I/O on the caller thread. The writer orders journal appends, compaction, reset, and shutdown barriers; platform factories perform initial recovery on a background dispatcher. Queue count, byte size, journal size, and event age are bounded. One mutex serializes flushes, and a small synchronized state section protects identity and the queue from calls made on different threads.

Storage is isolated by stable source ID and environment, so rotating a write key does not rotate identity or lose queued events. Android uses the app's no-backup directory; Apple uses Application Support with backup exclusion enabled, so analytics state is not restored onto another device. A process-local ownership registry rejects a second live client for the same namespace. An atomic manifest selects the active installation generation, and each generation owns its own event journal. `reset` creates an empty journal, guarantees removal of previous-generation journals, and then atomically switches the manifest. A successful reset therefore leaves no previous-generation event data behind. Corrupt or foreign-generation records are rejected through the same strict event validator as newly tracked events, and the valid bounded remainder is compacted.

A flush removes data only after a terminal outcome:

- `202`: accepted;
- `413`: reduce the effective batch size, or drop one individually oversized event;
- `408`, `429`, `5xx`, or transport failure: retry with bounded jitter, then defer the intact queue until another explicit flush, event, or application launch;
- other status: reject without retry.

`reset` first invalidates older delivery work, waits for the active flush and ordered persistence writer, then commits the durable generation before clearing queued events and rotating identifiers. An in-flight request is cancelled on a best-effort basis, and an older generation can never retry or remove new events. If the durable operation fails, reset fails without changing the active in-memory state. This matters when reset represents logout or consent withdrawal.

`shutdown` has one shared completion signal. Concurrent callers join the same close operation. The ordered persistence writer is drained before its scope and transport close, and cancellation of the initiating caller still runs cleanup before propagating cancellation. Unaccepted events remain in durable storage for the next launch.

The canonical protocol schema and complete event-batch fixture set are commit-pinned in `protocol-fixtures`. Android host tests consume that independent snapshot, while macOS CI compiles a real Swift consumer against the assembled XCFramework so Objective-C export changes cannot silently invalidate the documented API.

## Current limitations

The pre-1.0 implementation intentionally excludes automatic capture, user profiles, remote configuration, background services, and platform lifecycle hooks. One live client per source/environment is enforced inside a process; multi-process writers are not coordinated.

Swift Package Manager needs an immutable binary URL and checksum for every version. The release workflow publishes a checksummed XCFramework first; a Swift façade and atomic package-index workflow will follow rather than committing an unverifiable placeholder manifest.

## Compatibility policy

The event envelope is versioned independently through `schema_version`. Public Kotlin API dumps are checked in CI. Until 1.0, release notes may describe source-breaking SDK changes. After 1.0, incompatible public API or protocol changes require a major version.
