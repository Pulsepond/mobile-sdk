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

Storage is isolated by deployment, project, source, and environment, so separate self-hosted installations cannot collide and rotating a write key does not rotate identity or lose queued events. Android uses the app's no-backup directory; Apple uses Application Support with backup exclusion enabled, so analytics state is not restored onto another device. A process-local ownership registry rejects a second live client for the same namespace. An atomic manifest selects the active installation generation, and each generation owns its own event journal. `reset` first commits a non-replayable resetting generation, removes previous-generation journals and crash-left journal temporary files, then marks the new generation active. Recovery completes any interrupted cleanup before reading events. A successful reset therefore leaves no previous-generation event data behind. Corrupt or foreign-generation records are rejected through the same strict event validator as newly tracked events, and the valid bounded remainder is compacted.

A flush removes data only after a terminal outcome:

- `202`: accepted;
- `413`: reduce the effective batch size, or drop one individually oversized event;
- `408`, `429`, `5xx`, or transport failure: retry with bounded jitter, then defer the intact queue until another explicit flush, event, or application launch;
- other status: reject without retry.

`reset` first invalidates older delivery work, waits for the active flush and ordered persistence writer, then commits the durable generation before clearing queued events and rotating identifiers. An in-flight request is cancelled on a best-effort basis, and an older generation can never retry or remove new events. Failure before the resetting manifest commits leaves the old state intact. Failure after that privacy boundary rotates in-memory identity and reports cleanup failure; the old generation is never replayed, and recovery resumes cleanup before normal startup. This matters when reset represents logout or consent withdrawal.

`shutdown` has one shared completion signal. Concurrent callers join the same close operation. Before clearing memory, it makes a final ordered snapshot attempt; if durable storage is still unavailable, shutdown reports the exact loss count and fails instead of claiming a clean close. The writer is then drained before its scope and transport close, and cancellation of the initiating caller still runs cleanup before propagating cancellation. Successfully persisted, unaccepted events remain for the next launch.

`requestFlush` is the lifecycle-safe, non-suspending entry point. It coalesces repeated requests onto the SDK-owned scope and re-enables a queue deferred by exhausted retries. It does not claim a platform background execution grant; if the process is stopped, the durable queue remains available on the next launch. Platform applications choose the appropriate process, application, or scene callback instead of the SDK silently registering one.

The canonical protocol schema and complete event-batch fixture set are commit-pinned in `protocol-fixtures`. Android host tests consume that independent snapshot. CI publishes the Maven module to an isolated repository and compiles a separate Android application against it. On macOS, CI packages the release XCFramework ZIP, verifies its checksum and commit provenance, extracts it, and compiles a real Swift consumer. Publication metadata, archive shape, and Objective-C export changes therefore cannot silently invalidate the documented integration.

## Current limitations

The pre-1.0 implementation intentionally excludes automatic capture, user profiles, remote configuration, background services, and automatically registered platform lifecycle observers. One live client per source/environment is enforced inside a process; multi-process writers are not coordinated.

Swift Package Manager needs an immutable binary URL and checksum for every version. The release workflow publishes a checksummed XCFramework first; a Swift façade and atomic package-index workflow will follow rather than committing an unverifiable placeholder manifest.

## Compatibility policy

The event envelope is versioned independently through `schema_version`. Public Kotlin API dumps are checked in CI. The SDK compiler stays within the Kotlin metadata ceiling readable by the Android Gradle Plugin used by the standalone consumer check; this prevents publishing a valid Maven module that ordinary Android applications cannot compile. Until 1.0, release notes may describe source-breaking SDK changes. After 1.0, incompatible public API or protocol changes require a major version.
