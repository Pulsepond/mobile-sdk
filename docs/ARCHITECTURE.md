# Architecture

## Goals

The mobile SDK provides one protocol implementation for Android and iOS without making either platform depend on a WebView or JavaScript runtime. Its public surface remains deliberately small:

- immutable configuration;
- explicit `track` calls with a closed property model;
- bounded `flush`, `reset`, and `shutdown` operations;
- redacted operational diagnostics.

Kotlin Multiplatform owns event construction, identity and session rules, batching, retries, and state transitions. Platform source sets own secure randomness and the HTTP engine. Android is published as a Maven artifact; Apple builds are combined into a static XCFramework.

## Trust boundaries

The application owns event names and property values. The SDK validates their shape and size but does not infer whether a value is personal data. The write key is publishable and grants ingestion only; it must never grant administrative or read access.

The SDK accepts only an HTTPS endpoint ending in the exact `/v1/batch` path. Redirects are disabled so an application cannot accidentally forward its write key or payload to a different origin. Loopback HTTP is allowed for local development.

Diagnostics expose stable codes, counts, retryability, and HTTP status only. They do not expose event bodies, property values, write keys, or transport exceptions.

## State model

`track` validates and freezes an event before placing it in an in-memory queue. Queue count and byte size are both bounded. One mutex serializes flushes; a small synchronized state section protects identity and the queue from calls made on different threads.

A flush removes data only after a terminal outcome:

- `202`: accepted;
- `413`: reduce the effective batch size, or drop one individually oversized event;
- `408`, `429`, `5xx`, or transport failure: retry with bounded jitter, then drop with a diagnostic;
- other status: reject without retry.

`reset` increments a generation number before clearing queued events and rotating identifiers. An in-flight batch from an older generation can finish its network request but cannot remove new events.

## Current limitations

The initial pre-1.0 implementation intentionally excludes automatic capture, user profiles, remote configuration, background services, and platform lifecycle hooks. Queue and identity state are also memory-only. That keeps the first protocol boundary reviewable, but durable storage is required before the SDK can claim cross-launch installation identity or process-death delivery.

Swift Package Manager needs an immutable binary URL and checksum for every version. The release workflow publishes a checksummed XCFramework first; a Swift façade and atomic package-index workflow will follow rather than committing an unverifiable placeholder manifest.

## Compatibility policy

The event envelope is versioned independently through `schema_version`. Public Kotlin API dumps are checked in CI. Until 1.0, release notes may describe source-breaking SDK changes. After 1.0, incompatible public API or protocol changes require a major version.
