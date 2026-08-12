# Pulsepond Mobile SDK

[![CI](https://github.com/Pulsepond/mobile-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/Pulsepond/mobile-sdk/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-2e6b4f.svg)](LICENSE)

The first-party Pulsepond SDK for Android and iOS. Its delivery core is written once with Kotlin Multiplatform while the published artifacts stay natural to each platform: a Maven library for Android and a static XCFramework for Apple applications.

The SDK deliberately collects only events an application explicitly tracks. There is no automatic screen, click, location, advertising identifier, request-header, or crash collection.

> The repository is pre-1.0. The event protocol is stable, but the native packaging and public API may change before the first stable release.

## Current support

| Platform | Minimum | Distribution |
| --- | ---: | --- |
| Android | API 24 | `dev.pulsepond:pulsepond` on Maven Central |
| iOS | iOS arm64 and Apple Silicon simulator | `Pulsepond.xcframework` attached to GitHub releases |

Swift Package Manager distribution is intentionally deferred until the binary release workflow can update and verify checksums atomically. Until then, add the release XCFramework directly to the Xcode project.

## Android

```kotlin
dependencies {
    implementation("dev.pulsepond:pulsepond:<version>")
}
```

```kotlin
import android.content.Context
import dev.pulsepond.sdk.Pulsepond
import dev.pulsepond.sdk.PulsepondAndroid
import dev.pulsepond.sdk.PulsepondConfiguration
import dev.pulsepond.sdk.PulsepondProperties

suspend fun createAnalytics(context: Context): Pulsepond {
    val analytics = PulsepondAndroid.create(
        context,
        PulsepondConfiguration(
            endpoint = "https://pulsepond.example.com/v1/batch",
            writeKey = "ppw_v1_...",
            deploymentId = "01234567-89ab-4def-8abc-0123456789ab",
            projectId = "project_foundation",
            sourceId = "source_android",
            environment = "production",
            appVersion = BuildConfig.VERSION_NAME,
            release = "android@${BuildConfig.VERSION_NAME}",
        ),
    )

    analytics.track(
        "view_work",
        PulsepondProperties().setString("work_id", "work_123"),
    )
    return analytics
}
```

Creation performs recovery on a background dispatcher and is therefore suspending. Keep one client per source and environment. Call `analytics.shutdown()` from a coroutine when the application has a real finalization opportunity. Mobile operating systems do not guarantee a termination callback, so normal delivery is handled by bounded automatic flushes.

For an application-level lifecycle callback, request a non-blocking flush when the process moves to the background:

```kotlin
class AnalyticsLifecycle(
    private val analytics: Pulsepond,
) : DefaultLifecycleObserver {
    override fun onStop(owner: LifecycleOwner) {
        analytics.requestFlush()
    }
}
```

Register the observer with your application lifecycle, for example `ProcessLifecycleOwner`. Pulsepond does not register lifecycle observers itself and does not start a background service.

## iOS

Download `Pulsepond.xcframework.zip` from a release, verify the published SHA-256 checksum, unpack it, and add `Pulsepond.xcframework` to the Xcode target under **Frameworks, Libraries, and Embedded Content**.

```swift
import Pulsepond

let configuration = try PulsepondConfiguration(
    endpoint: "https://pulsepond.example.com/v1/batch",
    writeKey: "ppw_v1_...",
    deploymentId: "01234567-89ab-4def-8abc-0123456789ab",
    projectId: "project_foundation",
    sourceId: "source_ios",
    environment: "production",
    appVersion: Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String,
    release: "ios@1.0.0",
    batchSize: 20,
    flushIntervalMilliseconds: 5_000,
    maxQueueSize: 1_000,
    eventTtlMilliseconds: 82_800_000,
    diagnosticListener: nil
)
let analytics = try await PulsepondApple.shared.create(configuration: configuration)

try analytics.track(
    eventName: "view_work",
    properties: try PulsepondProperties().setString(key: "work_id", value: "work_123")
)
```

Validation failures cross the generated Objective-C boundary as Swift errors. Configuration, property setters, and tracking use `try`; creation, flush, reset, and shutdown use `try await`. A real Swift consumer is type-checked against the release XCFramework on macOS CI. A hand-written convenience façade and Swift Package Manager distribution are planned before the first stable release.

Call the non-blocking lifecycle method from an application or scene background callback:

```swift
func applicationDidEnterBackground(_ application: UIApplication) {
    analytics?.requestFlush()
}
```

`requestFlush()` coalesces repeated requests and returns immediately. It does not extend the operating system's background execution window; anything not delivered remains in app-private storage for the next launch. Use `try await analytics.flush()` when your code must wait for the attempt to finish.

## Contract and delivery

- Events are posted only to the configured HTTPS URL with the exact `/v1/batch` path. Plain HTTP is accepted only for loopback development.
- Every event receives a UUIDv7 `event_id`, UTC `occurred_at`, platform, environment, anonymous installation ID, and session ID.
- Properties are flat and intentionally closed to strings, safe integers, booleans, and null. Names and values are bounded before enqueueing.
- Queues, batches, retry counts, retry delays, event age, and on-disk journal size are bounded. A 413 response splits a batch without changing event IDs.
- Android and Apple factories persist the installation identity and unsent queue in app-private storage, isolated by deployment, project, source, and environment. `track` returns without filesystem I/O; one bounded serial writer preserves operation order in the background. Accepted event IDs may be replayed after a storage write failure, so the server remains responsible for idempotent ingestion.
- A batch body is frozen before its first attempt and remains byte-identical across retries.
- HTTP 202 is the only success response. Network errors, 408, 429, and 5xx responses retry with bounded jitter. Other 4xx responses are terminal.
- Diagnostics never contain event payloads, property values, endpoint query data, or credentials.

Create clients with `PulsepondAndroid.create` or `PulsepondApple.shared.create`; both select the durable platform implementation.

See [Architecture](docs/ARCHITECTURE.md) for the design constraints and [Contributing](CONTRIBUTING.md) for local verification.

## Credentials and privacy

A Pulsepond write key is a publishable, source-scoped ingestion credential. Mobile binaries cannot keep a bundled value secret. Never embed a control-plane token, Cloudflare token, or read credential. Rotate the write key if a source is abused. The non-secret deployment, project, and source IDs identify the durable storage namespace and must remain unchanged when its write key rotates.

Pulsepond cannot know whether a custom property is personal data. Do not send email addresses, authorization values, cookies, full URLs or query strings, search text, feedback bodies, request bodies, or other content that your disclosure and retention policy do not cover.

## License

Apache License 2.0. See [LICENSE](LICENSE).
