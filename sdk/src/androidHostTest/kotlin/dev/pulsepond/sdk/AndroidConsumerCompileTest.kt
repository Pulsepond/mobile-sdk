package dev.pulsepond.sdk

import android.content.Context

/** Compiled in CI to keep the documented Android entry point source-compatible. */
@Suppress("unused")
private suspend fun createAndroidConsumer(context: Context): Pulsepond {
    val configuration = PulsepondConfiguration(
        endpoint = "https://events.example.com/v1/batch",
        writeKey = testWriteKey,
        deploymentId = testDeploymentId,
        projectId = testProjectId,
        sourceId = testSourceId,
        environment = "production",
        appVersion = "1.0.0",
        release = "android@1.0.0",
    )
    return PulsepondAndroid.create(context, configuration).also { client ->
        client.track(
            "view_work",
            PulsepondProperties().setString("work_id", "work_123"),
        )
    }
}

/** Mirrors an Android application/process ON_STOP callback without taking a lifecycle dependency. */
@Suppress("unused")
private fun onAndroidApplicationBackgrounded(client: Pulsepond) {
    client.requestFlush()
}
