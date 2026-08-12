package dev.pulsepond.consumer

import android.content.Context
import dev.pulsepond.sdk.Pulsepond
import dev.pulsepond.sdk.PulsepondAndroid
import dev.pulsepond.sdk.PulsepondConfiguration
import dev.pulsepond.sdk.PulsepondProperties

private const val testWriteKey =
    "ppw_v1_0123456789abcdef0123456789abcdef_" +
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

suspend fun createPulsepond(context: Context): Pulsepond {
    val configuration = PulsepondConfiguration(
        endpoint = "https://events.example.com/v1/batch",
        writeKey = testWriteKey,
        deploymentId = "01234567-89ab-4def-8abc-0123456789ab",
        projectId = "project_foundation",
        sourceId = "source_android",
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

fun onApplicationBackgrounded(client: Pulsepond) {
    client.requestFlush()
}
