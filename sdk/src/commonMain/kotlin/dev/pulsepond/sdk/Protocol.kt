package dev.pulsepond.sdk

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.time.Instant

internal const val eventSchemaVersion: Int = 1
internal const val maxBatchBytes: Int = 60_000
internal const val maxQueueBytes: Int = 1_000_000

internal data class EventRecord(
    val eventId: String,
    val eventName: String,
    val occurredAtMilliseconds: Long,
    val occurredAt: String,
    val platform: String,
    val appVersion: String?,
    val release: String?,
    val environment: String,
    val anonymousInstallationId: String,
    val sessionId: String,
    val properties: Map<String, EventPropertyValue>,
) {
    fun json(): JsonObject = buildJsonObject {
        put("event_id", JsonPrimitive(eventId))
        put("schema_version", JsonPrimitive(eventSchemaVersion))
        put("event_name", JsonPrimitive(eventName))
        put("occurred_at", JsonPrimitive(occurredAt))
        put("platform", JsonPrimitive(platform))
        if (appVersion != null) put("app_version", JsonPrimitive(appVersion))
        if (release != null) put("release", JsonPrimitive(release))
        put("environment", JsonPrimitive(environment))
        put("anonymous_installation_id", JsonPrimitive(anonymousInstallationId))
        put("session_id", JsonPrimitive(sessionId))
        put(
            "properties",
            JsonObject(
                properties.mapValues { (_, value) ->
                    when (value) {
                        is EventPropertyValue.Text -> JsonPrimitive(value.value)
                        is EventPropertyValue.Integer -> JsonPrimitive(value.value)
                        is EventPropertyValue.Flag -> JsonPrimitive(value.value)
                        EventPropertyValue.Null -> JsonNull
                    }
                },
            ),
        )
    }
}

internal fun eventBatchJson(events: List<EventRecord>): String =
    buildJsonObject {
        put("events", JsonArray(events.map(EventRecord::json)))
    }.toString()

internal fun createUuidV7(timestampMilliseconds: Long, random: (ByteArray) -> Unit): String {
    if (timestampMilliseconds !in 0..0xffffffffffffL) {
        throw PulsepondConfigurationException("Pulsepond received an invalid system clock value")
    }
    val bytes = ByteArray(16)
    random(bytes)
    var timestamp = timestampMilliseconds
    for (index in 5 downTo 0) {
        bytes[index] = (timestamp and 0xff).toByte()
        timestamp = timestamp ushr 8
    }
    bytes[6] = (0x70 or (bytes[6].toInt() and 0x0f)).toByte()
    bytes[8] = (0x80 or (bytes[8].toInt() and 0x3f)).toByte()
    val hex = bytes.joinToString(separator = "") { byte ->
        (byte.toInt() and 0xff).toString(16).padStart(2, '0')
    }
    return buildString(36) {
        append(hex, 0, 8)
        append('-')
        append(hex, 8, 12)
        append('-')
        append(hex, 12, 16)
        append('-')
        append(hex, 16, 20)
        append('-')
        append(hex, 20, 32)
    }
}

internal fun occurredAt(timestampMilliseconds: Long): String =
    Instant.fromEpochMilliseconds(timestampMilliseconds).toString()
