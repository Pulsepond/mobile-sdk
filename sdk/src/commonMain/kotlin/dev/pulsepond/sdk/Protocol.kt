package dev.pulsepond.sdk

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlin.time.Instant

internal const val eventSchemaVersion: Int = 1
internal const val maxBatchBytes: Int = 60_000
internal const val maxQueueBytes: Int = 1_000_000
private val persistedPlatforms: Set<String> = setOf("android", "ios", "web", "server", "other")

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

internal fun parsePersistedEvent(value: String): EventRecord? = runCatching {
    val json = Json.parseToJsonElement(value).jsonObject
    if (json["schema_version"]?.jsonPrimitive?.intOrNull != eventSchemaVersion) return null
    val occurredAt = json.requiredString("occurred_at")
    val occurredAtMilliseconds = Instant.parse(occurredAt).toEpochMilliseconds()
    if (occurredAt(occurredAtMilliseconds) != occurredAt) error("occurred_at must be canonical")
    val properties = json["properties"]?.jsonObject?.entries?.associateTo(linkedMapOf()) {
        (key, element) ->
        validateSlug("property name", key, 64)
        val primitive = element as? JsonPrimitive ?: error("property must be scalar")
        key to when {
            primitive === JsonNull -> EventPropertyValue.Null
            primitive.isString -> EventPropertyValue.Text(primitive.content).also {
                validatePrintableText("string property", primitive.content, 256)
            }
            primitive.booleanOrNull != null -> EventPropertyValue.Flag(primitive.booleanOrNull!!)
            primitive.longOrNull != null -> EventPropertyValue.Integer(primitive.longOrNull!!).also {
                if (primitive.longOrNull!! !in -maxSafeInteger..maxSafeInteger) {
                    error("numeric property must be a safe integer")
                }
            }
            else -> error("property must use a supported scalar")
        }
    } ?: error("properties are required")
    if (properties.size > maxProperties) error("too many properties")
    val eventId = json.requiredString("event_id")
    val eventName = json.requiredString("event_name")
    val platform = json.requiredString("platform")
    val appVersion = json.optionalString("app_version")
    val release = json.optionalString("release")
    val environment = json.requiredString("environment")
    val anonymousInstallationId = json.requiredString("anonymous_installation_id")
    val sessionId = json.requiredString("session_id")
    if (!isUuidV7(eventId) || !isUuidV7(anonymousInstallationId) || !isUuidV7(sessionId)) {
        error("identifiers must be UUIDv7")
    }
    validateSlug("eventName", eventName, 64)
    if (platform !in persistedPlatforms) {
        error("platform is invalid")
    }
    validateOptionalText("appVersion", appVersion, 64)
    validateOptionalText("release", release, 128)
    validateSlug("environment", environment, 32)
    EventRecord(
        eventId = eventId,
        eventName = eventName,
        occurredAtMilliseconds = occurredAtMilliseconds,
        occurredAt = occurredAt,
        platform = platform,
        appVersion = appVersion,
        release = release,
        environment = environment,
        anonymousInstallationId = anonymousInstallationId,
        sessionId = sessionId,
        properties = properties,
    )
}.getOrNull()

private fun JsonObject.requiredString(name: String): String {
    val primitive = this[name] as? JsonPrimitive ?: error("$name is required")
    if (!primitive.isString) error("$name must be a string")
    return primitive.content
}

private fun JsonObject.optionalString(name: String): String? {
    val element = this[name] ?: return null
    if (element === JsonNull) return null
    val primitive = element as? JsonPrimitive ?: error("$name must be a string")
    if (!primitive.isString) error("$name must be a string")
    return primitive.content
}

internal fun isUuidV7(value: String): Boolean =
    value.length == 36 && value[14] == '7' && value[19].lowercaseChar() in "89ab" &&
        value.withIndex().all { (index, character) ->
            if (index == 8 || index == 13 || index == 18 || index == 23) {
                character == '-'
            } else {
                character.lowercaseChar() in '0'..'9' || character.lowercaseChar() in 'a'..'f'
            }
        }

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
