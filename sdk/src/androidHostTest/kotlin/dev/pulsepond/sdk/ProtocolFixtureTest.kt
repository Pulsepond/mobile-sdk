package dev.pulsepond.sdk

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.longOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Instant

class ProtocolFixtureTest {
    @Test
    fun canonicalSnapshotClassifiesEveryEventBatchFixture() {
        validFixtures.forEach { path ->
            assertTrue(isCanonicalBatch(resourceJson(path)), "expected valid fixture: $path")
        }
        invalidFixtures.forEach { path ->
            assertEquals(false, isCanonicalBatch(resourceJson(path)), "expected invalid fixture: $path")
        }
    }

    @Test
    fun mobileGeneratedBatchMatchesTheCanonicalSnapshot() {
        val event = EventRecord(
            eventId = "01890f3e-e4b8-7cc3-98c8-7f0d7b4c9a10",
            eventName = "view_work",
            occurredAtMilliseconds = 1_800_000_000_000,
            occurredAt = "2027-01-15T08:00:00Z",
            platform = "android",
            appVersion = "1.0.0",
            release = "android@1.0.0",
            environment = "production",
            anonymousInstallationId = "01890f3e-e4b8-7cc3-98c8-7f0d7b4c9a11",
            sessionId = "01890f3e-e4b8-7cc3-98c8-7f0d7b4c9a12",
            properties = PulsepondProperties()
                .setString("work_id", "work_123")
                .setBoolean("completed", false)
                .snapshot(),
        )

        assertTrue(isCanonicalBatch(Json.parseToJsonElement(eventBatchJson(listOf(event)))))
        val schema = resourceText("event-batch.v1.schema.json")
        assertTrue(schema.contains("https://pulsepond.dev/schemas/event-batch.v1.schema.json"))
        assertTrue(schema.contains("\"android\", \"ios\", \"other\", \"server\", \"web\""))
    }

    private fun resourceJson(path: String): JsonElement =
        Json.parseToJsonElement(resourceText(path))

    private fun resourceText(path: String): String =
        requireNotNull(javaClass.getResourceAsStream("/$path")) {
            "missing protocol fixture resource: $path"
        }.bufferedReader().use { it.readText() }
}

private val validFixtures = listOf(
    "valid/event-batch-null-optionals.json",
    "valid/event-batch.json",
)

private val invalidFixtures = listOf(
    "invalid/event-batch-client-routing.json",
    "invalid/event-batch-duplicate-event-id-case.json",
    "invalid/event-batch-duplicate-event-id.json",
    "invalid/event-batch-non-ascii-text.json",
    "invalid/event-batch-non-utc-timestamp.json",
    "invalid/event-batch-noncanonical-uuid.json",
    "invalid/event-batch-pre-epoch.json",
    "invalid/event-batch-property-array.json",
    "invalid/event-batch-property-float.json",
    "invalid/event-batch-property-invalid-key.json",
    "invalid/event-batch-property-non-ascii.json",
    "invalid/event-batch-property-object.json",
    "invalid/event-batch-property-unsafe-integer.json",
    "invalid/event-batch-surrounding-whitespace.json",
    "invalid/event-batch-too-many-properties.json",
    "invalid/event-batch-unknown-field.json",
    "invalid/event-batch-uuid-v4.json",
)

private val eventId = Regex(
    "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-7[0-9a-fA-F]{3}-[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$",
)
private val slug = Regex("^[A-Za-z0-9][A-Za-z0-9_.-]*$")
private val utcTimestamp = Regex("^(?:19(?:7[0-9]|[89][0-9])|[2-9][0-9]{3})-.*Z$")
private val platforms = setOf("android", "ios", "other", "server", "web")
private val requiredEventKeys = setOf(
    "event_id",
    "schema_version",
    "event_name",
    "occurred_at",
    "platform",
    "environment",
    "anonymous_installation_id",
    "session_id",
)
private val allowedEventKeys = requiredEventKeys + setOf("app_version", "release", "properties")

private fun isCanonicalBatch(value: JsonElement): Boolean {
    val root = value as? JsonObject ?: return false
    if (root.keys != setOf("events")) return false
    val events = runCatching { root.getValue("events").jsonArray }.getOrNull() ?: return false
    if (events.size !in 1..100) return false
    val ids = mutableSetOf<String>()
    return events.all { element ->
        val event = element as? JsonObject ?: return@all false
        val id = event.string("event_id") ?: return@all false
        id.matches(eventId) && ids.add(id.lowercase()) && validEvent(event)
    }
}

private fun validEvent(event: JsonObject): Boolean {
    if (!event.keys.containsAll(requiredEventKeys) || !allowedEventKeys.containsAll(event.keys)) {
        return false
    }
    if ((event["schema_version"] as? JsonPrimitive)?.longOrNull != 1L) return false
    if (!event.validSlug("event_name", 64) || !event.validSlug("environment", 32)) return false
    if (event.string("platform") !in platforms) return false
    val occurredAt = event.string("occurred_at") ?: return false
    if (!occurredAt.matches(utcTimestamp) || runCatching { Instant.parse(occurredAt) }.isFailure) {
        return false
    }
    if (!event.validText("anonymous_installation_id", 128)) return false
    if (!event.validText("session_id", 128)) return false
    if (!event.validOptionalText("app_version", 64)) return false
    if (!event.validOptionalText("release", 128)) return false
    val properties = event["properties"] ?: return true
    val values = properties as? JsonObject ?: return false
    return values.size <= 32 && values.all { (key, value) ->
        key.length <= 64 && key.matches(slug) && validProperty(value)
    }
}

private fun validProperty(value: JsonElement): Boolean {
    if (value == JsonNull) return true
    val primitive = value as? JsonPrimitive ?: return false
    if (primitive.isString) return printableAscii(primitive.content, 256)
    if (primitive.content == "true" || primitive.content == "false") return true
    val integer = primitive.longOrNull ?: return false
    return integer in -9_007_199_254_740_991..9_007_199_254_740_991
}

private fun JsonObject.string(key: String): String? {
    val primitive = this[key] as? JsonPrimitive ?: return null
    return primitive.takeIf { it.isString }?.content
}

private fun JsonObject.validSlug(key: String, maximum: Int): Boolean {
    val value = string(key) ?: return false
    return value.length <= maximum && value.matches(slug)
}

private fun JsonObject.validText(key: String, maximum: Int): Boolean =
    string(key)?.let { printableAscii(it, maximum) } == true

private fun JsonObject.validOptionalText(key: String, maximum: Int): Boolean {
    val value = this[key] ?: return true
    if (value == JsonNull) return true
    return string(key)?.let { printableAscii(it, maximum) } == true
}

private fun printableAscii(value: String, maximum: Int): Boolean =
    value.isNotEmpty() &&
        value.length <= maximum &&
        value.first().code in 0x21..0x7e &&
        value.last().code in 0x21..0x7e &&
        value.all { it.code in 0x20..0x7e }
