package dev.pulsepond.sdk

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ProtocolTest {
    @Test
    fun uuidV7UsesCanonicalTimestampVersionAndVariantBits() {
        val uuid = createUuidV7(0x010203040506) { bytes -> bytes.fill(0) }

        assertEquals("01020304-0506-7000-8000-000000000000", uuid)
        assertTrue(isUuidV7(uuid))
        assertFalse(isUuidV7("-1020304-0506-7000-8000-000000000000"))
        assertFalse(isUuidV7("01020304-0506-7000-7000-000000000000"))
        assertFailsWith<PulsepondConfigurationException> {
            createUuidV7(maxProtocolTimestampMilliseconds + 1) { bytes -> bytes.fill(0) }
        }
    }

    @Test
    fun propertiesAreFlatBoundedTypedAndDeterministicallyOrdered() {
        val properties = PulsepondProperties()
            .setString("z_name", "work_123")
            .setBoolean("active", true)
            .setInteger("count", 7)
            .setNull("optional")

        val event = EventRecord(
            eventId = "00000000-0000-7000-8000-000000000000",
            eventName = "view_work",
            occurredAtMilliseconds = 0,
            occurredAt = "1970-01-01T00:00:00Z",
            platform = "android",
            appVersion = null,
            release = null,
            environment = "test",
            anonymousInstallationId = "installation",
            sessionId = "session",
            properties = properties.snapshot(),
        )
        val encoded = eventBatchJson(listOf(event))
        val decoded = Json.parseToJsonElement(encoded).jsonObject
        val values = decoded["events"]!!.jsonArray.single().jsonObject["properties"]!!.jsonObject

        assertEquals(listOf("active", "count", "optional", "z_name"), values.keys.toList())
        assertEquals("work_123", values["z_name"]!!.jsonPrimitive.content)
    }

    @Test
    fun invalidPropertyValuesFailBeforeAnEventCanBeQueued() {
        assertFailsWith<PulsepondValidationException> {
            PulsepondProperties().setString("search_text", " surrounding ")
        }
        assertFailsWith<PulsepondValidationException> {
            PulsepondProperties().setInteger("count", 9_007_199_254_740_992)
        }
        assertFailsWith<PulsepondValidationException> {
            PulsepondProperties().setBoolean("bad key", true)
        }
    }

    @Test
    fun configurationRequiresTheExactPublishOnlyEndpointAndCredentialShape() {
        val valid = PulsepondConfiguration(
            endpoint = "https://events.example.com/v1/batch",
            writeKey = testWriteKey,
            deploymentId = testDeploymentId,
            projectId = testProjectId,
            sourceId = testSourceId,
            environment = "production",
        )
        assertEquals("events.example.com", valid.parsedEndpoint.host)
        assertEquals(
            "$testDeploymentId/$testProjectId/$testSourceId/production",
            valid.storageNamespace,
        )
        val rotatedCredential = PulsepondConfiguration(
            endpoint = "https://events.example.com/v1/batch",
            writeKey = replacementWriteKey,
            deploymentId = testDeploymentId,
            projectId = testProjectId,
            sourceId = testSourceId,
            environment = "production",
        )
        assertEquals(valid.storageNamespace, rotatedCredential.storageNamespace)
        assertFailsWith<PulsepondConfigurationException> {
            PulsepondConfiguration(
                "https://events.example.com/v1/batch",
                testWriteKey,
                "not-a-deployment-id",
                testProjectId,
                testSourceId,
                "production",
            )
        }
        assertFailsWith<PulsepondConfigurationException> {
            PulsepondConfiguration(
                "https://events.example.com/v1/batch",
                testWriteKey,
                testDeploymentId,
                "bad/project",
                testSourceId,
                "production",
            )
        }
        assertFailsWith<PulsepondConfigurationException> {
            PulsepondConfiguration(
                "https://events.example.com/v1/batch",
                testWriteKey,
                testDeploymentId,
                testProjectId,
                "bad/source",
                "production",
            )
        }

        for (endpoint in listOf(
            "http://events.example.com/v1/batch",
            "https://events.example.com/v1/batch?source=other",
            "https://events.example.com/v1/batch#fragment",
            "https://events.example.com/v2/batch",
        )) {
            assertFailsWith<PulsepondConfigurationException>(endpoint) {
                PulsepondConfiguration(
                    endpoint,
                    testWriteKey,
                    testDeploymentId,
                    testProjectId,
                    testSourceId,
                    "production",
                )
            }
        }
        assertTrue(
            runCatching {
                PulsepondConfiguration(
                    "http://localhost:8787/v1/batch",
                    testWriteKey,
                    testDeploymentId,
                    testProjectId,
                    testSourceId,
                    "test",
                )
            }.isSuccess,
        )
    }
}

internal const val testWriteKey: String =
    "ppw_v1_0123456789abcdef0123456789abcdef_" +
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

internal const val testDeploymentId: String = "01234567-89ab-4def-8abc-0123456789ab"
internal const val testProjectId: String = "project_foundation"
internal const val testSourceId: String = "source_mobile"

internal const val replacementWriteKey: String =
    "ppw_v1_fedcba9876543210fedcba9876543210_" +
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
