package dev.pulsepond.sdk

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class PulsepondTest {
    @Test
    fun acceptedBatchMatchesTheV1EnvelopeAndLeavesNoPendingEvents() = runTest {
        val transport = FakeTransport(FakeOutcome.Response(202))
        val client = client(transport)

        val eventId = client.track(
            "view_work",
            PulsepondProperties().setString("work_id", "work_123"),
        )
        client.flush()

        assertEquals(0, client.pendingEventCount())
        assertEquals(1, transport.bodies.size)
        val event = Json.parseToJsonElement(transport.bodies.single())
            .jsonObject["events"]!!.jsonArray.single().jsonObject
        assertEquals(eventId, event["event_id"]!!.jsonPrimitive.content)
        assertEquals("view_work", event["event_name"]!!.jsonPrimitive.content)
        assertEquals(pulsepondPlatform, event["platform"]!!.jsonPrimitive.content)
        assertEquals("production", event["environment"]!!.jsonPrimitive.content)
        assertEquals(1, event["schema_version"]!!.jsonPrimitive.content.toInt())
        assertTrue(event["occurred_at"]!!.jsonPrimitive.content.endsWith("Z"))
        client.shutdown()
    }

    @Test
    fun retryableFailuresResendByteIdenticalFrozenBatches() = runTest {
        val transport = FakeTransport(
            FakeOutcome.Failure,
            FakeOutcome.Response(500),
            FakeOutcome.Response(202),
        )
        val diagnostics = mutableListOf<PulsepondDiagnostic>()
        val client = client(transport, diagnostics)

        client.track("audio_play", PulsepondProperties().setString("work_id", "work_1"))
        client.flush()

        assertEquals(3, transport.bodies.size)
        assertEquals(1, transport.bodies.distinct().size)
        assertEquals(0, client.pendingEventCount())
        assertEquals(2, diagnostics.count { it.code == PulsepondDiagnosticCode.DeliveryFailed })
        client.shutdown()
    }

    @Test
    fun aLargeBatchIsSplitWithoutChangingEventIds() = runTest {
        val transport = FakeTransport(
            FakeOutcome.Response(413),
            FakeOutcome.Response(202),
            FakeOutcome.Response(202),
        )
        val client = client(transport, batchSize = 4)
        repeat(4) { index -> client.track("event_$index") }

        client.flush()

        assertEquals(listOf(4, 2, 2), transport.bodies.map(::batchSize))
        val originalIds = eventIds(transport.bodies.first())
        val splitIds = transport.bodies.drop(1).flatMap(::eventIds)
        assertEquals(originalIds, splitIds)
        assertEquals(0, client.pendingEventCount())
        client.shutdown()
    }

    @Test
    fun reachingBatchSizeFlushesWithoutWaitingForTheInterval() = runTest {
        val transport = FakeTransport(FakeOutcome.Response(202))
        val client = client(
            transport = transport,
            batchSize = 2,
            flushIntervalMilliseconds = 3_600_000,
            coroutineScope = this,
        )

        client.track("first")
        assertEquals(0, transport.bodies.size)
        client.track("second")
        advanceUntilIdle()

        assertEquals(1, transport.bodies.size)
        assertEquals(2, batchSize(transport.bodies.single()))
        client.shutdown()
    }

    @Test
    fun boundedQueueFailsClosedAndEmitsOnlyRedactedDiagnostics() = runTest {
        val transport = FakeTransport(FakeOutcome.Response(202))
        val diagnostics = mutableListOf<PulsepondDiagnostic>()
        val client = client(transport, diagnostics, batchSize = 1, maxQueueSize = 1)

        assertNotEquals(null, client.track("first"))
        assertNull(client.track("second"))
        assertEquals(PulsepondDiagnosticCode.QueueFull, diagnostics.single().code)
        assertEquals(1, diagnostics.single().droppedEvents)
        client.shutdown()
    }

    @Test
    fun resetDropsPendingEventsAndRotatesAnonymousIdentity() = runTest {
        val transport = FakeTransport(
            FakeOutcome.Response(202),
            FakeOutcome.Response(202),
        )
        val client = client(transport)
        client.track("before_reset")
        client.flush()
        val firstBody = transport.bodies.single()

        client.reset()
        client.track("after_reset")
        client.flush()
        val secondBody = transport.bodies.last()

        assertNotEquals(anonymousId(firstBody), anonymousId(secondBody))
        client.shutdown()
    }
}

private sealed interface FakeOutcome {
    data class Response(val status: Int, val retryAfter: String? = null) : FakeOutcome

    data object Failure : FakeOutcome
}

private open class RecordingTransport : EventTransport {
    val bodies: MutableList<String> = mutableListOf()
    var closed: Boolean = false

    override suspend fun post(body: String): TransportResponse {
        bodies += body
        return TransportResponse(202, null)
    }

    override fun close() {
        closed = true
    }
}

private class FakeTransport(vararg outcomes: FakeOutcome) : RecordingTransport() {
    private val outcomes: ArrayDeque<FakeOutcome> = ArrayDeque(outcomes.toList())

    override suspend fun post(body: String): TransportResponse {
        bodies += body
        return when (val outcome = outcomes.removeFirstOrNull() ?: FakeOutcome.Response(202)) {
            is FakeOutcome.Response -> TransportResponse(outcome.status, outcome.retryAfter)
            FakeOutcome.Failure -> error("network unavailable")
        }
    }
}

private fun client(
    transport: EventTransport,
    diagnostics: MutableList<PulsepondDiagnostic> = mutableListOf(),
    batchSize: Int = 20,
    maxQueueSize: Int = 1_000,
    flushIntervalMilliseconds: Long = 0,
    coroutineScope: kotlinx.coroutines.CoroutineScope? = null,
): Pulsepond {
    var now = 1_800_000_000_000L
    var randomByte = 0
    return Pulsepond(
        configuration = PulsepondConfiguration(
            endpoint = "https://events.example.com/v1/batch",
            writeKey = testWriteKey,
            environment = "production",
            batchSize = batchSize,
            flushIntervalMilliseconds = flushIntervalMilliseconds,
            maxQueueSize = maxQueueSize,
            diagnosticListener = PulsepondDiagnosticListener { diagnostic ->
                diagnostics += diagnostic
            },
        ),
        transport = transport,
        nowMilliseconds = { now++ },
        randomBytes = { bytes ->
            bytes.indices.forEach { index ->
                bytes[index] = randomByte.toByte()
                randomByte = (randomByte + 1) and 0xff
            }
        },
        coroutineScope = coroutineScope,
    )
}

private fun batchSize(body: String): Int =
    Json.parseToJsonElement(body).jsonObject["events"]!!.jsonArray.size

private fun eventIds(body: String): List<String> =
    Json.parseToJsonElement(body).jsonObject["events"]!!.jsonArray.map { event ->
        event.jsonObject["event_id"]!!.jsonPrimitive.content
    }

private fun anonymousId(body: String): String =
    Json.parseToJsonElement(body).jsonObject["events"]!!.jsonArray.single()
        .jsonObject["anonymous_installation_id"]!!.jsonPrimitive.content
