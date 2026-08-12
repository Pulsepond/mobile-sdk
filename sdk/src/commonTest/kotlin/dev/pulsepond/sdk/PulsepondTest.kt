package dev.pulsepond.sdk

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okio.Path.Companion.toPath
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
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
    fun httpDateRetryAfterUsesTheBoundedServerDelay() = runTest {
        val transport = FakeTransport(
            FakeOutcome.Response(429, "Fri, 15 Jan 2027 08:00:20 GMT"),
            FakeOutcome.Response(202),
        )
        val client = client(transport)
        client.track("audio_play")

        client.flush()

        assertEquals(2, transport.bodies.size)
        assertTrue(testScheduler.currentTime in 19_000..20_000)
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

    @Test
    fun aPersistedEventSurvivesClientRecreationAndIsRemovedAfterAcceptance() = runTest {
        val fileSystem = okio.fakefilesystem.FakeFileSystem()
        val directory = "/pulsepond/integration".toPath()
        val first = client(
            transport = FakeTransport(),
            persistence = FileEventPersistence(fileSystem, directory),
            coroutineScope = this,
        )
        first.track("offline_event")
        first.awaitPersistence()

        val delivery = FakeTransport(FakeOutcome.Response(202))
        val restored = client(
            transport = delivery,
            persistence = FileEventPersistence(fileSystem, directory),
            coroutineScope = this,
        )
        assertEquals(1, restored.pendingEventCount())
        restored.flush()

        assertEquals("offline_event", eventName(delivery.bodies.single()))
        val afterDelivery = client(
            transport = FakeTransport(),
            persistence = FileEventPersistence(fileSystem, directory),
            coroutineScope = this,
        )
        assertEquals(0, afterDelivery.pendingEventCount())
        restored.shutdown()
        afterDelivery.shutdown()
        first.reset()
        first.shutdown()
        fileSystem.checkNoOpenFiles()
    }

    @Test
    fun appendFailureKeepsTheEventInMemoryAndEmitsARedactedDiagnostic() = runTest {
        val diagnostics = mutableListOf<PulsepondDiagnostic>()
        val transport = FakeTransport(FakeOutcome.Response(202))
        val client = client(
            transport = transport,
            diagnostics = diagnostics,
            persistence = FailingPersistence(failAppend = true),
        )

        client.track("app_open")
        client.flush()
        client.awaitPersistence()

        assertEquals("app_open", eventName(transport.bodies.single()))
        assertEquals(PulsepondDiagnosticCode.StorageFailed, diagnostics.single().code)
        assertEquals(0, diagnostics.single().droppedEvents)
        client.shutdown()
    }

    @Test
    fun trackQueuesPersistenceWithoutCallingStorageInline() = runTest {
        val persistence = FailingPersistence()
        val client = client(
            transport = FakeTransport(),
            persistence = persistence,
            coroutineScope = this,
        )

        client.track("app_open")

        assertEquals(0, persistence.appendCalls)
        client.awaitPersistence()
        assertEquals(1, persistence.appendCalls)
        client.shutdown()
    }

    @Test
    fun failedDurableResetLeavesTheCurrentQueueAndIdentityUntouched() = runTest {
        val transport = FakeTransport(FakeOutcome.Response(202))
        val client = client(
            transport = transport,
            persistence = FailingPersistence(failReset = true),
        )
        client.track("before_reset")

        assertFailsWith<PulsepondStorageException> { client.reset() }
        client.track("after_failed_reset")
        client.flush()

        val events = Json.parseToJsonElement(transport.bodies.single())
            .jsonObject["events"]!!.jsonArray.map { it.jsonObject }
        assertEquals(listOf("before_reset", "after_failed_reset"), events.map {
            it["event_name"]!!.jsonPrimitive.content
        })
        assertEquals(1, events.map {
            it["anonymous_installation_id"]!!.jsonPrimitive.content
        }.distinct().size)
        client.shutdown()
    }

    @Test
    fun retryExhaustionDefersRatherThanDropsTheDurableQueue() = runTest {
        val fileSystem = okio.fakefilesystem.FakeFileSystem()
        val directory = "/pulsepond/retry".toPath()
        val diagnostics = mutableListOf<PulsepondDiagnostic>()
        val client = client(
            transport = FakeTransport(
                FakeOutcome.Failure,
                FakeOutcome.Failure,
                FakeOutcome.Failure,
                FakeOutcome.Failure,
                FakeOutcome.Failure,
                FakeOutcome.Failure,
            ),
            diagnostics = diagnostics,
            persistence = FileEventPersistence(fileSystem, directory),
            coroutineScope = this,
        )
        client.track("offline_event")

        client.flush()

        assertEquals(1, client.pendingEventCount())
        client.awaitPersistence()
        val exhausted = diagnostics.last()
        assertEquals(PulsepondDiagnosticCode.RetryExhausted, exhausted.code)
        assertEquals(0, exhausted.droppedEvents)
        assertTrue(exhausted.retryable)
        val delivery = FakeTransport(FakeOutcome.Response(202))
        val restored = client(
            transport = delivery,
            persistence = FileEventPersistence(fileSystem, directory),
            coroutineScope = this,
        )
        restored.flush()
        assertEquals("offline_event", eventName(delivery.bodies.single()))
        restored.shutdown()
        client.reset()
        client.shutdown()
        fileSystem.checkNoOpenFiles()
    }

    @Test
    fun resetCancelsAnInFlightBodyAndNeverRetriesIt() = runTest {
        val transport = ResetAwareTransport()
        val client = client(transport)
        client.track("before_reset")
        val flush = launch { client.flush() }
        transport.firstStarted.await()

        client.reset()
        client.track("after_reset")
        flush.join()
        client.flush()

        transport.firstCancelled.await()
        assertEquals(listOf("before_reset", "after_reset"), transport.bodies.map(::eventName))
        assertEquals(0, client.pendingEventCount())
        client.shutdown()
    }

    @Test
    fun cancelledShutdownStillClosesAndConcurrentCallersJoinIt() = runTest {
        val transport = BlockingTransport()
        val client = client(transport)
        client.track("pending")
        val first = launch { client.shutdown() }
        transport.started.await()
        val second = async { client.shutdown() }
        yield()
        assertFalse(second.isCompleted)

        first.cancelAndJoin()
        second.await()

        assertTrue(transport.closed)
        assertFailsWith<PulsepondValidationException> { client.track("too_late") }
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

private class ResetAwareTransport : RecordingTransport() {
    val firstStarted: CompletableDeferred<Unit> = CompletableDeferred()
    val firstCancelled: CompletableDeferred<Unit> = CompletableDeferred()

    override suspend fun post(body: String): TransportResponse {
        bodies += body
        if (bodies.size == 1) {
            firstStarted.complete(Unit)
            try {
                awaitCancellation()
            } finally {
                firstCancelled.complete(Unit)
            }
        }
        return TransportResponse(202, null)
    }
}

private class BlockingTransport : RecordingTransport() {
    val started: CompletableDeferred<Unit> = CompletableDeferred()

    override suspend fun post(body: String): TransportResponse {
        bodies += body
        started.complete(Unit)
        awaitCancellation()
    }
}

private class FailingPersistence(
    private val failAppend: Boolean = false,
    private val failReset: Boolean = false,
) : EventPersistence {
    override val isDurable: Boolean = true

    private var installationId: String? = null
    private val events: MutableList<EventRecord> = mutableListOf()
    var appendCalls: Int = 0
        private set

    override fun load(newInstallationId: () -> String): PersistedState {
        val activeId = installationId ?: newInstallationId().also { installationId = it }
        return PersistedState(activeId, events.toList())
    }

    override fun append(event: EventRecord, currentEvents: List<EventRecord>) {
        appendCalls += 1
        if (failAppend) throw PulsepondStorageException("test append failure")
        events += event
    }

    override fun replace(events: List<EventRecord>) {
        this.events.clear()
        this.events += events
    }

    override fun reset(newInstallationId: String) {
        if (failReset) throw PulsepondStorageException("test reset failure")
        installationId = newInstallationId
        events.clear()
    }
}

private fun client(
    transport: EventTransport,
    diagnostics: MutableList<PulsepondDiagnostic> = mutableListOf(),
    batchSize: Int = 20,
    maxQueueSize: Int = 1_000,
    flushIntervalMilliseconds: Long = 0,
    coroutineScope: kotlinx.coroutines.CoroutineScope? = null,
    persistence: EventPersistence = VolatileEventPersistence,
): Pulsepond {
    var now = 1_800_000_000_000L
    var randomByte = 0
    return Pulsepond(
        configuration = PulsepondConfiguration(
            endpoint = "https://events.example.com/v1/batch",
            writeKey = testWriteKey,
            sourceId = testSourceId,
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
        persistence = persistence,
        coroutineScope = coroutineScope,
    )
}

private fun batchSize(body: String): Int =
    Json.parseToJsonElement(body).jsonObject["events"]!!.jsonArray.size

private fun eventIds(body: String): List<String> =
    Json.parseToJsonElement(body).jsonObject["events"]!!.jsonArray.map { event ->
        event.jsonObject["event_id"]!!.jsonPrimitive.content
    }

private fun eventName(body: String): String =
    Json.parseToJsonElement(body).jsonObject["events"]!!.jsonArray.single()
        .jsonObject["event_name"]!!.jsonPrimitive.content

private fun anonymousId(body: String): String =
    Json.parseToJsonElement(body).jsonObject["events"]!!.jsonArray.single()
        .jsonObject["anonymous_installation_id"]!!.jsonPrimitive.content
