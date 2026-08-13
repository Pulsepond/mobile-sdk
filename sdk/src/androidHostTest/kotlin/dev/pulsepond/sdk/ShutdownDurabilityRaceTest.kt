package dev.pulsepond.sdk

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ShutdownDurabilityRaceTest {
    @Test
    fun earlierSnapshotSuccessSupersedesCancelledShutdownsFailedDuplicate() = runTest {
        val persistence = BlockingFirstReplacePersistence()
        val diagnostics = mutableListOf<PulsepondDiagnostic>()
        val clientScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val shutdownDispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
        val client = Pulsepond(
            configuration = PulsepondConfiguration(
                endpoint = "https://events.example.com/v1/batch",
                writeKey = testWriteKey,
                deploymentId = testDeploymentId,
                projectId = testProjectId,
                sourceId = testSourceId,
                environment = "shutdown-race",
                flushIntervalMilliseconds = 0,
                diagnosticListener = PulsepondDiagnosticListener(diagnostics::add),
            ),
            transport = NoopTransport,
            nowMilliseconds = { 1_800_000_000_000L },
            randomBytes = { bytes -> bytes.fill(1) },
            persistence = persistence,
            automaticallyStart = false,
            coroutineScope = clientScope,
        )
        client.track("pending")
        val initiatorFailure = CompletableDeferred<Throwable?>()
        try {
            val initiator = launch(shutdownDispatcher) {
                try {
                    client.shutdown()
                    initiatorFailure.complete(null)
                } catch (error: Throwable) {
                    initiatorFailure.complete(error)
                }
            }
            assertTrue(persistence.firstReplaceStarted.await(5, TimeUnit.SECONDS))
            val waiter = async(shutdownDispatcher) { runCatching { client.shutdown() } }
            withContext(shutdownDispatcher) {}

            initiator.cancel()
            withContext(shutdownDispatcher) {}
            persistence.releaseFirstReplace.countDown()

            initiator.join()
            assertTrue(initiatorFailure.await() is CancellationException)
            assertTrue(waiter.await().isSuccess)
            assertEquals(2, persistence.replaceCalls.get())
            assertFalse(
                diagnostics.any {
                    it.code == PulsepondDiagnosticCode.StorageFailed ||
                        it.code == PulsepondDiagnosticCode.DeliveryFailed
                },
            )
        } finally {
            persistence.releaseFirstReplace.countDown()
            clientScope.cancel()
            shutdownDispatcher.close()
        }
    }
}

private object NoopTransport : EventTransport {
    override suspend fun post(body: String): TransportResponse = TransportResponse(202, null)

    override fun close(): Unit = Unit
}

private class BlockingFirstReplacePersistence : EventPersistence {
    override val isDurable: Boolean = true
    val firstReplaceStarted: CountDownLatch = CountDownLatch(1)
    val releaseFirstReplace: CountDownLatch = CountDownLatch(1)
    val replaceCalls: AtomicInteger = AtomicInteger()

    override fun load(newInstallationId: () -> String): PersistedState =
        PersistedState(newInstallationId(), emptyList())

    override fun append(
        event: EventRecord,
        currentEvents: List<EventRecord>,
    ): AppendPersistenceResult = AppendPersistenceResult.AppendedEvent

    override fun replace(events: List<EventRecord>) {
        when (replaceCalls.incrementAndGet()) {
            1 -> {
                firstReplaceStarted.countDown()
                check(releaseFirstReplace.await(5, TimeUnit.SECONDS)) {
                    "timed out waiting to release the first replacement"
                }
            }
            2 -> throw PulsepondStorageException("test duplicate replacement failure")
            else -> error("unexpected replacement")
        }
    }

    override fun reset(newInstallationId: String): PersistenceResetResult =
        PersistenceResetResult(completed = true)
}
