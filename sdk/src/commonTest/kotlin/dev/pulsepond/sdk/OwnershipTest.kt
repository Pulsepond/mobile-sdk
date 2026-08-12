package dev.pulsepond.sdk

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFailsWith

class OwnershipTest {
    @Test
    fun oneNamespaceHasOnlyOneLiveProcessOwner() {
        val namespace = "${testSourceId}/ownership-test"
        val first = ClientOwnershipRegistry.acquire(namespace)
        try {
            assertFailsWith<PulsepondStorageException> {
                ClientOwnershipRegistry.acquire(namespace)
            }
        } finally {
            first.release()
        }

        ClientOwnershipRegistry.acquire(namespace).release()
    }

    @Test
    fun clientShutdownReleasesItsNamespaceLease() = runTest {
        val environment = "shutdown-ownership-test"
        val namespace = "$testSourceId/$environment"
        val lease = ClientOwnershipRegistry.acquire(namespace)
        val client = Pulsepond(
            configuration = PulsepondConfiguration(
                endpoint = "https://events.example.com/v1/batch",
                writeKey = testWriteKey,
                sourceId = testSourceId,
                environment = environment,
                flushIntervalMilliseconds = 0,
            ),
            transport = object : EventTransport {
                override suspend fun post(body: String): TransportResponse =
                    TransportResponse(202, null)

                override fun close() = Unit
            },
            nowMilliseconds = { 1_800_000_000_000L },
            randomBytes = { bytes -> bytes.fill(0) },
            ownershipLease = lease,
            coroutineScope = this,
        )

        client.shutdown()

        ClientOwnershipRegistry.acquire(namespace).release()
    }
}
