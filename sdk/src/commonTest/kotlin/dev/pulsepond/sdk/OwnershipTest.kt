package dev.pulsepond.sdk

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFailsWith

class OwnershipTest {
    @Test
    fun oneNamespaceHasOnlyOneLiveProcessOwner() {
        val namespace = "$testDeploymentId/$testProjectId/$testSourceId/ownership-test"
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
        val namespace = "$testDeploymentId/$testProjectId/$testSourceId/$environment"
        val lease = ClientOwnershipRegistry.acquire(namespace)
        val client = Pulsepond(
            configuration = PulsepondConfiguration(
                endpoint = "https://events.example.com/v1/batch",
                writeKey = testWriteKey,
                deploymentId = testDeploymentId,
                projectId = testProjectId,
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

    @Test
    fun cancelledFactoryCreationReleasesItsNamespaceLease() = runTest {
        val configuration = PulsepondConfiguration(
            endpoint = "https://events.example.com/v1/batch",
            writeKey = testWriteKey,
            deploymentId = testDeploymentId,
            projectId = testProjectId,
            sourceId = testSourceId,
            environment = "cancelled-factory-test",
            flushIntervalMilliseconds = 0,
        )

        assertFailsWith<CancellationException> {
            createPulsepondInBackground(
                create = {
                    createOwnedPersistentPulsepond(
                        configuration,
                        VolatileEventPersistence,
                        startAutomaticDelivery = false,
                    )
                },
                afterCreate = { throw CancellationException("test cancellation") },
            )
        }

        ClientOwnershipRegistry.acquire(configuration.storageNamespace).release()
    }
}
