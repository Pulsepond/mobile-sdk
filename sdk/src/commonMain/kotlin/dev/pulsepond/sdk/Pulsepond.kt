package dev.pulsepond.sdk

import kotlinx.atomicfu.locks.SynchronizedObject
import kotlinx.atomicfu.locks.synchronized
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Clock

private const val maxPropertiesJsonBytes: Int = 20_000
private const val maxRetries: Int = 5
private const val maxRetryDelayMilliseconds: Long = 30_000
private const val retryBaseDelayMilliseconds: Long = 1_000
private const val maxBatchesPerFlush: Int = 100

private data class QueuedEvent(
    val event: EventRecord,
    val serializedBytes: Int,
)

private data class Batch(
    val events: List<QueuedEvent>,
    val body: String,
    val generation: Long,
)

private sealed interface DeliveryResult {
    data object Accepted : DeliveryResult

    data object TooLarge : DeliveryResult

    data class Rejected(val status: Int) : DeliveryResult

    data object RetryExhausted : DeliveryResult
}

/**
 * Sends explicit product events to one source-scoped Pulsepond ingestion endpoint.
 *
 * Events and identifiers are memory-only in 0.1. Call [shutdown] from the host application's
 * lifecycle when a final best-effort flush is appropriate.
 */
public class Pulsepond internal constructor(
    private val configuration: PulsepondConfiguration,
    private val transport: EventTransport,
    private val nowMilliseconds: () -> Long,
    private val randomBytes: (ByteArray) -> Unit,
    coroutineScope: CoroutineScope? = null,
) {
    public constructor(configuration: PulsepondConfiguration) : this(
        configuration = configuration,
        transport = KtorEventTransport(configuration),
        nowMilliseconds = { Clock.System.now().toEpochMilliseconds() },
        randomBytes = ::fillSecureRandom,
        coroutineScope = null,
    )

    private val stateLock: SynchronizedObject = SynchronizedObject()
    private val flushLock: Mutex = Mutex()
    private val ownsScope: Boolean = coroutineScope == null
    private val scope: CoroutineScope =
        coroutineScope ?: CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val queue: MutableList<QueuedEvent> = mutableListOf()
    private val identity: IdentityManager = IdentityManager(randomBytes, nowMilliseconds())
    private var queueBytes: Int = 0
    private var generation: Long = 0
    private var effectiveBatchSize: Int = configuration.batchSize
    private var automaticFlushScheduled: Boolean = false
    private var immediateFlushScheduled: Boolean = false
    private var closing: Boolean = false
    private var closed: Boolean = false

    /** Enqueues an event and returns its UUIDv7, or null when the bounded queue is full. */
    public fun track(eventName: String): String? = track(eventName, null)

    /** Enqueues an event with a defensive copy of its bounded, flat properties. */
    public fun track(eventName: String, properties: PulsepondProperties?): String? {
        validateSlug("eventName", eventName, 64)
        val propertySnapshot = properties?.snapshot().orEmpty()
        validatePropertyBytes(propertySnapshot)
        val now = nowMilliseconds()
        var requestTimedFlush = false
        var requestImmediateFlush = false
        val eventId = synchronized(stateLock) {
            if (closing || closed) {
                throw PulsepondValidationException(
                    "Pulsepond cannot track after shutdown has started",
                )
            }
            if (queue.size >= configuration.maxQueueSize || queueBytes >= maxQueueBytes) {
                null
            } else {
                val currentIdentity = identity.current(now)
                val id = createUuidV7(now, randomBytes)
                val event = EventRecord(
                    eventId = id,
                    eventName = eventName,
                    occurredAtMilliseconds = now,
                    occurredAt = occurredAt(now),
                    platform = pulsepondPlatform,
                    appVersion = configuration.appVersion,
                    release = configuration.release,
                    environment = configuration.environment,
                    anonymousInstallationId = currentIdentity.anonymousInstallationId,
                    sessionId = currentIdentity.sessionId,
                    properties = propertySnapshot,
                )
                val serializedBytes = event.json().toString().encodeToByteArray().size
                if (queueBytes + serializedBytes > maxQueueBytes) {
                    null
                } else {
                    queue += QueuedEvent(event, serializedBytes)
                    queueBytes += serializedBytes
                    if (queue.size >= effectiveBatchSize) {
                        if (!immediateFlushScheduled) {
                            immediateFlushScheduled = true
                            requestImmediateFlush = true
                        }
                    } else if (
                        !automaticFlushScheduled &&
                        configuration.flushIntervalMilliseconds > 0
                    ) {
                        automaticFlushScheduled = true
                        requestTimedFlush = true
                    }
                    id
                }
            }
        }
        if (eventId == null) {
            notify(
                PulsepondDiagnostic(
                    code = PulsepondDiagnosticCode.QueueFull,
                    droppedEvents = 1,
                    retryable = false,
                ),
            )
            return null
        }
        if (requestImmediateFlush) scheduleImmediateFlush()
        if (requestTimedFlush) scheduleTimedFlush()
        return eventId
    }

    /** Attempts to deliver every event that can be processed within bounded batches. */
    public suspend fun flush(): Unit = flushLock.withLock {
        if (synchronized(stateLock) { closed }) return@withLock
        val staleEvents = dropStaleEvents()
        if (staleEvents > 0) {
            notify(
                PulsepondDiagnostic(
                    code = PulsepondDiagnosticCode.StaleEvent,
                    droppedEvents = staleEvents,
                    retryable = false,
                ),
            )
        }
        repeat(maxBatchesPerFlush) {
            val batch = nextBatch() ?: return@withLock
            when (val result = deliver(batch)) {
                DeliveryResult.Accepted -> removeBatch(batch)
                DeliveryResult.TooLarge -> {
                    if (batch.events.size > 1) {
                        synchronized(stateLock) {
                            effectiveBatchSize = maxOf(1, batch.events.size / 2)
                        }
                    } else {
                        removeBatch(batch)
                        notify(
                            PulsepondDiagnostic(
                                code = PulsepondDiagnosticCode.BatchRejected,
                                droppedEvents = 1,
                                retryable = false,
                                status = 413,
                            ),
                        )
                    }
                }
                is DeliveryResult.Rejected -> {
                    removeBatch(batch)
                    notify(
                        PulsepondDiagnostic(
                            code = PulsepondDiagnosticCode.BatchRejected,
                            droppedEvents = batch.events.size,
                            retryable = false,
                            status = result.status,
                        ),
                    )
                }
                DeliveryResult.RetryExhausted -> {
                    removeBatch(batch)
                    notify(
                        PulsepondDiagnostic(
                            code = PulsepondDiagnosticCode.RetryExhausted,
                            droppedEvents = batch.events.size,
                            retryable = false,
                        ),
                    )
                }
            }
        }
    }

    /** Discards unsent events and rotates the in-memory installation and session identifiers. */
    public fun reset() {
        val now = nowMilliseconds()
        synchronized(stateLock) {
            if (closed) return
            generation += 1
            queue.clear()
            queueBytes = 0
            effectiveBatchSize = configuration.batchSize
            identity.reset(now)
        }
    }

    /** Makes one final bounded delivery attempt and permanently closes the client. */
    public suspend fun shutdown() {
        val shouldClose = synchronized(stateLock) {
            if (closing || closed) {
                false
            } else {
                closing = true
                true
            }
        }
        if (!shouldClose) return
        flush()
        val droppedEvents = synchronized(stateLock) {
            val count = queue.size
            queue.clear()
            queueBytes = 0
            closed = true
            closing = false
            count
        }
        if (droppedEvents > 0) {
            notify(
                PulsepondDiagnostic(
                    code = PulsepondDiagnosticCode.DeliveryFailed,
                    droppedEvents = droppedEvents,
                    retryable = false,
                ),
            )
        }
        transport.close()
        if (ownsScope) scope.cancel()
    }

    internal fun pendingEventCount(): Int = synchronized(stateLock) { queue.size }

    private fun scheduleImmediateFlush() {
        scope.launch {
            try {
                flush()
            } finally {
                val flushAgain = synchronized(stateLock) {
                    immediateFlushScheduled = false
                    if (queue.size >= effectiveBatchSize && !closing && !closed) {
                        immediateFlushScheduled = true
                        true
                    } else {
                        false
                    }
                }
                if (flushAgain) scheduleImmediateFlush()
            }
        }
    }

    private fun scheduleTimedFlush() {
        scope.launch {
            delay(configuration.flushIntervalMilliseconds)
            try {
                flush()
            } finally {
                val scheduleAgain = synchronized(stateLock) {
                    automaticFlushScheduled = false
                    if (
                        queue.isNotEmpty() &&
                        queue.size < effectiveBatchSize &&
                        !closing &&
                        !closed
                    ) {
                        automaticFlushScheduled = true
                        true
                    } else {
                        false
                    }
                }
                if (scheduleAgain) scheduleTimedFlush()
            }
        }
    }

    private fun validatePropertyBytes(properties: Map<String, EventPropertyValue>) {
        val propertyEvent = EventRecord(
            eventId = "00000000-0000-7000-8000-000000000000",
            eventName = "property_size",
            occurredAtMilliseconds = 0,
            occurredAt = "1970-01-01T00:00:00Z",
            platform = "other",
            appVersion = null,
            release = null,
            environment = "test",
            anonymousInstallationId = "installation",
            sessionId = "session",
            properties = properties,
        )
        val bytes = propertyEvent.json()["properties"].toString().encodeToByteArray().size
        if (bytes > maxPropertiesJsonBytes) {
            throw PulsepondValidationException(
                "properties must serialize within $maxPropertiesJsonBytes bytes",
            )
        }
    }

    private fun dropStaleEvents(): Int = synchronized(stateLock) {
        val cutoff = nowMilliseconds() - configuration.eventTtlMilliseconds
        var dropped = 0
        val iterator = queue.iterator()
        while (iterator.hasNext()) {
            val item = iterator.next()
            if (item.event.occurredAtMilliseconds < cutoff) {
                queueBytes -= item.serializedBytes
                iterator.remove()
                dropped += 1
            }
        }
        dropped
    }

    private fun nextBatch(): Batch? = synchronized(stateLock) {
        if (queue.isEmpty()) return@synchronized null
        val selected = mutableListOf<QueuedEvent>()
        var body = ""
        for (item in queue) {
            if (selected.size >= effectiveBatchSize) break
            val candidate = selected + item
            val candidateBody = eventBatchJson(candidate.map { it.event })
            if (candidateBody.encodeToByteArray().size > maxBatchBytes && selected.isNotEmpty()) {
                break
            }
            selected += item
            body = candidateBody
        }
        Batch(selected, body, generation)
    }

    private suspend fun deliver(batch: Batch): DeliveryResult {
        var attempts = 0
        while (true) {
            val response = try {
                transport.post(batch.body)
            } catch (error: CancellationException) {
                throw error
            } catch (_: Throwable) {
                null
            }
            if (response?.status == 202) return DeliveryResult.Accepted
            if (response?.status == 413) return DeliveryResult.TooLarge
            if (
                response != null &&
                response.status != 408 &&
                response.status != 429 &&
                response.status < 500
            ) {
                return DeliveryResult.Rejected(response.status)
            }
            attempts += 1
            if (attempts > maxRetries) return DeliveryResult.RetryExhausted
            notify(
                PulsepondDiagnostic(
                    code = PulsepondDiagnosticCode.DeliveryFailed,
                    droppedEvents = 0,
                    retryable = true,
                    status = response?.status,
                ),
            )
            delay(retryDelayMilliseconds(attempts, response?.retryAfter))
        }
    }

    private fun retryDelayMilliseconds(attempt: Int, retryAfter: String?): Long {
        val trimmedRetryAfter = retryAfter?.trim()
        if (trimmedRetryAfter?.all(Char::isDigit) == true) {
            val retryAfterSeconds = trimmedRetryAfter.toLongOrNull()
            return if (retryAfterSeconds == null) {
                maxRetryDelayMilliseconds
            } else {
                retryAfterSeconds.coerceAtMost(maxRetryDelayMilliseconds / 1_000) * 1_000
            }
        }
        val ceiling = minOf(
            maxRetryDelayMilliseconds,
            retryBaseDelayMilliseconds * (1L shl (attempt - 1)),
        )
        val random = ByteArray(2)
        randomBytes(random)
        val value = ((random[0].toInt() and 0xff) shl 8) or (random[1].toInt() and 0xff)
        return value.toLong() * ceiling / 0xffff
    }

    private fun removeBatch(batch: Batch) {
        synchronized(stateLock) {
            if (batch.generation != generation || queue.size < batch.events.size) return
            val expectedIds = batch.events.map { it.event.eventId }
            val actualIds = queue.take(expectedIds.size).map { it.event.eventId }
            if (expectedIds != actualIds) return
            repeat(batch.events.size) {
                queueBytes -= queue.removeAt(0).serializedBytes
            }
        }
    }

    private fun notify(diagnostic: PulsepondDiagnostic) {
        try {
            configuration.diagnosticListener?.onDiagnostic(diagnostic)
        } catch (_: Throwable) {
            // Consumer callbacks cannot break delivery or receive event data.
        }
    }
}
