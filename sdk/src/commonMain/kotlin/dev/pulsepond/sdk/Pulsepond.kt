package dev.pulsepond.sdk

import io.ktor.http.fromHttpToGmtDate
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.locks.SynchronizedObject
import kotlinx.atomicfu.locks.synchronized
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.selects.select
import kotlin.time.Clock

private const val maxRetries: Int = 5
private const val maxRetryDelayMilliseconds: Long = 30_000
private const val retryBaseDelayMilliseconds: Long = 1_000
private const val maxBatchesPerFlush: Int = 100

private data class QueuedEvent(
    val event: EventRecord,
    val serializedBytes: Int,
)

private data class QueueMutation(
    val eventCount: Int,
    val storageFailed: Boolean,
)

private data class QueueSnapshot(
    val events: List<EventRecord>,
    val revision: Long,
)

private data class PendingQueueSnapshot(
    val events: List<EventRecord>,
    val revision: Long,
    val completion: CompletableDeferred<Boolean>?,
)

private data class Batch(
    val events: List<QueuedEvent>,
    val body: String,
    val generation: Long,
    val invalidation: Deferred<Unit>,
)

private sealed interface DeliveryResult {
    data object Accepted : DeliveryResult

    data object TooLarge : DeliveryResult

    data class Rejected(val status: Int) : DeliveryResult

    data object RetryExhausted : DeliveryResult

    data object Invalidated : DeliveryResult
}

private sealed interface TransportAttempt {
    data class Completed(val response: TransportResponse) : TransportAttempt

    data object Failed : TransportAttempt

    data object Invalidated : TransportAttempt
}

/**
 * Sends explicit product events to one source-scoped Pulsepond ingestion endpoint.
 *
 * Construct this client through the platform factory so identity and unsent events use
 * app-private durable storage.
 */
public class Pulsepond internal constructor(
    private val configuration: PulsepondConfiguration,
    private val transport: EventTransport,
    private val nowMilliseconds: () -> Long,
    private val randomBytes: (ByteArray) -> Unit,
    private val persistence: EventPersistence = VolatileEventPersistence,
    private val ownershipLease: ClientOwnershipLease? = null,
    private val automaticallyStart: Boolean = true,
    coroutineScope: CoroutineScope? = null,
) {
    private val stateLock: SynchronizedObject = SynchronizedObject()
    private val flushLock: Mutex = Mutex()
    private val ownsScope: Boolean = coroutineScope == null
    private val scope: CoroutineScope =
        coroutineScope ?: CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val startupTimeMilliseconds: Long = nowMilliseconds()
    private val persistedState: PersistedState = persistence.load {
        createUuidV7(startupTimeMilliseconds, randomBytes)
    }
    private val queue: MutableList<QueuedEvent> = mutableListOf<QueuedEvent>().apply {
        var restoredBytes = 0
        for (event in persistedState.events) {
            val serializedBytes = event.json().toString().encodeToByteArray().size
            if (
                size >= configuration.maxQueueSize ||
                restoredBytes + serializedBytes > maxQueueBytes ||
                event.environment != configuration.environment
            ) {
                continue
            }
            add(QueuedEvent(event, serializedBytes))
            restoredBytes += serializedBytes
        }
    }
    private val identity: IdentityManager = IdentityManager(
        randomBytes,
        startupTimeMilliseconds,
        persistedState.installationId,
    )
    private var queueBytes: Int = queue.sumOf(QueuedEvent::serializedBytes)
    private var queueRevision: Long = 0
    private var durableQueueRevision: Long = if (
        persistence.isDurable && persistedState.events.size == queue.size
    ) {
        queueRevision
    } else {
        -1
    }
    private val durableEventIds: MutableSet<String> = if (persistence.isDurable) {
        queue.mapTo(mutableSetOf()) { it.event.eventId }
    } else {
        mutableSetOf()
    }
    private var generation: Long = 0
    private var generationInvalidation: CompletableDeferred<Unit> = CompletableDeferred()
    private var effectiveBatchSize: Int = configuration.batchSize
    private var automaticFlushScheduled: Boolean = false
    private var immediateFlushScheduled: Boolean = false
    private var deliveryDeferred: Boolean = false
    private var resetting: Boolean = false
    private var closing: Boolean = false
    private var closed: Boolean = false
    private var shutdownCompletion: CompletableDeferred<Throwable?>? = null
    private var automaticDeliveryStarted: Boolean = false
    private val persistenceWriter: PersistenceWriter = PersistenceWriter(
        persistence = persistence,
        initialEvents = queue.map(QueuedEvent::event),
        scope = scope,
        capacity = configuration.maxQueueSize + 16,
        onFailure = ::notifyStorageFailure,
        onAppendPersisted = ::markEventDurable,
        onSnapshotPersisted = ::markSnapshotDurable,
    )

    init {
        val discardedRecords = persistedState.events.size - queue.size
        if (discardedRecords > 0) {
            try {
                persistence.replace(queue.map(QueuedEvent::event))
                durableQueueRevision = queueRevision
            } catch (_: PulsepondStorageException) {
                notifyStorageFailure()
            }
        }
        val recoveredRecords = persistedState.recoveredRecords + discardedRecords
        if (recoveredRecords > 0) {
            notify(
                PulsepondDiagnostic(
                    code = PulsepondDiagnosticCode.StorageRecovered,
                    droppedEvents = recoveredRecords,
                    retryable = false,
                ),
            )
        }
        if (automaticallyStart) startAutomaticDelivery()
    }

    /** Enqueues an event and returns its UUIDv7, or null when the bounded queue is full. */
    @Throws(PulsepondValidationException::class, PulsepondConfigurationException::class)
    public fun track(eventName: String): String? = track(eventName, null)

    /** Enqueues an event with a defensive copy of its bounded, flat properties. */
    @Throws(PulsepondValidationException::class, PulsepondConfigurationException::class)
    public fun track(eventName: String, properties: PulsepondProperties?): String? {
        validateSlug("eventName", eventName, 64)
        val propertySnapshot = properties?.snapshot().orEmpty()
        val now = nowMilliseconds()
        var requestTimedFlush = false
        var requestImmediateFlush = false
        var storageFailed = false
        val eventId = synchronized(stateLock) {
            if (resetting || closing || closed) {
                throw PulsepondValidationException(
                    "Pulsepond cannot track while reset or shutdown is in progress",
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
                ).also(::validateEventRecord)
                val serializedBytes = event.json().toString().encodeToByteArray().size
                if (queueBytes + serializedBytes > maxQueueBytes) {
                    null
                } else {
                    storageFailed = !persistenceWriter.tryAppend(event)
                    queue += QueuedEvent(event, serializedBytes)
                    queueBytes += serializedBytes
                    queueRevision += 1
                    deliveryDeferred = false
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
        if (storageFailed) notifyStorageFailure()
        if (requestImmediateFlush) scheduleImmediateFlush()
        if (requestTimedFlush) scheduleTimedFlush()
        return eventId
    }

    /** Attempts to deliver every event that can be processed within bounded batches. */
    @Throws(CancellationException::class)
    public suspend fun flush(): Unit = flushLock.withLock {
        val isClosed = synchronized(stateLock) {
            if (!closed) deliveryDeferred = false
            closed
        }
        if (isClosed) return@withLock
        val staleEvents = dropStaleEvents()
        if (staleEvents.storageFailed) notifyStorageFailure()
        if (staleEvents.eventCount > 0) {
            notify(
                PulsepondDiagnostic(
                    code = PulsepondDiagnosticCode.StaleEvent,
                    droppedEvents = staleEvents.eventCount,
                    retryable = false,
                ),
            )
        }
        repeat(maxBatchesPerFlush) {
            val batch = nextBatch() ?: return@withLock
            if (!persistCurrentQueue()) {
                synchronized(stateLock) { deliveryDeferred = true }
                notifyStorageFailure()
                return@withLock
            }
            when (val result = deliver(batch)) {
                DeliveryResult.Accepted -> {
                    if (removeBatch(batch)) notifyStorageFailure()
                }
                DeliveryResult.TooLarge -> {
                    if (batch.events.size > 1) {
                        synchronized(stateLock) {
                            effectiveBatchSize = maxOf(1, batch.events.size / 2)
                        }
                    } else {
                        val storageFailed = removeBatch(batch)
                        notify(
                            PulsepondDiagnostic(
                                code = PulsepondDiagnosticCode.BatchRejected,
                                droppedEvents = 1,
                                retryable = false,
                                status = 413,
                            ),
                        )
                        if (storageFailed) notifyStorageFailure()
                    }
                }
                is DeliveryResult.Rejected -> {
                    val storageFailed = removeBatch(batch)
                    notify(
                        PulsepondDiagnostic(
                            code = PulsepondDiagnosticCode.BatchRejected,
                            droppedEvents = batch.events.size,
                            retryable = false,
                            status = result.status,
                        ),
                    )
                    if (storageFailed) notifyStorageFailure()
                }
                DeliveryResult.RetryExhausted -> {
                    synchronized(stateLock) { deliveryDeferred = true }
                    notify(
                        PulsepondDiagnostic(
                            code = PulsepondDiagnosticCode.RetryExhausted,
                            droppedEvents = 0,
                            retryable = true,
                        ),
                    )
                    return@withLock
                }
                DeliveryResult.Invalidated -> Unit
            }
        }
    }

    /** Discards unsent events and atomically rotates the installation and session identifiers. */
    @Throws(
        CancellationException::class,
        PulsepondConfigurationException::class,
        PulsepondStorageException::class,
        PulsepondValidationException::class,
    )
    public suspend fun reset() {
        val now = nowMilliseconds()
        val newInstallationId = synchronized(stateLock) {
            if (closed) return
            if (resetting || closing) {
                throw PulsepondValidationException(
                    "Pulsepond cannot reset while reset or shutdown is in progress",
                )
            }
            val installationId = createUuidV7(now, randomBytes)
            resetting = true
            generationInvalidation.complete(Unit)
            installationId
        }
        var resetCompleted = true
        try {
            withContext(NonCancellable) {
                flushLock.withLock {
                    resetCompleted = persistenceWriter.reset(newInstallationId).completed
                    synchronized(stateLock) {
                        generation += 1
                        generationInvalidation = CompletableDeferred()
                        queue.clear()
                        queueBytes = 0
                        queueRevision += 1
                        durableQueueRevision = queueRevision
                        durableEventIds.clear()
                        effectiveBatchSize = configuration.batchSize
                        deliveryDeferred = false
                        identity.reset(now, newInstallationId)
                        resetting = false
                    }
                }
            }
        } catch (error: Throwable) {
            synchronized(stateLock) {
                generationInvalidation = CompletableDeferred()
                resetting = false
            }
            throw error
        }
        if (!resetCompleted) {
            notifyStorageFailure()
            throw PulsepondStorageException(
                "Pulsepond committed identity reset but could not finish durable cleanup",
            )
        }
    }

    /** Makes one final bounded delivery attempt, closes the client, and joins concurrent callers. */
    @Throws(CancellationException::class, PulsepondStorageException::class)
    public suspend fun shutdown() {
        var initiatesShutdown = false
        val completion = synchronized(stateLock) {
            shutdownCompletion ?: CompletableDeferred<Throwable?>().also {
                shutdownCompletion = it
                closing = true
                initiatesShutdown = true
            }
        }
        if (!initiatesShutdown) {
            completion.await()?.let { throw it }
            return
        }

        var cancellation: CancellationException? = null
        var cleanupFailure: PulsepondStorageException? = null
        try {
            flush()
        } catch (error: CancellationException) {
            cancellation = error
        } catch (error: Throwable) {
            cleanupFailure = PulsepondStorageException(
                "Pulsepond could not complete final delivery during shutdown",
            )
        }

        withContext(NonCancellable) {
            synchronized(stateLock) {
                generationInvalidation.complete(Unit)
                generation += 1
            }
            flushLock.withLock {
                val pendingSnapshot = synchronized(stateLock) {
                    when {
                        !persistence.isDurable || isCurrentQueueDurableLocked() -> {
                            val snapshot = currentQueueSnapshotLocked()
                            PendingQueueSnapshot(snapshot.events, snapshot.revision, null)
                        }
                        else -> tryPersistCurrentQueueLocked()
                    }
                }
                var pendingEventsPersisted = when {
                    !persistence.isDurable -> pendingSnapshot.events.isEmpty()
                    pendingSnapshot.completion == null -> true
                    else -> awaitQueueSnapshot(pendingSnapshot)
                }
                if (
                    persistence.isDurable &&
                    !pendingEventsPersisted
                ) {
                    pendingEventsPersisted = synchronized(stateLock) {
                        isCurrentQueueDurableLocked()
                    }
                }
                val uncoveredEvents = synchronized(stateLock) {
                    pendingSnapshot.events.count { it.eventId !in durableEventIds }
                }
                val droppedEvents = synchronized(stateLock) {
                    val count = queue.size
                    queue.clear()
                    queueBytes = 0
                    closed = true
                    closing = false
                    count
                }
                if (droppedEvents > 0 && uncoveredEvents > 0) {
                    notify(
                        PulsepondDiagnostic(
                            code = PulsepondDiagnosticCode.DeliveryFailed,
                            droppedEvents = uncoveredEvents,
                            retryable = false,
                        ),
                    )
                    if (persistence.isDurable && cleanupFailure == null) {
                        cleanupFailure = PulsepondStorageException(
                            "Pulsepond could not preserve pending events during shutdown",
                        )
                    }
                }
                if (
                    persistence.isDurable &&
                    !pendingEventsPersisted &&
                    uncoveredEvents == 0
                ) {
                    notifyStorageFailure()
                    if (cleanupFailure == null) {
                        cleanupFailure = PulsepondStorageException(
                            "Pulsepond could not finalize durable state during shutdown",
                        )
                    }
                }
                try {
                    persistenceWriter.close()
                } catch (_: Throwable) {
                    if (cleanupFailure == null) {
                        cleanupFailure = PulsepondStorageException(
                            "Pulsepond could not close durable state",
                        )
                    }
                }
                try {
                    transport.close()
                } catch (_: Throwable) {
                    if (cleanupFailure == null) {
                        cleanupFailure = PulsepondStorageException(
                            "Pulsepond could not close its transport",
                        )
                    }
                } finally {
                    if (ownsScope) scope.cancel()
                    ownershipLease?.release()
                    completion.complete(cleanupFailure)
                }
            }
        }
        cancellation?.let { throw it }
        cleanupFailure?.let { throw it }
    }

    internal fun pendingEventCount(): Int = synchronized(stateLock) { queue.size }

    internal suspend fun awaitPersistence() {
        persistenceWriter.drain()
    }

    internal fun startAutomaticDelivery() {
        val action = synchronized(stateLock) {
            if (automaticDeliveryStarted || closing || closed) return
            automaticDeliveryStarted = true
            when {
                queue.size >= effectiveBatchSize -> {
                    immediateFlushScheduled = true
                    1
                }
                queue.isNotEmpty() && configuration.flushIntervalMilliseconds > 0 -> {
                    automaticFlushScheduled = true
                    2
                }
                else -> 0
            }
        }
        if (action == 1) scheduleImmediateFlush()
        if (action == 2) scheduleTimedFlush()
    }

    internal suspend fun disposeAfterCancelledCreation() {
        withContext(NonCancellable) {
            synchronized(stateLock) {
                closing = true
                closed = true
                generationInvalidation.complete(Unit)
            }
            try {
                persistenceWriter.close()
            } catch (_: Throwable) {
                // The caller cannot recover an instance that was cancelled before publication.
            }
            try {
                transport.close()
            } catch (_: Throwable) {
                // The namespace lease and owned scope must still be released.
            } finally {
                if (ownsScope) scope.cancel()
                ownershipLease?.release()
            }
        }
    }

    private fun scheduleImmediateFlush() {
        scope.launch {
            try {
                flush()
            } finally {
                val flushAgain = synchronized(stateLock) {
                    immediateFlushScheduled = false
                    if (
                        queue.size >= effectiveBatchSize &&
                        !deliveryDeferred &&
                        !closing &&
                        !closed
                    ) {
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
                        !deliveryDeferred &&
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

    private suspend fun dropStaleEvents(): QueueMutation {
        val (dropped, replacement) = synchronized(stateLock) {
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
            dropped to if (dropped > 0) {
                queueRevision += 1
                tryPersistCurrentQueueLocked()
            } else {
                null
            }
        }
        val storageFailed = dropped > 0 && (
            replacement == null || !awaitQueueSnapshot(replacement)
        )
        return QueueMutation(dropped, storageFailed)
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
        Batch(selected, body, generation, generationInvalidation)
    }

    private suspend fun deliver(batch: Batch): DeliveryResult {
        var attempts = 0
        while (true) {
            val attempt = postBatch(batch)
            if (attempt == TransportAttempt.Invalidated) return DeliveryResult.Invalidated
            val response = when (attempt) {
                is TransportAttempt.Completed -> attempt.response
                TransportAttempt.Failed -> null
                TransportAttempt.Invalidated -> return DeliveryResult.Invalidated
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
            val shouldRetry = waitForRetry(
                batch,
                retryDelayMilliseconds(attempts, response?.retryAfter),
            )
            if (!shouldRetry) return DeliveryResult.Invalidated
        }
    }

    private suspend fun postBatch(batch: Batch): TransportAttempt = coroutineScope<TransportAttempt> {
        if (batch.invalidation.isCompleted) return@coroutineScope TransportAttempt.Invalidated
        val request = async {
            try {
                TransportAttempt.Completed(transport.post(batch.body))
            } catch (error: CancellationException) {
                throw error
            } catch (_: Throwable) {
                TransportAttempt.Failed
            }
        }
        select {
            request.onAwait { it }
            batch.invalidation.onAwait {
                request.cancel()
                TransportAttempt.Invalidated
            }
        }
    }

    private suspend fun waitForRetry(batch: Batch, delayMilliseconds: Long): Boolean {
        if (batch.invalidation.isCompleted) return false
        return withTimeoutOrNull(delayMilliseconds) {
            batch.invalidation.await()
            false
        } ?: true
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
        val retryAtMilliseconds = runCatching {
            trimmedRetryAfter?.fromHttpToGmtDate()?.timestamp
        }.getOrNull()
        if (retryAtMilliseconds != null) {
            return (retryAtMilliseconds - nowMilliseconds())
                .coerceIn(0, maxRetryDelayMilliseconds)
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

    private suspend fun removeBatch(batch: Batch): Boolean {
        val replacement = synchronized(stateLock) {
            if (batch.generation != generation || queue.size < batch.events.size) return false
            val expectedIds = batch.events.map { it.event.eventId }
            val actualIds = queue.take(expectedIds.size).map { it.event.eventId }
            if (expectedIds != actualIds) return false
            repeat(batch.events.size) {
                queueBytes -= queue.removeAt(0).serializedBytes
            }
            queueRevision += 1
            tryPersistCurrentQueueLocked()
        }
        return !awaitQueueSnapshot(replacement)
    }

    private suspend fun persistCurrentQueue(): Boolean {
        val snapshot = synchronized(stateLock) { tryPersistCurrentQueueLocked() }
        return awaitQueueSnapshot(snapshot)
    }

    private fun currentQueueSnapshotLocked(): QueueSnapshot = QueueSnapshot(
        events = queue.map(QueuedEvent::event),
        revision = queueRevision,
    )

    private fun tryPersistCurrentQueueLocked(): PendingQueueSnapshot {
        val snapshot = currentQueueSnapshotLocked()
        return PendingQueueSnapshot(
            events = snapshot.events,
            revision = snapshot.revision,
            completion = persistenceWriter.tryReplace(snapshot.events, snapshot.revision),
        )
    }

    private suspend fun awaitQueueSnapshot(snapshot: PendingQueueSnapshot): Boolean {
        val completion = snapshot.completion ?: return false
        return persistenceWriter.awaitReplace(completion)
    }

    private fun markEventDurable(eventId: String) {
        synchronized(stateLock) {
            durableEventIds += eventId
        }
    }

    private fun markSnapshotDurable(revision: Long, eventIds: List<String>) {
        synchronized(stateLock) {
            durableQueueRevision = revision
            durableEventIds.clear()
            durableEventIds += eventIds
        }
    }

    private fun isCurrentQueueDurableLocked(): Boolean = durableQueueRevision == queueRevision

    private fun notify(diagnostic: PulsepondDiagnostic) {
        try {
            configuration.diagnosticListener?.onDiagnostic(diagnostic)
        } catch (_: Throwable) {
            // Consumer callbacks cannot break delivery or receive event data.
        }
    }

    private fun notifyStorageFailure() {
        notify(
            PulsepondDiagnostic(
                code = PulsepondDiagnosticCode.StorageFailed,
                droppedEvents = 0,
                retryable = true,
            ),
        )
    }
}

internal fun createPersistentPulsepond(
    configuration: PulsepondConfiguration,
    persistence: EventPersistence,
    ownershipLease: ClientOwnershipLease? = null,
    startAutomaticDelivery: Boolean = true,
): Pulsepond = Pulsepond(
    configuration = configuration,
    transport = KtorEventTransport(configuration),
    nowMilliseconds = { Clock.System.now().toEpochMilliseconds() },
    randomBytes = ::fillSecureRandom,
    persistence = persistence,
    ownershipLease = ownershipLease,
    automaticallyStart = startAutomaticDelivery,
    coroutineScope = null,
)

internal fun createOwnedPersistentPulsepond(
    configuration: PulsepondConfiguration,
    persistence: EventPersistence,
    startAutomaticDelivery: Boolean = true,
): Pulsepond {
    val lease = ClientOwnershipRegistry.acquire(configuration.storageNamespace)
    return try {
        createPersistentPulsepond(configuration, persistence, lease, startAutomaticDelivery)
    } catch (error: Throwable) {
        lease.release()
        throw error
    }
}

internal suspend fun createPulsepondInBackground(
    create: () -> Pulsepond,
    afterCreate: () -> Unit = {},
): Pulsepond {
    val createdClient = atomic<Pulsepond?>(null)
    return try {
        withContext(Dispatchers.Default) {
            create().also { client ->
                createdClient.value = client
                afterCreate()
            }
        }.also(Pulsepond::startAutomaticDelivery)
    } catch (error: CancellationException) {
        createdClient.value?.disposeAfterCancelledCreation()
        throw error
    }
}
