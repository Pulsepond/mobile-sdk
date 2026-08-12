package dev.pulsepond.sdk

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import okio.FileSystem
import okio.Path
import okio.buffer
import okio.use

private const val persistenceFormat: Int = 1
internal const val maximumJournalBytes: Long = 1_100_000
private const val maximumPersistedLineBytes: Long = 64_000

internal data class PersistedState(
    val installationId: String,
    val events: List<EventRecord>,
    val recoveredRecords: Int = 0,
)

internal interface EventPersistence {
    val isDurable: Boolean

    fun load(newInstallationId: () -> String): PersistedState

    fun append(event: EventRecord, currentEvents: List<EventRecord>): AppendPersistenceResult

    fun replace(events: List<EventRecord>)

    fun reset(newInstallationId: String): PersistenceResetResult
}

internal data class PersistenceResetResult(val completed: Boolean)

internal enum class AppendPersistenceResult {
    AppendedEvent,
    ReplacedSnapshot,
}

private sealed interface PersistenceCommand {
    data class Append(
        val event: EventRecord,
        val revision: Long,
    ) : PersistenceCommand

    data class Replace(
        val events: List<EventRecord>,
        val revision: Long,
        val completion: CompletableDeferred<Boolean>,
    ) : PersistenceCommand

    data class Reset(
        val installationId: String,
        val revision: Long,
        val completion: CompletableDeferred<Result<PersistenceResetResult>>,
    ) : PersistenceCommand

    data class Barrier(val completion: CompletableDeferred<Unit>) : PersistenceCommand

    data class Close(val completion: CompletableDeferred<Unit>) : PersistenceCommand
}

internal class PersistenceWriter(
    private val persistence: EventPersistence,
    initialEvents: List<EventRecord>,
    scope: CoroutineScope,
    capacity: Int,
    private val onFailure: () -> Unit,
    private val onAppendPersisted: (Long, String) -> Unit,
    private val onSnapshotPersisted: (Long, List<String>, Boolean) -> Unit,
) {
    private val commands: Channel<PersistenceCommand> = Channel(capacity)
    private val job = scope.launch {
        val currentEvents = initialEvents.toMutableList()
        var currentRevision = 0L
        var currentEventsAreExact = true
        for (command in commands) {
            when (command) {
                is PersistenceCommand.Append -> {
                    currentEvents += command.event
                    currentEventsAreExact = currentEventsAreExact &&
                        command.revision == currentRevision + 1
                    currentRevision = command.revision
                    try {
                        when (persistence.append(command.event, currentEvents)) {
                            AppendPersistenceResult.AppendedEvent -> {
                                onAppendPersisted(command.revision, command.event.eventId)
                            }
                            AppendPersistenceResult.ReplacedSnapshot -> {
                                onSnapshotPersisted(
                                    command.revision,
                                    currentEvents.map(EventRecord::eventId),
                                    currentEventsAreExact,
                                )
                            }
                        }
                    } catch (_: PulsepondStorageException) {
                        onFailure()
                    }
                }
                is PersistenceCommand.Replace -> {
                    currentEvents.clear()
                    currentEvents += command.events
                    currentRevision = command.revision
                    currentEventsAreExact = true
                    command.completion.complete(
                        try {
                            persistence.replace(command.events)
                            onSnapshotPersisted(
                                command.revision,
                                command.events.map(EventRecord::eventId),
                                true,
                            )
                            true
                        } catch (_: PulsepondStorageException) {
                            false
                        },
                    )
                }
                is PersistenceCommand.Reset -> {
                    val result = try {
                        val reset = persistence.reset(command.installationId)
                        currentEvents.clear()
                        currentRevision = command.revision
                        currentEventsAreExact = true
                        Result.success(reset)
                    } catch (failure: PulsepondStorageException) {
                        Result.failure(failure)
                    }
                    command.completion.complete(result)
                }
                is PersistenceCommand.Barrier -> command.completion.complete(Unit)
                is PersistenceCommand.Close -> {
                    command.completion.complete(Unit)
                    return@launch
                }
            }
        }
    }

    fun tryAppend(event: EventRecord, revision: Long): Boolean {
        return commands.trySend(PersistenceCommand.Append(event, revision)).isSuccess
    }

    fun tryReplace(
        events: List<EventRecord>,
        revision: Long,
    ): CompletableDeferred<Boolean>? {
        val completion = CompletableDeferred<Boolean>()
        return if (
            commands.trySend(
                PersistenceCommand.Replace(events.toList(), revision, completion),
            ).isSuccess
        ) {
            completion
        } else {
            null
        }
    }

    suspend fun awaitReplace(completion: CompletableDeferred<Boolean>): Boolean {
        return try {
            completion.await()
        } catch (error: CancellationException) {
            throw error
        } catch (_: Throwable) {
            false
        }
    }

    suspend fun reset(
        installationId: String,
        revision: Long,
    ): PersistenceResetResult {
        val completion = CompletableDeferred<Result<PersistenceResetResult>>()
        try {
            commands.send(PersistenceCommand.Reset(installationId, revision, completion))
        } catch (error: CancellationException) {
            throw error
        } catch (_: Throwable) {
            throw PulsepondStorageException("Pulsepond could not queue durable reset")
        }
        return completion.await().getOrThrow()
    }

    suspend fun drain() {
        val completion = CompletableDeferred<Unit>()
        try {
            commands.send(PersistenceCommand.Barrier(completion))
            completion.await()
        } catch (error: CancellationException) {
            throw error
        } catch (_: Throwable) {
            throw PulsepondStorageException("Pulsepond could not drain durable state")
        }
    }

    suspend fun close() {
        val completion = CompletableDeferred<Unit>()
        try {
            commands.send(PersistenceCommand.Close(completion))
            completion.await()
            commands.close()
            job.join()
        } catch (error: CancellationException) {
            throw error
        } catch (_: Throwable) {
            throw PulsepondStorageException("Pulsepond could not close durable state")
        }
    }
}

internal object VolatileEventPersistence : EventPersistence {
    override val isDurable: Boolean = false

    override fun load(newInstallationId: () -> String): PersistedState =
        PersistedState(newInstallationId(), emptyList())

    override fun append(
        event: EventRecord,
        currentEvents: List<EventRecord>,
    ): AppendPersistenceResult = AppendPersistenceResult.AppendedEvent

    override fun replace(events: List<EventRecord>) = Unit

    override fun reset(newInstallationId: String): PersistenceResetResult =
        PersistenceResetResult(completed = true)
}

internal class FileEventPersistence(
    private val fileSystem: FileSystem,
    private val directory: Path,
) : EventPersistence {
    override val isDurable: Boolean = true

    private val manifest: Path = directory / "active.json"
    private var installationId: String? = null

    override fun load(newInstallationId: () -> String): PersistedState = storageOperation("load") {
        fileSystem.createDirectories(directory)
        var recovered = 0
        val activeId = when (val stored = readManifest()) {
            is ManifestState.Active -> stored.installationId
            is ManifestState.Resetting -> {
                installationId = stored.installationId
                deleteInactiveEventFiles(stored.installationId)
                writeManifest(stored.installationId, resetting = false)
                recovered += 1
                stored.installationId
            }
            ManifestState.Missing -> newInstallationId().also {
                writeManifest(it, resetting = false)
            }
            ManifestState.Invalid -> {
                recovered += 1
                newInstallationId().also { writeManifest(it, resetting = false) }
            }
        }
        installationId = activeId
        deleteInactiveEventFiles(activeId)
        val journal = journal(activeId)
        if (!fileSystem.exists(journal)) return@storageOperation PersistedState(activeId, emptyList(), recovered)
        if ((fileSystem.metadata(journal).size ?: 0) > maximumJournalBytes) {
            atomicWrite(journal, "")
            return@storageOperation PersistedState(activeId, emptyList(), recovered + 1)
        }
        val events = mutableListOf<EventRecord>()
        var eventBytes = 0
        fileSystem.source(journal).buffer().use { source ->
            while (!source.exhausted()) {
                val line = source.readBoundedUtf8Line()
                if (line == null) {
                    recovered += 1
                    continue
                }
                if (line.isBlank()) continue
                val event = parsePersistedEvent(line)
                if (event == null || event.anonymousInstallationId != activeId) {
                    recovered += 1
                } else {
                    val encodedBytes = persistedLine(event).encodeToByteArray().size
                    if (encodedBytes > maxQueueBytes) {
                        recovered += 1
                        continue
                    }
                    if (eventBytes + encodedBytes > maxQueueBytes) {
                        recovered += 1
                        continue
                    }
                    events += event
                    eventBytes += encodedBytes
                }
            }
        }
        if (recovered > 0) replace(events)
        PersistedState(activeId, events, recovered)
    }

    override fun append(
        event: EventRecord,
        currentEvents: List<EventRecord>,
    ): AppendPersistenceResult = storageOperation("append") {
        val journal = journal(requireInstallationId())
        val line = persistedLine(event)
        val currentSize = fileSystem.metadataOrNull(journal)?.size ?: 0
        if (currentSize + line.encodeToByteArray().size > maximumJournalBytes) {
            replace(currentEvents)
            AppendPersistenceResult.ReplacedSnapshot
        } else {
            fileSystem.appendingSink(journal, mustExist = false).buffer().use { sink ->
                sink.writeUtf8(line)
            }
            AppendPersistenceResult.AppendedEvent
        }
    }

    override fun replace(events: List<EventRecord>): Unit = storageOperation("replace") {
        val body = events.joinToString(separator = "", transform = ::persistedLine)
        if (body.encodeToByteArray().size > maximumJournalBytes) {
            throw PulsepondStorageException("Pulsepond durable queue exceeds its storage limit")
        }
        atomicWrite(journal(requireInstallationId()), body)
    }

    override fun reset(
        newInstallationId: String,
    ): PersistenceResetResult = storageOperation("reset") {
        requireInstallationId()
        atomicWrite(journal(newInstallationId), "")
        writeManifest(newInstallationId, resetting = true)
        installationId = newInstallationId
        try {
            storageOperation("complete reset") {
                deleteInactiveEventFiles(newInstallationId)
                writeManifest(newInstallationId, resetting = false)
            }
            PersistenceResetResult(completed = true)
        } catch (_: PulsepondStorageException) {
            PersistenceResetResult(completed = false)
        }
    }

    private fun readManifest(): ManifestState {
        if (!fileSystem.exists(manifest)) return ManifestState.Missing
        val manifestSize = fileSystem.metadata(manifest).size ?: return ManifestState.Invalid
        if (manifestSize !in 1..512) return ManifestState.Invalid
        val body = fileSystem.read(manifest) { readUtf8() }
        return runCatching {
            val json = Json.parseToJsonElement(body).jsonObject
            val installationId = json["installation_id"]?.jsonPrimitive?.content
            if (
                json["format"]?.jsonPrimitive?.intOrNull == persistenceFormat &&
                installationId != null &&
                isUuidV7(installationId)
            ) {
                when (json["state"]?.jsonPrimitive?.content) {
                    "active" -> ManifestState.Active(installationId)
                    "resetting" -> ManifestState.Resetting(installationId)
                    else -> ManifestState.Invalid
                }
            } else {
                ManifestState.Invalid
            }
        }.getOrElse { ManifestState.Invalid }
    }

    private fun writeManifest(value: String, resetting: Boolean) {
        val body = buildJsonObject {
            put("format", persistenceFormat)
            put("state", if (resetting) "resetting" else "active")
            put("installation_id", value)
        }.toString()
        atomicWrite(manifest, body)
    }

    private fun atomicWrite(target: Path, value: String) {
        val temporary = directory / ".${target.name}.tmp"
        fileSystem.delete(temporary, mustExist = false)
        var moved = false
        try {
            fileSystem.write(temporary) { writeUtf8(value) }
            fileSystem.atomicMove(temporary, target)
            moved = true
        } finally {
            if (moved) {
                runCatching { fileSystem.delete(temporary, mustExist = false) }
            } else {
                fileSystem.delete(temporary, mustExist = false)
            }
        }
    }

    private fun journal(value: String): Path = directory / "$value.events"

    private fun deleteInactiveEventFiles(activeInstallationId: String) {
        val activeJournal = journal(activeInstallationId)
        fileSystem.list(directory)
            .filter { path ->
                (isOwnedJournal(path) && path != activeJournal) || isOwnedJournalTemporary(path)
            }
            .forEach { path -> fileSystem.delete(path, mustExist = false) }
    }

    private fun requireInstallationId(): String =
        installationId ?: throw PulsepondStorageException("Pulsepond storage is not initialized")
}

private sealed interface ManifestState {
    data object Missing : ManifestState

    data object Invalid : ManifestState

    data class Active(val installationId: String) : ManifestState

    data class Resetting(val installationId: String) : ManifestState
}

private fun persistedLine(event: EventRecord): String = "${event.json()}\n"

private fun isOwnedJournal(path: Path): Boolean =
    path.name.endsWith(".events") && isUuidV7(path.name.removeSuffix(".events"))

private fun isOwnedJournalTemporary(path: Path): Boolean =
    path.name.startsWith('.') &&
        path.name.endsWith(".events.tmp") &&
        isUuidV7(path.name.removePrefix(".").removeSuffix(".events.tmp"))

private fun okio.BufferedSource.readBoundedUtf8Line(): String? {
    if (!request(maximumPersistedLineBytes + 1)) return readUtf8Line()
    val newline = indexOf('\n'.code.toByte(), 0, maximumPersistedLineBytes + 1)
    if (newline >= 0) {
        val line = readUtf8(newline)
        skip(1)
        return line
    }
    skip(maximumPersistedLineBytes + 1)
    while (true) {
        val nextNewline = indexOf('\n'.code.toByte(), 0, maximumPersistedLineBytes + 1)
        if (nextNewline >= 0) {
            skip(nextNewline + 1)
            return null
        }
        if (!request(maximumPersistedLineBytes + 1)) {
            skip(buffer.size)
            return null
        }
        skip(maximumPersistedLineBytes + 1)
    }
}

private inline fun <T> storageOperation(action: String, block: () -> T): T = try {
    block()
} catch (error: PulsepondStorageException) {
    throw error
} catch (_: Throwable) {
    throw PulsepondStorageException("Pulsepond could not $action durable state")
}
