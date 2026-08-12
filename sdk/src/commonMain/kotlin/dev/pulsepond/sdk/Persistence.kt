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

internal data class PersistedState(
    val installationId: String,
    val events: List<EventRecord>,
    val recoveredRecords: Int = 0,
)

internal interface EventPersistence {
    val isDurable: Boolean

    fun load(newInstallationId: () -> String): PersistedState

    fun append(event: EventRecord, currentEvents: List<EventRecord>)

    fun replace(events: List<EventRecord>)

    fun reset(newInstallationId: String)
}

private sealed interface PersistenceCommand {
    data class Append(val event: EventRecord) : PersistenceCommand

    data class Replace(
        val events: List<EventRecord>,
        val completion: CompletableDeferred<Boolean>,
    ) : PersistenceCommand

    data class Reset(
        val installationId: String,
        val completion: CompletableDeferred<Throwable?>,
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
) {
    private val commands: Channel<PersistenceCommand> = Channel(capacity)
    private val job = scope.launch {
        val currentEvents = initialEvents.toMutableList()
        for (command in commands) {
            when (command) {
                is PersistenceCommand.Append -> {
                    currentEvents += command.event
                    try {
                        persistence.append(command.event, currentEvents)
                    } catch (_: PulsepondStorageException) {
                        onFailure()
                    }
                }
                is PersistenceCommand.Replace -> {
                    currentEvents.clear()
                    currentEvents += command.events
                    command.completion.complete(
                        try {
                            persistence.replace(command.events)
                            true
                        } catch (_: PulsepondStorageException) {
                            false
                        },
                    )
                }
                is PersistenceCommand.Reset -> {
                    val error = try {
                        persistence.reset(command.installationId)
                        currentEvents.clear()
                        null
                    } catch (failure: PulsepondStorageException) {
                        failure
                    }
                    command.completion.complete(error)
                }
                is PersistenceCommand.Barrier -> command.completion.complete(Unit)
                is PersistenceCommand.Close -> {
                    command.completion.complete(Unit)
                    return@launch
                }
            }
        }
    }

    fun tryAppend(event: EventRecord): Boolean {
        return commands.trySend(PersistenceCommand.Append(event)).isSuccess
    }

    fun tryReplace(events: List<EventRecord>): CompletableDeferred<Boolean>? {
        val completion = CompletableDeferred<Boolean>()
        return if (
            commands.trySend(PersistenceCommand.Replace(events.toList(), completion)).isSuccess
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

    suspend fun reset(installationId: String) {
        val completion = CompletableDeferred<Throwable?>()
        try {
            commands.send(PersistenceCommand.Reset(installationId, completion))
        } catch (error: CancellationException) {
            throw error
        } catch (_: Throwable) {
            throw PulsepondStorageException("Pulsepond could not queue durable reset")
        }
        completion.await()?.let { throw it }
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

    override fun append(event: EventRecord, currentEvents: List<EventRecord>) = Unit

    override fun replace(events: List<EventRecord>) = Unit

    override fun reset(newInstallationId: String) = Unit
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
            is ManifestState.Valid -> stored.installationId
            ManifestState.Missing -> newInstallationId().also(::writeManifest)
            ManifestState.Invalid -> {
                recovered += 1
                newInstallationId().also(::writeManifest)
            }
        }
        installationId = activeId
        deleteOrphanedJournals(activeId)
        val journal = journal(activeId)
        if (!fileSystem.exists(journal)) return@storageOperation PersistedState(activeId, emptyList(), recovered)
        val journalExceededLimit = (fileSystem.metadata(journal).size ?: 0) > maximumJournalBytes
        val events = mutableListOf<EventRecord>()
        var eventBytes = 0
        fileSystem.source(journal).buffer().use { source ->
            while (!source.exhausted()) {
                val line = source.readUtf8Line() ?: break
                if (line.isBlank()) continue
                val event = parsePersistedEvent(line)
                if (event == null || event.anonymousInstallationId != activeId) {
                    recovered += 1
                } else {
                    val encodedBytes = persistedLine(event).encodeToByteArray().size
                    while (events.isNotEmpty() && eventBytes + encodedBytes > maxQueueBytes) {
                        eventBytes -= persistedLine(events.removeAt(0)).encodeToByteArray().size
                        recovered += 1
                    }
                    if (encodedBytes > maxQueueBytes) {
                        recovered += 1
                        continue
                    }
                    events += event
                    eventBytes += encodedBytes
                }
            }
        }
        if (recovered > 0 || journalExceededLimit) replace(events)
        PersistedState(activeId, events, recovered)
    }

    override fun append(
        event: EventRecord,
        currentEvents: List<EventRecord>,
    ): Unit = storageOperation("append") {
        val journal = journal(requireInstallationId())
        val line = persistedLine(event)
        val currentSize = fileSystem.metadataOrNull(journal)?.size ?: 0
        if (currentSize + line.encodeToByteArray().size > maximumJournalBytes) {
            replace(currentEvents)
        } else {
            fileSystem.appendingSink(journal, mustExist = false).buffer().use { sink ->
                sink.writeUtf8(line)
            }
        }
    }

    override fun replace(events: List<EventRecord>): Unit = storageOperation("replace") {
        val body = events.joinToString(separator = "", transform = ::persistedLine)
        if (body.encodeToByteArray().size > maximumJournalBytes) {
            throw PulsepondStorageException("Pulsepond durable queue exceeds its storage limit")
        }
        atomicWrite(journal(requireInstallationId()), body)
    }

    override fun reset(newInstallationId: String): Unit = storageOperation("reset") {
        requireInstallationId()
        atomicWrite(journal(newInstallationId), "")
        try {
            deleteOrphanedJournals(newInstallationId)
            writeManifest(newInstallationId)
            installationId = newInstallationId
        } catch (error: Throwable) {
            runCatching { fileSystem.delete(journal(newInstallationId), mustExist = false) }
            throw error
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
                ManifestState.Valid(installationId)
            } else {
                ManifestState.Invalid
            }
        }.getOrElse { ManifestState.Invalid }
    }

    private fun writeManifest(value: String) {
        val body = buildJsonObject {
            put("format", persistenceFormat)
            put("installation_id", value)
        }.toString()
        atomicWrite(manifest, body)
    }

    private fun atomicWrite(target: Path, value: String) {
        val temporary = directory / ".${target.name}.tmp"
        fileSystem.delete(temporary, mustExist = false)
        try {
            fileSystem.write(temporary) { writeUtf8(value) }
            fileSystem.atomicMove(temporary, target)
        } finally {
            fileSystem.delete(temporary, mustExist = false)
        }
    }

    private fun journal(value: String): Path = directory / "$value.events"

    private fun deleteOrphanedJournals(activeInstallationId: String) {
        val activeJournal = journal(activeInstallationId)
        fileSystem.list(directory)
            .filter { path -> path.name.endsWith(".events") && path != activeJournal }
            .forEach { path -> fileSystem.delete(path, mustExist = false) }
    }

    private fun requireInstallationId(): String =
        installationId ?: throw PulsepondStorageException("Pulsepond storage is not initialized")
}

private sealed interface ManifestState {
    data object Missing : ManifestState

    data object Invalid : ManifestState

    data class Valid(val installationId: String) : ManifestState
}

private fun persistedLine(event: EventRecord): String = "${event.json()}\n"

private inline fun <T> storageOperation(action: String, block: () -> T): T = try {
    block()
} catch (error: PulsepondStorageException) {
    throw error
} catch (_: Throwable) {
    throw PulsepondStorageException("Pulsepond could not $action durable state")
}
