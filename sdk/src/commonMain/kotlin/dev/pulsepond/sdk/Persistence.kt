package dev.pulsepond.sdk

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
private const val maximumJournalBytes: Long = 1_100_000

internal data class PersistedState(
    val installationId: String,
    val events: List<EventRecord>,
    val recoveredRecords: Int = 0,
)

internal interface EventPersistence {
    val isDurable: Boolean

    fun load(newInstallationId: () -> String): PersistedState

    fun append(event: EventRecord)

    fun replace(events: List<EventRecord>)

    fun reset(newInstallationId: String)
}

internal object VolatileEventPersistence : EventPersistence {
    override val isDurable: Boolean = false

    override fun load(newInstallationId: () -> String): PersistedState =
        PersistedState(newInstallationId(), emptyList())

    override fun append(event: EventRecord) = Unit

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
        val storedId = readManifest()
        val activeId = if (storedId != null) {
            storedId
        } else {
            if (fileSystem.exists(manifest)) recovered += 1
            newInstallationId().also {
                writeManifest(it)
                deleteOrphanedJournals(it)
            }
        }
        installationId = activeId
        val journal = journal(activeId)
        if (!fileSystem.exists(journal)) return@storageOperation PersistedState(activeId, emptyList(), recovered)
        if ((fileSystem.metadata(journal).size ?: 0) > maximumJournalBytes) {
            atomicWrite(journal, "")
            return@storageOperation PersistedState(activeId, emptyList(), recovered + 1)
        }
        val events = mutableListOf<EventRecord>()
        fileSystem.source(journal).buffer().use { source ->
            while (!source.exhausted()) {
                val line = source.readUtf8Line() ?: break
                if (line.isBlank()) continue
                val event = parsePersistedEvent(line)
                if (event == null) recovered += 1 else events += event
            }
        }
        if (recovered > 0) replace(events)
        PersistedState(activeId, events, recovered)
    }

    override fun append(event: EventRecord): Unit = storageOperation("append") {
        val journal = journal(requireInstallationId())
        fileSystem.appendingSink(journal, mustExist = false).buffer().use { sink ->
            sink.writeUtf8(event.json().toString())
            sink.writeByte('\n'.code)
        }
    }

    override fun replace(events: List<EventRecord>): Unit = storageOperation("replace") {
        val body = buildString {
            events.forEach { event ->
                append(event.json())
                append('\n')
            }
        }
        atomicWrite(journal(requireInstallationId()), body)
    }

    override fun reset(newInstallationId: String): Unit = storageOperation("reset") {
        requireInstallationId()
        atomicWrite(journal(newInstallationId), "")
        writeManifest(newInstallationId)
        installationId = newInstallationId
        deleteOrphanedJournals(newInstallationId)
    }

    private fun readManifest(): String? = runCatching {
        if (!fileSystem.exists(manifest)) return null
        val manifestSize = fileSystem.metadata(manifest).size ?: return null
        if (manifestSize !in 1..512) return null
        val json = Json.parseToJsonElement(fileSystem.read(manifest) { readUtf8() }).jsonObject
        if (json["format"]?.jsonPrimitive?.intOrNull != persistenceFormat) return null
        json["installation_id"]?.jsonPrimitive?.content?.takeIf(::isUuidV7)
    }.getOrNull()

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
        runCatching {
            fileSystem.list(directory)
                .filter { path -> path.name.endsWith(".events") && path != activeJournal }
                .forEach { path -> fileSystem.delete(path, mustExist = false) }
        }
    }

    private fun requireInstallationId(): String =
        installationId ?: throw PulsepondStorageException("Pulsepond storage is not initialized")
}

private inline fun <T> storageOperation(action: String, block: () -> T): T = try {
    block()
} catch (error: PulsepondStorageException) {
    throw error
} catch (_: Throwable) {
    throw PulsepondStorageException("Pulsepond could not $action durable state")
}
