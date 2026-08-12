package dev.pulsepond.sdk

import okio.FileSystem
import okio.ForwardingFileSystem
import okio.Path
import okio.Path.Companion.toPath
import okio.buffer
import okio.fakefilesystem.FakeFileSystem
import okio.use
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PersistenceTest {
    @Test
    fun writeKeyRotationRetainsTheSourceScopedIdentityAndQueue() {
        val fileSystem = FakeFileSystem()
        val initial = PulsepondConfiguration(
            "https://events.example.com/v1/batch",
            testWriteKey,
            testDeploymentId,
            testProjectId,
            testSourceId,
            "production",
        )
        val rotated = PulsepondConfiguration(
            "https://events.example.com/v1/batch",
            replacementWriteKey,
            testDeploymentId,
            testProjectId,
            testSourceId,
            "production",
        )
        val first = FileEventPersistence(
            fileSystem,
            "/pulsepond/${initial.storageNamespace}".toPath(),
        )
        val queued = event(installationIdOne)
        first.load { installationIdOne }
        first.append(queued, listOf(queued))

        val restored = FileEventPersistence(
            fileSystem,
            "/pulsepond/${rotated.storageNamespace}".toPath(),
        ).load { installationIdTwo }

        assertEquals(installationIdOne, restored.installationId)
        assertEquals(listOf(queued), restored.events)
        fileSystem.checkNoOpenFiles()
    }

    @Test
    fun identityAndQueuedEventsRoundTripAndAcceptedEventsCanBeRemoved() {
        val fileSystem = FakeFileSystem()
        val directory = "/pulsepond/source/production".toPath()
        val first = FileEventPersistence(fileSystem, directory)

        assertEquals(installationIdOne, first.load { installationIdOne }.installationId)
        first.append(event(installationIdOne), listOf(event(installationIdOne)))

        val second = FileEventPersistence(fileSystem, directory)
        val restored = second.load { installationIdTwo }
        assertEquals(installationIdOne, restored.installationId)
        assertEquals(listOf("app_open"), restored.events.map(EventRecord::eventName))

        second.replace(emptyList())
        val third = FileEventPersistence(fileSystem, directory).load { installationIdTwo }
        assertEquals(installationIdOne, third.installationId)
        assertTrue(third.events.isEmpty())
        fileSystem.checkNoOpenFiles()
    }

    @Test
    fun resetAtomicallySelectsANewEmptyGeneration() {
        val fileSystem = FakeFileSystem()
        val directory = "/pulsepond/source/production".toPath()
        val persistence = FileEventPersistence(fileSystem, directory)
        persistence.load { installationIdOne }
        persistence.append(event(installationIdOne), listOf(event(installationIdOne)))

        persistence.reset(installationIdTwo)

        val restored = FileEventPersistence(fileSystem, directory).load { installationIdOne }
        assertEquals(installationIdTwo, restored.installationId)
        assertTrue(restored.events.isEmpty())
        assertFalse(fileSystem.exists(directory / "$installationIdOne.events"))
        fileSystem.checkNoOpenFiles()
    }

    @Test
    fun corruptJournalRecordsAreDroppedAndCompacted() {
        val fileSystem = FakeFileSystem()
        val directory = "/pulsepond/source/production".toPath()
        val persistence = FileEventPersistence(fileSystem, directory)
        persistence.load { installationIdOne }
        persistence.append(event(installationIdOne), listOf(event(installationIdOne)))
        fileSystem.appendingSink(directory / "$installationIdOne.events").buffer().use { sink ->
            sink.writeUtf8("{\"event_name\":\"invalid\"}\n")
            sink.writeUtf8(event(installationIdOne).json().toString().replace(
                "\"event_name\":\"app_open\"",
                "\"event_name\":123",
            ))
            sink.writeByte('\n'.code)
            sink.writeUtf8(event(installationIdTwo).json().toString())
            sink.writeByte('\n'.code)
            sink.writeUtf8(event(installationIdOne).json().toString().replace(
                "1970-01-01T00:00:00Z",
                "+10000-01-01T00:00:00Z",
            ))
            sink.writeByte('\n'.code)
        }

        val restored = FileEventPersistence(fileSystem, directory).load { installationIdTwo }

        assertEquals(4, restored.recoveredRecords)
        assertEquals(1, restored.events.size)
        val compacted = FileEventPersistence(fileSystem, directory).load { installationIdTwo }
        assertEquals(0, compacted.recoveredRecords)
        assertEquals(1, compacted.events.size)
        fileSystem.checkNoOpenFiles()
    }

    @Test
    fun corruptManifestRotatesIdentityWithoutReplayingTheOrphanedJournal() {
        val fileSystem = FakeFileSystem()
        val directory = "/pulsepond/source/production".toPath()
        val persistence = FileEventPersistence(fileSystem, directory)
        persistence.load { installationIdOne }
        persistence.append(event(installationIdOne), listOf(event(installationIdOne)))
        fileSystem.write(directory / "active.json") { writeUtf8("not-json") }

        val restored = FileEventPersistence(fileSystem, directory).load { installationIdTwo }

        assertEquals(installationIdTwo, restored.installationId)
        assertEquals(1, restored.recoveredRecords)
        assertTrue(restored.events.isEmpty())
        assertFalse(fileSystem.exists(directory / "$installationIdOne.events"))
        fileSystem.checkNoOpenFiles()
    }

    @Test
    fun interruptedResetNeverReplaysThePreviousGeneration() {
        val delegate = FakeFileSystem()
        val directory = "/pulsepond/source/production".toPath()
        val oldJournal = directory / "$installationIdOne.events"
        val fileSystem = FaultingFileSystem(delegate).apply {
            failDelete = { path -> path == oldJournal }
        }
        val persistence = FileEventPersistence(fileSystem, directory)
        persistence.load { installationIdOne }
        persistence.append(event(installationIdOne), listOf(event(installationIdOne)))

        val result = persistence.reset(installationIdTwo)

        assertFalse(result.completed)
        fileSystem.failDelete = null
        val restored = FileEventPersistence(fileSystem, directory).load { installationIdTwo }
        assertEquals(installationIdTwo, restored.installationId)
        assertTrue(restored.events.isEmpty())
        assertFalse(delegate.exists(oldJournal))
        delegate.checkNoOpenFiles()
    }

    @Test
    fun resetFailsWhenPreviousJournalsCannotBeEnumerated() {
        val delegate = FakeFileSystem()
        val directory = "/pulsepond/source/production".toPath()
        val fileSystem = FaultingFileSystem(delegate)
        val persistence = FileEventPersistence(fileSystem, directory)
        persistence.load { installationIdOne }
        persistence.append(event(installationIdOne), listOf(event(installationIdOne)))
        fileSystem.failList = { path -> path == directory }

        val result = persistence.reset(installationIdTwo)

        assertFalse(result.completed)
        fileSystem.failList = null
        val restored = FileEventPersistence(fileSystem, directory).load { installationIdTwo }
        assertEquals(installationIdTwo, restored.installationId)
        assertTrue(restored.events.isEmpty())
        delegate.checkNoOpenFiles()
    }

    @Test
    fun recoveryCompletesResetWhenFinalManifestWriteFailed() {
        val delegate = FakeFileSystem()
        val directory = "/pulsepond/source/production".toPath()
        val manifest = directory / "active.json"
        val fileSystem = FaultingFileSystem(delegate)
        val persistence = FileEventPersistence(fileSystem, directory)
        persistence.load { installationIdOne }
        persistence.append(event(installationIdOne), listOf(event(installationIdOne)))
        fileSystem.failAtomicMoveTarget = manifest
        fileSystem.successfulTargetMovesBeforeFailure = 1

        val result = persistence.reset(installationIdTwo)

        assertFalse(result.completed)
        fileSystem.failAtomicMoveTarget = null
        val restored = FileEventPersistence(fileSystem, directory).load { installationIdOne }
        assertEquals(installationIdTwo, restored.installationId)
        assertTrue(restored.events.isEmpty())
        assertFalse(delegate.exists(directory / "$installationIdOne.events"))
        delegate.checkNoOpenFiles()
    }

    @Test
    fun resetDeletesCrashLeftJournalTemporaryFiles() {
        val fileSystem = FakeFileSystem()
        val directory = "/pulsepond/source/production".toPath()
        val persistence = FileEventPersistence(fileSystem, directory)
        persistence.load { installationIdOne }
        val oldTemporary = directory / ".$installationIdOne.events.tmp"
        fileSystem.write(oldTemporary) { writeUtf8("${event(installationIdOne).json()}\n") }

        val result = persistence.reset(installationIdTwo)

        assertTrue(result.completed)
        assertFalse(fileSystem.exists(oldTemporary))
        fileSystem.checkNoOpenFiles()
    }

    @Test
    fun appendCompactsBeforeTheJournalCanExceedItsHardLimit() {
        val delegate = FakeFileSystem()
        val directory = "/pulsepond/source/production".toPath()
        val fileSystem = FaultingFileSystem(delegate)
        val persistence = FileEventPersistence(fileSystem, directory)
        persistence.load { installationIdOne }
        val journal = directory / "$installationIdOne.events"
        delegate.write(journal) {
            writeUtf8("x".repeat((maximumJournalBytes - 16).toInt()))
        }
        val currentEvents = listOf(event(installationIdOne))
        fileSystem.failedAtomicMoves = 2

        repeat(2) {
            assertFailsWith<PulsepondStorageException> {
                persistence.append(currentEvents.single(), currentEvents)
            }
        }
        val result = persistence.append(currentEvents.single(), currentEvents)

        assertEquals(AppendPersistenceResult.ReplacedSnapshot, result)
        assertTrue(delegate.metadata(journal).size!! <= maximumJournalBytes)
        val restored = FileEventPersistence(fileSystem, directory).load { installationIdTwo }
        assertEquals(currentEvents, restored.events)
        delegate.checkNoOpenFiles()
    }

    @Test
    fun oversizedLegacyJournalIsDiscardedWithoutReadingItsContents() {
        val delegate = FakeFileSystem()
        val directory = "/pulsepond/source/production".toPath()
        val persistence = FileEventPersistence(delegate, directory)
        persistence.load { installationIdOne }
        val line = "${event(installationIdOne).json()}\n"
        val copies = (maximumJournalBytes * 3 / line.encodeToByteArray().size + 2).toInt()
        val journal = directory / "$installationIdOne.events"
        delegate.write(journal) {
            repeat(copies) { writeUtf8(line) }
        }
        val fileSystem = FaultingFileSystem(delegate).apply {
            failSource = { path -> path == journal }
        }

        val restored = FileEventPersistence(fileSystem, directory).load { installationIdTwo }

        assertTrue(restored.events.isEmpty())
        assertEquals(1, restored.recoveredRecords)
        assertEquals(0, delegate.metadata(journal).size)
        delegate.checkNoOpenFiles()
    }

    @Test
    fun manifestReadFailuresDoNotRotateIdentityOrDeleteQueuedEvents() {
        val delegate = FakeFileSystem()
        val directory = "/pulsepond/source/production".toPath()
        val persistence = FileEventPersistence(delegate, directory)
        persistence.load { installationIdOne }
        persistence.append(event(installationIdOne), listOf(event(installationIdOne)))
        val fileSystem = FaultingFileSystem(delegate).apply {
            failSource = { path -> path == directory / "active.json" }
        }

        assertFailsWith<PulsepondStorageException> {
            FileEventPersistence(fileSystem, directory).load { installationIdTwo }
        }

        fileSystem.failSource = null
        val restored = FileEventPersistence(fileSystem, directory).load { installationIdTwo }
        assertEquals(installationIdOne, restored.installationId)
        assertEquals(1, restored.events.size)
        delegate.checkNoOpenFiles()
    }
}

private class FaultingFileSystem(
    delegate: FileSystem,
) : ForwardingFileSystem(delegate) {
    var failDelete: ((Path) -> Boolean)? = null
    var failList: ((Path) -> Boolean)? = null
    var failSource: ((Path) -> Boolean)? = null
    var failedAtomicMoves: Int = 0
    var failAtomicMoveTarget: Path? = null
    var successfulTargetMovesBeforeFailure: Int = 0

    override fun delete(path: Path, mustExist: Boolean) {
        if (failDelete?.invoke(path) == true) error("test delete failure")
        super.delete(path, mustExist)
    }

    override fun list(dir: Path): List<Path> {
        if (failList?.invoke(dir) == true) error("test list failure")
        return super.list(dir)
    }

    override fun source(file: Path): okio.Source {
        if (failSource?.invoke(file) == true) error("test source failure")
        return super.source(file)
    }

    override fun atomicMove(source: Path, target: Path) {
        if (target == failAtomicMoveTarget) {
            if (successfulTargetMovesBeforeFailure == 0) error("test target move failure")
            successfulTargetMovesBeforeFailure -= 1
        }
        if (failedAtomicMoves > 0) {
            failedAtomicMoves -= 1
            error("test atomic move failure")
        }
        super.atomicMove(source, target)
    }
}

private const val installationIdOne: String = "00000000-0000-7000-8000-000000000001"
private const val installationIdTwo: String = "00000000-0000-7000-8000-000000000002"

private fun event(installationId: String): EventRecord = EventRecord(
    eventId = "00000000-0000-7000-8000-000000000003",
    eventName = "app_open",
    occurredAtMilliseconds = 0,
    occurredAt = "1970-01-01T00:00:00Z",
    platform = "android",
    appVersion = "1.0.0",
    release = "android@1.0.0",
    environment = "production",
    anonymousInstallationId = installationId,
    sessionId = "00000000-0000-7000-8000-000000000004",
    properties = mapOf("cold_start" to EventPropertyValue.Flag(true)),
)
