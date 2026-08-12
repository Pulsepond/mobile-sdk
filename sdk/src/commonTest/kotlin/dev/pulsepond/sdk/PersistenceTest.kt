package dev.pulsepond.sdk

import okio.Path.Companion.toPath
import okio.buffer
import okio.fakefilesystem.FakeFileSystem
import okio.use
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PersistenceTest {
    @Test
    fun identityAndQueuedEventsRoundTripAndAcceptedEventsCanBeRemoved() {
        val fileSystem = FakeFileSystem()
        val directory = "/pulsepond/source/production".toPath()
        val first = FileEventPersistence(fileSystem, directory)

        assertEquals(installationIdOne, first.load { installationIdOne }.installationId)
        first.append(event(installationIdOne))

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
        persistence.append(event(installationIdOne))

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
        persistence.append(event(installationIdOne))
        fileSystem.appendingSink(directory / "$installationIdOne.events").buffer().use { sink ->
            sink.writeUtf8("{\"event_name\":\"invalid\"}\n")
        }

        val restored = FileEventPersistence(fileSystem, directory).load { installationIdTwo }

        assertEquals(1, restored.recoveredRecords)
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
        persistence.append(event(installationIdOne))
        fileSystem.write(directory / "active.json") { writeUtf8("not-json") }

        val restored = FileEventPersistence(fileSystem, directory).load { installationIdTwo }

        assertEquals(installationIdTwo, restored.installationId)
        assertEquals(1, restored.recoveredRecords)
        assertTrue(restored.events.isEmpty())
        assertFalse(fileSystem.exists(directory / "$installationIdOne.events"))
        fileSystem.checkNoOpenFiles()
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
