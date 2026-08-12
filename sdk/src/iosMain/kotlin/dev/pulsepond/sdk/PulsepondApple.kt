@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package dev.pulsepond.sdk

import okio.FileSystem
import okio.Path.Companion.toPath
import platform.Foundation.NSBundle
import platform.Foundation.NSHomeDirectory
import platform.Foundation.NSNumber
import platform.Foundation.NSURL
import platform.Foundation.NSURLIsExcludedFromBackupKey

/** Apple entry point with app-private durable identity and offline event storage. */
public object PulsepondApple {
    /** Creates a client scoped to the source and environment in [configuration]. */
    @Throws(PulsepondConfigurationException::class, PulsepondStorageException::class)
    public fun create(configuration: PulsepondConfiguration): Pulsepond {
        val bundleId = NSBundle.mainBundle.bundleIdentifier ?: "unknown-application"
        val directory = "${NSHomeDirectory()}/Library/Application Support/$bundleId/Pulsepond"
            .toPath() /
            configuration.storageNamespace
        try {
            FileSystem.SYSTEM.createDirectories(directory)
            val excludedFromBackup = NSURL.fileURLWithPath(
                directory.toString(),
                isDirectory = true,
            ).setResourceValue(
                NSNumber(bool = true),
                forKey = NSURLIsExcludedFromBackupKey,
                error = null,
            )
            if (!excludedFromBackup) {
                throw PulsepondStorageException("Pulsepond could not protect durable state from backup")
            }
        } catch (error: PulsepondStorageException) {
            throw error
        } catch (_: Throwable) {
            throw PulsepondStorageException("Pulsepond could not initialize app-private storage")
        }
        return createPersistentPulsepond(
            configuration,
            FileEventPersistence(FileSystem.SYSTEM, directory),
        )
    }
}
