package dev.pulsepond.sdk

import android.content.Context
import okio.FileSystem
import okio.Path.Companion.toOkioPath

/** Android entry point with app-private durable identity and offline event storage. */
public object PulsepondAndroid {
    /** Creates a client scoped to the source and environment in [configuration]. */
    @JvmStatic
    @Throws(PulsepondConfigurationException::class, PulsepondStorageException::class)
    public fun create(context: Context, configuration: PulsepondConfiguration): Pulsepond {
        val directory = context.applicationContext.noBackupFilesDir.toOkioPath() /
            "pulsepond" /
            configuration.storageNamespace
        return createPersistentPulsepond(
            configuration,
            FileEventPersistence(FileSystem.SYSTEM, directory),
        )
    }
}
