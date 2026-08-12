package dev.pulsepond.sdk

import android.content.Context
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okio.FileSystem
import okio.Path.Companion.toOkioPath

/** Android entry point with app-private durable identity and offline event storage. */
public object PulsepondAndroid {
    /** Creates a client scoped to the source and environment in [configuration]. */
    @JvmStatic
    @Throws(
        CancellationException::class,
        PulsepondConfigurationException::class,
        PulsepondStorageException::class,
    )
    public suspend fun create(context: Context, configuration: PulsepondConfiguration): Pulsepond =
        withContext(Dispatchers.Default) {
            val directory = context.applicationContext.noBackupFilesDir.toOkioPath() /
                "pulsepond" /
                configuration.storageNamespace
            createOwnedPersistentPulsepond(
                configuration,
                FileEventPersistence(FileSystem.SYSTEM, directory),
            )
        }
}
