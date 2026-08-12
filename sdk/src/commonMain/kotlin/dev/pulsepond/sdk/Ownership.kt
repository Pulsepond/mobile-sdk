package dev.pulsepond.sdk

import kotlinx.atomicfu.locks.SynchronizedObject
import kotlinx.atomicfu.locks.synchronized

internal class ClientOwnershipLease(
    private val namespace: String,
) {
    private var released: Boolean = false

    fun release() {
        synchronized(ClientOwnershipRegistry) {
            if (released) return
            released = true
            ClientOwnershipRegistry.release(namespace)
        }
    }
}

internal object ClientOwnershipRegistry : SynchronizedObject() {
    private val activeNamespaces: MutableSet<String> = mutableSetOf()

    fun acquire(namespace: String): ClientOwnershipLease = synchronized(this) {
        if (!activeNamespaces.add(namespace)) {
            throw PulsepondStorageException(
                "Pulsepond already has an active client for this source and environment",
            )
        }
        ClientOwnershipLease(namespace)
    }

    fun release(namespace: String) {
        activeNamespaces.remove(namespace)
    }
}
