package dev.pulsepond.sdk

private const val sessionTimeoutMilliseconds: Long = 30 * 60 * 1_000

internal data class Identity(
    val anonymousInstallationId: String,
    val sessionId: String,
)

internal class IdentityManager(
    private val random: (ByteArray) -> Unit,
    nowMilliseconds: Long,
    installationId: String = createUuidV7(nowMilliseconds, random),
) {
    private var anonymousInstallationId: String = installationId
    private var sessionId: String = createUuidV7(nowMilliseconds, random)
    private var lastActivityMilliseconds: Long = nowMilliseconds

    fun current(nowMilliseconds: Long): Identity {
        if (
            nowMilliseconds < lastActivityMilliseconds ||
            nowMilliseconds - lastActivityMilliseconds > sessionTimeoutMilliseconds
        ) {
            sessionId = createUuidV7(nowMilliseconds, random)
        }
        lastActivityMilliseconds = nowMilliseconds
        return Identity(anonymousInstallationId, sessionId)
    }

    fun reset(nowMilliseconds: Long, installationId: String = createUuidV7(nowMilliseconds, random)) {
        anonymousInstallationId = installationId
        sessionId = createUuidV7(nowMilliseconds, random)
        lastActivityMilliseconds = nowMilliseconds
    }
}
