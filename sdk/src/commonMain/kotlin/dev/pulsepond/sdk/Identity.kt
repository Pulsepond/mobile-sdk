package dev.pulsepond.sdk

private const val sessionTimeoutMilliseconds: Long = 30 * 60 * 1_000

internal data class Identity(
    val anonymousInstallationId: String,
    val sessionId: String,
)

internal class IdentityManager(
    private val random: (ByteArray) -> Unit,
    nowMilliseconds: Long,
) {
    private var anonymousInstallationId: String = createUuidV7(nowMilliseconds, random)
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

    fun reset(nowMilliseconds: Long) {
        anonymousInstallationId = createUuidV7(nowMilliseconds, random)
        sessionId = createUuidV7(nowMilliseconds, random)
        lastActivityMilliseconds = nowMilliseconds
    }
}
