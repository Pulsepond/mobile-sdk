package dev.pulsepond.sdk

/** Stable, redacted delivery states that are safe to expose to application logging. */
public enum class PulsepondDiagnosticCode(public val wireName: String) {
    BatchRejected("batch_rejected"),
    DeliveryFailed("delivery_failed"),
    QueueFull("queue_full"),
    RetryExhausted("retry_exhausted"),
    StaleEvent("stale_event"),
}

/** A diagnostic never includes a write key, event payload, or property value. */
public class PulsepondDiagnostic public constructor(
    public val code: PulsepondDiagnosticCode,
    public val droppedEvents: Int,
    public val retryable: Boolean,
    public val status: Int? = null,
)

/** Receives redacted delivery diagnostics. Listener failures are isolated from collection. */
public fun interface PulsepondDiagnosticListener {
    public fun onDiagnostic(diagnostic: PulsepondDiagnostic)
}
