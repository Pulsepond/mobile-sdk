package dev.pulsepond.sdk

import io.ktor.http.URLProtocol
import io.ktor.http.Url

private val writeKeyPattern = Regex("^ppw_v1_[0-9a-f]{32}_[0-9a-f]{64}$")

/** Immutable configuration for one source-scoped Pulsepond client. */
public class PulsepondConfiguration @Throws(PulsepondConfigurationException::class)
public constructor(
    public val endpoint: String,
    public val writeKey: String,
    public val environment: String,
    public val appVersion: String? = null,
    public val release: String? = null,
    public val batchSize: Int = 20,
    public val flushIntervalMilliseconds: Long = 5_000,
    public val maxQueueSize: Int = 1_000,
    public val eventTtlMilliseconds: Long = 23 * 60 * 60 * 1_000,
    public val diagnosticListener: PulsepondDiagnosticListener? = null,
) {
    internal val parsedEndpoint: Url

    init {
        parsedEndpoint = validateEndpoint(endpoint)
        if (!writeKeyPattern.matches(writeKey)) {
            throw PulsepondConfigurationException(
                "writeKey must be a canonical Pulsepond publishable key",
            )
        }
        try {
            validateSlug("environment", environment, 32)
            validateOptionalText("appVersion", appVersion, 64)
            validateOptionalText("release", release, 128)
        } catch (error: PulsepondValidationException) {
            throw PulsepondConfigurationException(error.message ?: "invalid Pulsepond configuration")
        }
        if (batchSize !in 1..100) {
            throw PulsepondConfigurationException("batchSize must be between 1 and 100")
        }
        if (flushIntervalMilliseconds !in 0..3_600_000) {
            throw PulsepondConfigurationException(
                "flushIntervalMilliseconds must be between 0 and 3600000",
            )
        }
        if (maxQueueSize !in batchSize..10_000) {
            throw PulsepondConfigurationException(
                "maxQueueSize must be between batchSize and 10000",
            )
        }
        if (eventTtlMilliseconds !in 60_000..604_800_000) {
            throw PulsepondConfigurationException(
                "eventTtlMilliseconds must be between 60000 and 604800000",
            )
        }
    }
}

internal val PulsepondConfiguration.storageNamespace: String
    get() {
        val sourceId = writeKey.removePrefix("ppw_v1_").substringBefore('_')
        return "$sourceId/$environment"
    }

private fun validateEndpoint(value: String): Url {
    val url = try {
        Url(value)
    } catch (_: Throwable) {
        throw PulsepondConfigurationException(
            "endpoint must be an absolute Pulsepond /v1/batch URL",
        )
    }
    val localHttp =
        url.protocol == URLProtocol.HTTP &&
            (url.host == "localhost" || url.host == "127.0.0.1" || url.host == "::1")
    if (
        (url.protocol != URLProtocol.HTTPS && !localHttp) ||
        url.host.isEmpty() ||
        url.encodedPath != "/v1/batch" ||
        !url.parameters.isEmpty() ||
        url.fragment.isNotEmpty() ||
        url.user != null ||
        url.password != null
    ) {
        throw PulsepondConfigurationException(
            "endpoint must be HTTPS with the exact /v1/batch path and no credentials, query, or fragment",
        )
    }
    return url
}

internal fun validateSlug(field: String, value: String, maximumLength: Int) {
    if (
        value.isEmpty() ||
        value.length > maximumLength ||
        !value.first().isAsciiAlphaNumeric() ||
        value.any { !it.isAsciiSlugCharacter() }
    ) {
        throw PulsepondValidationException(
            "$field must be an ASCII slug within $maximumLength characters",
        )
    }
}

internal fun validateOptionalText(field: String, value: String?, maximumLength: Int) {
    if (value == null) return
    validatePrintableText(field, value, maximumLength)
}

internal fun validatePrintableText(field: String, value: String, maximumLength: Int) {
    if (
        value.isEmpty() ||
        value.length > maximumLength ||
        value.first().code !in 0x21..0x7e ||
        value.last().code !in 0x21..0x7e ||
        value.any { it.code !in 0x20..0x7e }
    ) {
        throw PulsepondValidationException(
            "$field must be trimmed printable ASCII within $maximumLength characters",
        )
    }
}

private fun Char.isAsciiAlphaNumeric(): Boolean =
    this in 'A'..'Z' || this in 'a'..'z' || this in '0'..'9'

private fun Char.isAsciiSlugCharacter(): Boolean =
    isAsciiAlphaNumeric() || this == '_' || this == '.' || this == '-'
