package dev.pulsepond.sdk

import io.ktor.client.HttpClient
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.contentType

internal data class TransportResponse(
    val status: Int,
    val retryAfter: String?,
)

internal interface EventTransport {
    suspend fun post(body: String): TransportResponse

    fun close()
}

internal class KtorEventTransport(
    private val configuration: PulsepondConfiguration,
    private val client: HttpClient = createPlatformHttpClient(),
) : EventTransport {
    override suspend fun post(body: String): TransportResponse {
        val response = client.post(configuration.parsedEndpoint) {
            contentType(ContentType.Application.Json)
            header(HttpHeaders.Authorization, "Bearer ${configuration.writeKey}")
            setBody(body)
        }
        return TransportResponse(
            status = response.status.value,
            retryAfter = response.headers[HttpHeaders.RetryAfter],
        )
    }

    override fun close() {
        client.close()
    }
}
