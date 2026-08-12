package dev.pulsepond.sdk

import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.HttpTimeout
import java.security.SecureRandom

internal actual val pulsepondPlatform: String = "android"

private val secureRandom: SecureRandom = SecureRandom()

internal actual fun fillSecureRandom(target: ByteArray) {
    secureRandom.nextBytes(target)
}

internal actual fun createPlatformHttpClient(): HttpClient = HttpClient(OkHttp) {
    expectSuccess = false
    followRedirects = false
    install(HttpTimeout) {
        requestTimeoutMillis = 10_000
        connectTimeoutMillis = 10_000
        socketTimeoutMillis = 10_000
    }
}
