@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package dev.pulsepond.sdk

import io.ktor.client.HttpClient
import io.ktor.client.engine.darwin.Darwin
import io.ktor.client.plugins.HttpTimeout
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.usePinned
import platform.Security.SecRandomCopyBytes
import platform.Security.errSecSuccess
import platform.Security.kSecRandomDefault

internal actual val pulsepondPlatform: String = "ios"

internal actual fun fillSecureRandom(target: ByteArray) {
    if (target.isEmpty()) return
    val status = target.usePinned { pinned ->
        SecRandomCopyBytes(kSecRandomDefault, target.size.convert(), pinned.addressOf(0))
    }
    check(status == errSecSuccess) { "secure random generation failed" }
}

internal actual fun createPlatformHttpClient(): HttpClient = HttpClient(Darwin) {
    expectSuccess = false
    followRedirects = false
    install(HttpTimeout) {
        requestTimeoutMillis = 10_000
        connectTimeoutMillis = 10_000
        socketTimeoutMillis = 10_000
    }
}
