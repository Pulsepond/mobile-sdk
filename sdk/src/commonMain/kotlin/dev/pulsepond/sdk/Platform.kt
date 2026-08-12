package dev.pulsepond.sdk

import io.ktor.client.HttpClient

internal expect val pulsepondPlatform: String

internal expect fun fillSecureRandom(target: ByteArray)

internal expect fun createPlatformHttpClient(): HttpClient
