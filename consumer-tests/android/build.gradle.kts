plugins {
    id("com.android.application") version "9.1.0"
}

android {
    namespace = "dev.pulsepond.consumer"
    compileSdk = 36

    defaultConfig {
        applicationId = "dev.pulsepond.consumer"
        minSdk = 24
        targetSdk = 36
        versionCode = 1
        versionName = "1.0"
    }
}

dependencies {
    val pulsepondVersion = providers.gradleProperty("pulsepondVersion").get()
    implementation("dev.pulsepond:pulsepond:$pulsepondVersion")
}
