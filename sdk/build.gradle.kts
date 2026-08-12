import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.plugin.mpp.apple.XCFramework

plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.android.kotlin.multiplatform.library)
    alias(libs.plugins.kotlinSerialization)
    alias(libs.plugins.vanniktechMavenPublish)
}

group = "dev.pulsepond"
version = providers.gradleProperty("VERSION_NAME").orElse("0.1.0-SNAPSHOT").get()

kotlin {
    explicitApi()
    @OptIn(org.jetbrains.kotlin.gradle.dsl.abi.ExperimentalAbiValidation::class)
    abiValidation()

    android {
        namespace = "dev.pulsepond.sdk"
        compileSdk = libs.versions.android.compileSdk.get().toInt()
        minSdk = libs.versions.android.minSdk.get().toInt()

        withJava()
        withHostTestBuilder {}.configure {}

        compilerOptions {
            jvmTarget = JvmTarget.JVM_11
        }
    }

    val pulsepondXcframework = XCFramework("Pulsepond")
    listOf(iosArm64(), iosSimulatorArm64()).forEach { target ->
        target.binaries.framework {
            baseName = "Pulsepond"
            isStatic = true
            binaryOption("bundleId", "dev.pulsepond.sdk")
            pulsepondXcframework.add(this)
        }
    }

    sourceSets {
        commonMain.dependencies {
            implementation(libs.atomicfu)
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.ktor.client.core)
            implementation(libs.okio)
        }

        commonTest.dependencies {
            implementation(libs.kotlinx.coroutines.test)
            implementation(libs.kotlin.test)
            implementation(libs.okio.fakefilesystem)
        }

        androidMain.dependencies {
            implementation(libs.ktor.client.okhttp)
        }

        getByName("androidHostTest") {
            resources.srcDir(rootProject.layout.projectDirectory.dir("protocol-fixtures"))
        }

        iosMain.dependencies {
            implementation(libs.ktor.client.darwin)
        }
    }
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()

    coordinates(group.toString(), "pulsepond", version.toString())

    pom {
        name = "Pulsepond Mobile SDK"
        description = "Privacy-conscious product analytics for Android and iOS."
        inceptionYear = "2026"
        url = "https://github.com/Pulsepond/mobile-sdk"
        licenses {
            license {
                name = "The Apache License, Version 2.0"
                url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                distribution = "repo"
            }
        }
        developers {
            developer {
                id = "pulsepond"
                name = "Pulsepond"
                url = "https://pulsepond.dev"
            }
        }
        scm {
            url = "https://github.com/Pulsepond/mobile-sdk"
            connection = "scm:git:https://github.com/Pulsepond/mobile-sdk.git"
            developerConnection = "scm:git:ssh://git@github.com/Pulsepond/mobile-sdk.git"
        }
    }
}
