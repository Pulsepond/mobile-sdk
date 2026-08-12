# Contributing

Pulsepond Mobile SDK uses Kotlin Multiplatform and Gradle. Use JDK 21 and an Android SDK with API 36 installed.

Run the Android tests and public API check on every change:

```shell
./gradlew :sdk:testAndroidHostTest :sdk:checkKotlinAbi
```

On macOS, also validate the Apple artifact:

```shell
./gradlew :sdk:iosSimulatorArm64Test :sdk:assemblePulsepondReleaseXCFramework
```

When intentionally changing a public API, review the source and binary compatibility impact, then update the checked-in API dump:

```shell
./gradlew :sdk:updateKotlinAbi
```

Keep changes focused. New collection must be explicit, bounded, documented, covered by protocol tests, and must not expand diagnostic payloads with event or credential data.
