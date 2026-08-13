# Releasing

Pulsepond Mobile SDK publishes Android artifacts to Maven Central and attaches the Apple XCFramework archive, checksum, and provenance to the matching GitHub release. A release must never be published until these repository Actions secrets are configured:

- `MAVEN_CENTRAL_USERNAME`
- `MAVEN_CENTRAL_PASSWORD`
- `SIGNING_KEY_ID`
- `GPG_KEY_CONTENTS`

Configure `SIGNING_PASSWORD` only when the exported GPG private key is protected by a passphrase. The current workflow requires `SIGNING_KEY_ID` so Gradle selects the intended key explicitly.

The release workflow passes only configured/not-configured flags to the preflight script; the script never receives or prints a secret value. It stops before build, signing, or upload when configuration is incomplete. The actual credentials are exposed only to the final Maven publication step.

## Prepare a release

1. Confirm `main` is green and the documented Android and Apple consumer checks pass.
2. Choose a semantic version and create an annotated tag such as `v0.1.0` on the exact `main` commit to release.
3. Push the tag.
4. Create a draft GitHub release for that existing tag and review its notes.
5. Publish the GitHub release.

The workflow checks out the fully qualified tag without persisting GitHub credentials, proves `HEAD` matches the tag commit, and requires that commit to be reachable from `origin/main` before any repository script runs. It derives the Maven version from the tag and builds both platform artifacts. It then:

1. packages and verifies `Pulsepond.xcframework.zip`;
2. uploads the Apple archive, SHA-256 file, and commit-bound provenance without overwriting an existing asset set;
3. signs and publishes `dev.pulsepond:pulsepond:<version>` to Maven Central.

If a transient failure occurs, rerun the failed workflow. A retry accepts an existing Apple asset set only when all three files exist and their checksum and provenance match the same release commit. Do not create a second release for the same version.
