# Releasing

Pulsepond Mobile SDK publishes Android artifacts to Maven Central and attaches the Apple XCFramework archive, checksum, and provenance to the matching GitHub release. A release must never be published until every credential below is configured as a repository Actions secret:

- `MAVEN_CENTRAL_USERNAME`
- `MAVEN_CENTRAL_PASSWORD`
- `SIGNING_KEY_ID`
- `SIGNING_PASSWORD`
- `GPG_KEY_CONTENTS`

The release workflow checks only whether each value is present and never prints a secret. It stops before build, signing, or upload when configuration is incomplete.

## Prepare a release

1. Confirm `main` is green and the documented Android and Apple consumer checks pass.
2. Choose a semantic version and create an annotated tag such as `v0.1.0` on the exact `main` commit to release.
3. Push the tag.
4. Create a draft GitHub release for that existing tag and review its notes.
5. Publish the GitHub release.

The workflow checks out the fully qualified tag, proves `HEAD` matches the tag commit, derives the Maven version from the tag, and builds both platform artifacts. It then:

1. packages and verifies `Pulsepond.xcframework.zip`;
2. uploads the Apple archive, SHA-256 file, and commit-bound provenance without overwriting an existing asset set;
3. signs and publishes `dev.pulsepond:pulsepond:<version>` to Maven Central.

If a transient failure occurs, rerun the failed workflow. A retry accepts an existing Apple asset set only when all three files exist and their checksum and provenance match the same release commit. Do not create a second release for the same version.
