#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
preflight="$script_dir/check-release-secrets.sh"
source "$script_dir/configure-signing-environment.sh"

run_preflight() {
  MAVEN_CENTRAL_USERNAME_CONFIGURED="$1" \
    MAVEN_CENTRAL_PASSWORD_CONFIGURED="$2" \
    SIGNING_KEY_ID_CONFIGURED="$3" \
    GPG_KEY_CONTENTS_CONFIGURED="$4" \
    SIGNING_PASSWORD_CONFIGURED=false \
    "$preflight"
}

run_preflight true true true true

if output="$(run_preflight true true true false 2>&1)"; then
  echo "Release preflight unexpectedly accepted a missing GPG key." >&2
  exit 1
fi

grep -Fq "Missing required release secrets: GPG_KEY_CONTENTS" <<< "$output"

if output="$(run_preflight true true false true 2>&1)"; then
  echo "Release preflight unexpectedly accepted a missing signing key ID." >&2
  exit 1
fi

grep -Fq "Missing required release secrets: SIGNING_KEY_ID" <<< "$output"

SIGNING_PASSWORD=""
ORG_GRADLE_PROJECT_signingInMemoryKeyPassword="unexpected"
configure_signing_environment
if [[ -v ORG_GRADLE_PROJECT_signingInMemoryKeyPassword ]]; then
  echo "Passwordless signing unexpectedly exported a Gradle password property." >&2
  exit 1
fi

SIGNING_PASSWORD="test-passphrase"
configure_signing_environment
if [[ "$ORG_GRADLE_PROJECT_signingInMemoryKeyPassword" != "$SIGNING_PASSWORD" ]]; then
  echo "Protected signing key passphrase was not exported to Gradle." >&2
  exit 1
fi
