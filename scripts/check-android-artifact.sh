#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
version="${PULSEPOND_ARTIFACT_TEST_VERSION:-0.0.0-artifact-test-SNAPSHOT}"

mkdir -p "$repo_root/build"
artifact_root="$(mktemp -d "$repo_root/build/android-consumer.XXXXXX")"
cleanup() {
  rm -rf -- "$artifact_root"
}
trap cleanup EXIT

"$repo_root/gradlew" \
  -p "$repo_root" \
  :sdk:publishToMavenLocal \
  -PpulsepondVersion="$version" \
  -Dmaven.repo.local="$artifact_root/maven" \
  --no-configuration-cache

"$repo_root/gradlew" \
  -p "$repo_root/consumer-tests/android" \
  clean compileDebugKotlin \
  -PpulsepondVersion="$version" \
  -Dmaven.repo.local="$artifact_root/maven" \
  --no-configuration-cache \
  --rerun-tasks
