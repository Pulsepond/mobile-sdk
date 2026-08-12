#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
requested_output_dir="${1:-}"
commit="${2:-$(git -C "$repo_root" rev-parse HEAD)}"

mkdir -p "$repo_root/build"
if [[ -n "$requested_output_dir" ]]; then
  artifact_dir="$requested_output_dir"
else
  artifact_dir="$(mktemp -d "$repo_root/build/apple-consumer.XXXXXX")"
fi
extracted_dir="$(mktemp -d "$repo_root/build/apple-extracted.XXXXXX")"

cleanup() {
  rm -rf -- "$extracted_dir"
  if [[ -z "$requested_output_dir" ]]; then
    rm -rf -- "$artifact_dir"
  fi
}
trap cleanup EXIT

"$script_dir/package-apple-artifact.sh" "$artifact_dir" "$commit"

archive_name="Pulsepond.xcframework.zip"
checksum_name="$archive_name.sha256"
provenance_name="$archive_name.provenance.json"
(
  cd "$artifact_dir"
  shasum -a 256 -c "$checksum_name"
)
archive_sha="$(shasum -a 256 "$artifact_dir/$archive_name" | awk '{print $1}')"
jq -e \
  --arg artifact "$archive_name" \
  --arg commit "$commit" \
  --arg sha256 "$archive_sha" \
  '.artifact == $artifact and .commit == $commit and .sha256 == $sha256' \
  "$artifact_dir/$provenance_name" >/dev/null

ditto -x -k "$artifact_dir/$archive_name" "$extracted_dir"
"$script_dir/check-swift-api.sh" \
  "$extracted_dir/Pulsepond.xcframework/ios-arm64-simulator"
