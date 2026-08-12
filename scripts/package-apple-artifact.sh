#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
output_dir="${1:?usage: package-apple-artifact.sh <output-directory> [commit]}"
commit="${2:-$(git -C "$repo_root" rev-parse HEAD)}"
framework="$repo_root/sdk/build/XCFrameworks/release/Pulsepond.xcframework"
archive_name="Pulsepond.xcframework.zip"
checksum_name="$archive_name.sha256"
provenance_name="$archive_name.provenance.json"

if [[ ! -d "$framework" ]]; then
  echo "Pulsepond XCFramework not found at $framework" >&2
  exit 1
fi

mkdir -p "$output_dir"
for asset in "$archive_name" "$checksum_name" "$provenance_name"; do
  if [[ -e "$output_dir/$asset" ]]; then
    echo "Refusing to overwrite existing release asset: $output_dir/$asset" >&2
    exit 1
  fi
done

ditto -c -k --sequesterRsrc --keepParent "$framework" "$output_dir/$archive_name"
archive_sha="$(shasum -a 256 "$output_dir/$archive_name" | awk '{print $1}')"
printf '%s  %s\n' "$archive_sha" "$archive_name" > "$output_dir/$checksum_name"
jq -n \
  --arg artifact "$archive_name" \
  --arg commit "$commit" \
  --arg sha256 "$archive_sha" \
  '{artifact: $artifact, commit: $commit, sha256: $sha256}' \
  > "$output_dir/$provenance_name"
