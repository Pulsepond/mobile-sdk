#!/usr/bin/env bash
set -euo pipefail

required_secrets=(
  MAVEN_CENTRAL_USERNAME
  MAVEN_CENTRAL_PASSWORD
  SIGNING_KEY_ID
  SIGNING_PASSWORD
  GPG_KEY_CONTENTS
)
missing_secrets=()

for secret_name in "${required_secrets[@]}"; do
  if [[ -z "${!secret_name:-}" ]]; then
    missing_secrets+=("$secret_name")
  fi
done

if (( ${#missing_secrets[@]} > 0 )); then
  printf 'Missing required release secrets: %s\n' "${missing_secrets[*]}" >&2
  exit 1
fi

echo "All required release secrets are configured."
