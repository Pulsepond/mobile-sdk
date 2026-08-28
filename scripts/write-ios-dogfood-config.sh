#!/usr/bin/env bash
set -euo pipefail

required=(
  PULSEPOND_ENDPOINT
  PULSEPOND_WRITE_KEY
  PULSEPOND_DEPLOYMENT_ID
  PULSEPOND_PROJECT_ID
  PULSEPOND_SOURCE_ID
  PULSEPOND_EVENT_NAME
  PULSEPOND_ARTIFACT_VERSION
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "$name is required" >&2
    exit 1
  fi
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output="$repo_root/dogfood/ios/Tests/PulsepondDogfoodTests/Fixtures/config.json"

jq -n \
  --arg endpoint "$PULSEPOND_ENDPOINT" \
  --arg writeKey "$PULSEPOND_WRITE_KEY" \
  --arg deploymentId "$PULSEPOND_DEPLOYMENT_ID" \
  --arg projectId "$PULSEPOND_PROJECT_ID" \
  --arg sourceId "$PULSEPOND_SOURCE_ID" \
  --arg eventName "$PULSEPOND_EVENT_NAME" \
  --arg artifactVersion "$PULSEPOND_ARTIFACT_VERSION" \
  '{
    endpoint: $endpoint,
    writeKey: $writeKey,
    deploymentId: $deploymentId,
    projectId: $projectId,
    sourceId: $sourceId,
    eventName: $eventName,
    artifactVersion: $artifactVersion
  }' > "$output"
chmod 600 "$output"
