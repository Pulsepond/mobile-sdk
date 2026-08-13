#!/usr/bin/env bash

configure_signing_environment() {
  if [[ -n "${SIGNING_PASSWORD:-}" ]]; then
    export ORG_GRADLE_PROJECT_signingInMemoryKeyPassword="$SIGNING_PASSWORD"
  else
    unset ORG_GRADLE_PROJECT_signingInMemoryKeyPassword
  fi
}
