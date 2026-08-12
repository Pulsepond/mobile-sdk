#!/usr/bin/env bash
set -euo pipefail

framework_slice="${1:-sdk/build/XCFrameworks/release/Pulsepond.xcframework/ios-arm64-simulator}"
if [[ ! -d "$framework_slice/Pulsepond.framework" ]]; then
  echo "Pulsepond simulator framework not found at $framework_slice" >&2
  exit 1
fi

simulator_sdk="$(xcrun --sdk iphonesimulator --show-sdk-path)"
xcrun swiftc \
  -typecheck \
  -sdk "$simulator_sdk" \
  -target arm64-apple-ios13.0-simulator \
  -F "$framework_slice" \
  -framework Pulsepond \
  swift-consumer/Smoke.swift
