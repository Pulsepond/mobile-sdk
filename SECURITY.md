# Security Policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private vulnerability reporting for `Pulsepond/mobile-sdk`.

Include the affected version, platform, reproduction steps, and expected impact. Do not include real write keys, Cloudflare credentials, personal data, or production event payloads.

## Credential model

Pulsepond write keys are publishable, source-scoped ingestion credentials and cannot be treated as secrets inside a mobile binary. They must not grant read or administrative access. Control-plane credentials and Cloudflare tokens are never valid SDK configuration.
