#!/usr/bin/env bash
#
# Cross-compile every Go Lambda binary into infra/cdk/bin/<name>/bootstrap
# so CDK's Code_FromAsset can pick them up. The CDK stack uses
# Runtime_PROVIDED_AL2023 + Architecture_ARM_64, so we build for
# linux/arm64 and name the binary "bootstrap" (the convention the
# provided.* runtimes require).
#
# Usage: ./build-lambdas.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="$ROOT/infra/cdk/bin"

LAMBDAS=(
    gemini-lambda
    submit-pages-lambda
    check-pages-lambda
    merge-lambda
    mark-job-failed-lambda
)

rm -rf "$OUT"
mkdir -p "$OUT"

cd "$ROOT"

for lambda in "${LAMBDAS[@]}"; do
    echo "building $lambda → bin/$lambda/bootstrap"
    mkdir -p "$OUT/$lambda"
    GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build \
        -trimpath \
        -ldflags="-s -w" \
        -o "$OUT/$lambda/bootstrap" \
        "./cmd/$lambda"
done

echo "ok: $OUT"
