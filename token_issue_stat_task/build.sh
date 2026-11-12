#!/usr/bin/env bash
# 物理机打包（Linux amd64，静态二进制，免装依赖 ✓）
set -euo pipefail
cd "$(dirname "$0")"

OUT=${1:-token_issue_stat_task}
GOOS=${GOOS:-linux} GOARCH=${GOARCH:-amd64} CGO_ENABLED=0 \
  go build -trimpath -ldflags "-s -w" -o "$OUT" .

echo "built: $OUT"
ls -lh "$OUT"
