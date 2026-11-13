#!/usr/bin/env bash
# 后台启动 token_issue_stat_task，日志追加到 logs/run.log
set -euo pipefail

cd "$(dirname "$0")"

mkdir -p logs
nohup ./token_issue_stat_task -config config.yaml >> logs/run.log 2>&1 &

echo "token_issue_stat_task started, pid=$!, log=logs/run.log"
