#!/bin/bash
# Wrapper launchd actually invokes (com.idx.daily.plist). Gives each run
# its own timestamped log file instead of one unbounded, ever-growing one.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$REPO_ROOT/data/logs/launchd"
mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/daily_$(date +%Y%m%d_%H%M%S).log"

cd "$REPO_ROOT"
exec "$REPO_ROOT/.venv/bin/python" -m idx.jobs.daily >> "$LOG_FILE" 2>&1
