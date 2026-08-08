#!/bin/bash
# Wrapper launchd actually invokes (com.idx.deadmansswitch.plist).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$REPO_ROOT/data/logs/launchd"
mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/dead_mans_switch_$(date +%Y%m%d_%H%M%S).log"

cd "$REPO_ROOT"
exec "$REPO_ROOT/.venv/bin/python" -m idx.jobs.dead_mans_switch >> "$LOG_FILE" 2>&1
