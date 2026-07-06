#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/realtime_dw_project"
docker compose down
