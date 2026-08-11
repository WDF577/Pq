#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

journeys="${1:-10000}"
orders="${2:-1000}"

echo "The Windows benchmark is the reference implementation because this project"
echo "is developed and validated with Docker Desktop on Windows."
echo "Run: powershell -ExecutionPolicy Bypass -File scripts/benchmark_pipeline.ps1 -Journeys $journeys -Orders $orders"
echo "The script records incremental Kafka offsets, runs a latest-offset ClickHouse"
echo "consumer concurrently, verifies logical idempotency, and writes artifacts/benchmark_*.md."
