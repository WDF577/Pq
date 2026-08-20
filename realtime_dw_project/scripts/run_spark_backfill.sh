#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 START_DATE END_DATE [--allow-mismatch]" >&2
  exit 2
fi

start_date="$1"
end_date="$2"
allow_mismatch="false"
if [[ "${3:-}" == "--allow-mismatch" ]]; then
  allow_mismatch="true"
elif [[ -n "${3:-}" ]]; then
  echo "unknown option: $3" >&2
  exit 2
fi
[[ "$start_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "invalid START_DATE" >&2; exit 2; }
[[ "$end_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "invalid END_DATE" >&2; exit 2; }
[[ "$start_date" < "$end_date" ]] || { echo "END_DATE must be later than START_DATE" >&2; exit 2; }

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
driver="$project_root/flink-lib/mysql-connector-j-8.3.0.jar"
[[ -s "$driver" ]] || { echo "missing $driver; run scripts/download_connectors.sh first" >&2; exit 1; }
mkdir -p "$project_root/data/spark-warehouse" "$project_root/artifacts"

cd "$project_root"
docker compose --profile batch run --rm \
  -e "BACKFILL_START_DATE=$start_date" \
  -e "BACKFILL_END_DATE=$end_date" \
  -e "ALLOW_RECONCILIATION_MISMATCH=$allow_mismatch" \
  spark-backfill
