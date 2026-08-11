#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

docker exec -i rtdw_mysql mysql -uroot -proot ecommerce < scripts/schema_evolution_additive.sql
sleep 20

python3 - <<'PY'
import json
import urllib.request

with urllib.request.urlopen("http://localhost:8081/jobs/overview", timeout=10) as response:
    jobs = json.load(response)["jobs"]
failed = [job for job in jobs if job["state"] != "RUNNING"]
if len(jobs) < 9 or failed:
    raise SystemExit(f"schema drill failed: jobs={len(jobs)}, failed={failed}")
print(f"schema drill passed: {len(jobs)} jobs remain RUNNING")
PY

docker exec rtdw_mysql mysql -uroot -proot ecommerce -e \
  "SELECT table_name, schema_version, compatible_change, applied_at FROM cdc_schema_contract ORDER BY table_name, schema_version;"
