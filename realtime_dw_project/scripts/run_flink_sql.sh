#!/usr/bin/env bash
set -euo pipefail

MSYS_NO_PATHCONV=1 cat \
  flink-sql/01_create_source_tables.sql \
  flink-sql/02_create_dwd_tables.sql \
  flink-sql/03_create_dws_ads_tables.sql \
  | MSYS_NO_PATHCONV=1 docker exec -i rtdw_flink_jobmanager /opt/flink/bin/sql-client.sh
