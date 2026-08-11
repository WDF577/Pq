#!/usr/bin/env bash
set -euo pipefail

mkdir -p flink-lib

download() {
  local url="$1"
  local output="$2"
  local expected_sha256="$3"
  if [ -s "$output" ] \
    && printf '%s  %s\n' "$expected_sha256" "$output" | sha256sum --check --status; then
    echo "exists: $output"
    return
  fi
  echo "download: $output"
  local temporary="${output}.download"
  curl -fL "$url" -o "$temporary"
  printf '%s  %s\n' "$expected_sha256" "$temporary" | sha256sum --check --status
  mv -f "$temporary" "$output"
}

download "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" \
  "flink-lib/flink-sql-connector-kafka-3.1.0-1.18.jar" \
  "ad001ada5a43aca44341790a1e28135c808034be2e6b6b47e0394ad8d4d2bf0b"

download "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar" \
  "flink-lib/flink-connector-jdbc-3.1.2-1.18.jar" \
  "141b294306ceabc82fee624e12c6844cf4777ee0dd529f376d18ee98af2d9c9c"

download "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar" \
  "flink-lib/mysql-connector-j-8.3.0.jar" \
  "94e7fa815370cdcefed915db7f53f88445fac110f8c3818392b992ec9ee6d295"

download "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/3.2.1/flink-sql-connector-mysql-cdc-3.2.1.jar" \
  "flink-lib/flink-sql-connector-mysql-cdc-3.2.1.jar" \
  "7cd1c46d722c02fdfa5ee421f67ec9492b2275a2c512e4c9999053bdd41025b1"

download "https://repo1.maven.org/maven2/org/apache/flink/flink-statebackend-rocksdb/1.18.1/flink-statebackend-rocksdb-1.18.1.jar" \
  "flink-lib/flink-statebackend-rocksdb-1.18.1.jar" \
  "9f2ac426d0e5ca91cfcb41bd8575dbbce9e69373acabe72555220bec0cbe56c0"

download "https://repo1.maven.org/maven2/com/ververica/frocksdbjni/6.20.3-ververica-2.0/frocksdbjni-6.20.3-ververica-2.0.jar" \
  "flink-lib/frocksdbjni-6.20.3-ververica-2.0.jar" \
  "5e6e5063b75196a17fbeaf656f088a807f83177525d9ecbec7125b9f3630b966"

download "https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar" \
  "flink-lib/flink-metrics-prometheus-1.18.1.jar" \
  "3f8a0e13d0cb33df0264272f649af63fde34ba5f6ebb8132afd2797710f89f76"

echo "connector jars are ready."
