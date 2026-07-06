#!/usr/bin/env bash
set -euo pipefail

mkdir -p flink-lib

download() {
  local url="$1"
  local output="$2"
  if [ -s "$output" ]; then
    echo "exists: $output"
    return
  fi
  echo "download: $output"
  curl -fL "$url" -o "$output"
}

download "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" \
  "flink-lib/flink-sql-connector-kafka-3.1.0-1.18.jar"

download "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar" \
  "flink-lib/flink-connector-jdbc-3.1.2-1.18.jar"

download "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar" \
  "flink-lib/mysql-connector-j-8.3.0.jar"

echo "connector jars are ready."
