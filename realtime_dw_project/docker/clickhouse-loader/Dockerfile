FROM python:3.12-slim

ARG CONFLUENT_KAFKA_VERSION=2.6.1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir "confluent-kafka==${CONFLUENT_KAFKA_VERSION}" \
    && groupadd --system loader \
    && useradd --system --gid loader --home-dir /app loader

WORKDIR /app
COPY --chown=loader:loader scripts/load_kafka_to_clickhouse.py /app/load_kafka_to_clickhouse.py

USER loader
ENTRYPOINT ["python", "/app/load_kafka_to_clickhouse.py"]
