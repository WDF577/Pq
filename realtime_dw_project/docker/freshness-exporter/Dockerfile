FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --system freshness \
    && useradd --system --gid freshness --home-dir /app freshness

WORKDIR /app
COPY --chown=freshness:freshness scripts/export_pipeline_freshness.py /app/export_pipeline_freshness.py

USER freshness
EXPOSE 9420
ENTRYPOINT ["python", "/app/export_pipeline_freshness.py"]
