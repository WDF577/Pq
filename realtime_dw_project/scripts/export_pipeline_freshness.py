#!/usr/bin/env python3
"""Expose end-to-end ClickHouse data freshness as Prometheus metrics.

The exporter deliberately separates observability from alert policy.  A quiet
business period and a broken upstream can produce the same data age, so stale
data alerts are gated by ``PIPELINE_FRESHNESS_MONITOR_ENABLED`` and are disabled
by default.
"""

from __future__ import annotations

import base64
import logging
import os
import re
import signal
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Protocol


LOGGER = logging.getLogger("pipeline-freshness-exporter")


@dataclass(frozen=True)
class QueryTarget:
    pipeline: str
    table: str
    query: str


_TIMEZONE_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_+\-/]{0,63}$")


def build_targets(business_timezone: str) -> tuple[QueryTarget, ...]:
    """Build queries that interpret warehouse wall-clock values explicitly.

    The demo's Flink/MySQL timestamps are timezone-less business timestamps.
    ClickHouse itself runs in UTC, so converting them directly with
    ``toUnixTimestamp`` would make Asia/Shanghai data appear eight hours in the
    future.  Parsing the displayed wall clock with an explicit business
    timezone keeps the freshness SLA honest and portable across hosts.
    """

    if not _TIMEZONE_PATTERN.fullmatch(business_timezone):
        raise ValueError(f"invalid PIPELINE_BUSINESS_TIMEZONE: {business_timezone!r}")
    timezone = business_timezone

    def epoch(expression: str) -> str:
        return (
            "(toUnixTimestamp64Milli(parseDateTime64BestEffortOrNull(toString("
            f"{expression}), 3, '{timezone}')) / 1000)"
        )

    return (
        QueryTarget(
            pipeline="behavior_dwd",
            table="dwd_user_behavior",
            query=(
                f"SELECT ifNull({epoch('max(event_ts)')}, 0) "
                "FROM default.dwd_user_behavior FINAL"
            ),
        ),
        QueryTarget(
            pipeline="order_dwd",
            table="dwd_order_detail",
            query=f"""
                SELECT ifNull(
                    max(
                        greatest(
                            ifNull({epoch('order_create_time')}, 0),
                            ifNull({epoch('order_update_time')}, 0),
                            ifNull({epoch('detail_update_time')}, 0),
                            ifNull({epoch('payment_time')}, 0),
                            ifNull({epoch('refund_time')}, 0)
                        )
                    ),
                    0
                )
                FROM default.dwd_order_detail FINAL
                WHERE is_deleted = 0
            """,
        ),
        QueryTarget(
            pipeline="behavior_ads",
            table="ads_realtime_overview",
            query=(
                f"SELECT ifNull({epoch('max(window_end)')}, 0) "
                "FROM default.ads_realtime_overview FINAL"
            ),
        ),
    )


TARGETS = build_targets("Asia/Shanghai")


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


def positive_float_from_env(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


class ScalarQueryClient(Protocol):
    def query_scalar(self, query: str) -> float: ...


class ClickHouseClient:
    def __init__(self, url: str, user: str, password: str, timeout_seconds: float) -> None:
        self._url = url.rstrip("/") + "/"
        self._timeout_seconds = timeout_seconds
        token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        self._headers = {
            "Authorization": f"Basic {token}",
            "Content-Type": "text/plain; charset=utf-8",
            "User-Agent": "realtime-dw-freshness-exporter/1.0",
        }

    def query_scalar(self, query: str) -> float:
        request = urllib.request.Request(
            self._url,
            data=query.encode("utf-8"),
            headers=self._headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_seconds) as response:
                payload = response.read().decode("utf-8").strip()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"ClickHouse returned HTTP {exc.code}: {detail}") from exc
        if not payload:
            raise RuntimeError("ClickHouse returned an empty scalar response")
        return float(payload.splitlines()[0])


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


class FreshnessCollector:
    def __init__(
        self,
        client: ScalarQueryClient,
        *,
        enabled: bool,
        stale_after_seconds: float,
        targets: tuple[QueryTarget, ...] = TARGETS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._enabled = enabled
        self._stale_after_seconds = stale_after_seconds
        self._targets = targets
        self._clock = clock
        self._lock = threading.Lock()
        self._max_business_time: dict[tuple[str, str], float | None] = {
            (target.pipeline, target.table): None for target in targets
        }
        self._collection_success = 0
        self._collection_failures_total = 0
        self._last_collection_time = 0.0
        self._last_success_time = 0.0

    def collect(self) -> None:
        results: dict[tuple[str, str], float] = {}
        failures = 0
        for target in self._targets:
            try:
                results[(target.pipeline, target.table)] = self._client.query_scalar(target.query)
            except Exception:
                failures += 1
                LOGGER.exception(
                    "freshness query failed pipeline=%s table=%s",
                    target.pipeline,
                    target.table,
                )

        collected_at = self._clock()
        with self._lock:
            self._max_business_time.update(results)
            self._last_collection_time = collected_at
            self._collection_success = int(failures == 0)
            self._collection_failures_total += failures
            if failures == 0:
                self._last_success_time = collected_at

    def render_metrics(self) -> str:
        now = self._clock()
        with self._lock:
            values = dict(self._max_business_time)
            collection_success = self._collection_success
            failures_total = self._collection_failures_total
            last_collection_time = self._last_collection_time
            last_success_time = self._last_success_time

        lines = [
            "# HELP pipeline_freshness_monitor_enabled Whether stale-data alerting is explicitly enabled.",
            "# TYPE pipeline_freshness_monitor_enabled gauge",
            f"pipeline_freshness_monitor_enabled {int(self._enabled)}",
            "# HELP pipeline_freshness_stale_threshold_seconds Configured maximum acceptable business-data age.",
            "# TYPE pipeline_freshness_stale_threshold_seconds gauge",
            f"pipeline_freshness_stale_threshold_seconds {self._stale_after_seconds:g}",
            "# HELP pipeline_freshness_collection_success Whether every ClickHouse freshness query in the latest collection succeeded.",
            "# TYPE pipeline_freshness_collection_success gauge",
            f"pipeline_freshness_collection_success {collection_success}",
            "# HELP pipeline_freshness_collection_failures_total Total failed ClickHouse freshness queries.",
            "# TYPE pipeline_freshness_collection_failures_total counter",
            f"pipeline_freshness_collection_failures_total {failures_total}",
            "# HELP pipeline_freshness_last_collection_unixtime_seconds Unix time of the latest collection attempt.",
            "# TYPE pipeline_freshness_last_collection_unixtime_seconds gauge",
            f"pipeline_freshness_last_collection_unixtime_seconds {last_collection_time:.6f}",
            "# HELP pipeline_freshness_last_success_unixtime_seconds Unix time of the latest fully successful collection.",
            "# TYPE pipeline_freshness_last_success_unixtime_seconds gauge",
            f"pipeline_freshness_last_success_unixtime_seconds {last_success_time:.6f}",
            "# HELP pipeline_data_available Whether the monitored table currently contains a business timestamp.",
            "# TYPE pipeline_data_available gauge",
            "# HELP pipeline_data_max_business_unixtime_seconds Latest business timestamp found in the monitored table.",
            "# TYPE pipeline_data_max_business_unixtime_seconds gauge",
            "# HELP pipeline_data_age_seconds Seconds since the latest business timestamp in the monitored table.",
            "# TYPE pipeline_data_age_seconds gauge",
        ]

        for target in self._targets:
            key = (target.pipeline, target.table)
            value = values[key]
            labels = (
                f'pipeline="{_escape_label(target.pipeline)}",'
                f'table="{_escape_label(target.table)}"'
            )
            available = int(value is not None and value > 0)
            lines.append(f"pipeline_data_available{{{labels}}} {available}")
            if available:
                lines.append(f"pipeline_data_max_business_unixtime_seconds{{{labels}}} {value:.6f}")
                lines.append(f"pipeline_data_age_seconds{{{labels}}} {max(0.0, now - value):.6f}")

        return "\n".join(lines) + "\n"


def make_handler(collector: FreshnessCollector) -> type[BaseHTTPRequestHandler]:
    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
            path = urllib.parse.urlsplit(self.path).path
            if path == "/metrics":
                payload = collector.render_metrics().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            if path == "/healthz":
                payload = b"ok\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            self.send_error(404)

        def log_message(self, fmt: str, *args: object) -> None:
            LOGGER.debug("http " + fmt, *args)

    return MetricsHandler


def collection_loop(
    collector: FreshnessCollector,
    interval_seconds: float,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        collector.collect()
        stop_event.wait(interval_seconds)


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    enabled = parse_bool(os.getenv("PIPELINE_FRESHNESS_MONITOR_ENABLED", "false"))
    interval_seconds = positive_float_from_env("PIPELINE_FRESHNESS_INTERVAL_SECONDS", 15)
    stale_after_seconds = positive_float_from_env("PIPELINE_FRESHNESS_STALE_AFTER_SECONDS", 300)
    timeout_seconds = positive_float_from_env("PIPELINE_FRESHNESS_QUERY_TIMEOUT_SECONDS", 5)
    port = int(os.getenv("PIPELINE_FRESHNESS_METRICS_PORT", "9420"))
    if not 1 <= port <= 65535:
        raise ValueError("PIPELINE_FRESHNESS_METRICS_PORT must be between 1 and 65535")

    client = ClickHouseClient(
        url=os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123"),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
        timeout_seconds=timeout_seconds,
    )
    collector = FreshnessCollector(
        client,
        enabled=enabled,
        stale_after_seconds=stale_after_seconds,
        targets=build_targets(os.getenv("PIPELINE_BUSINESS_TIMEZONE", "Asia/Shanghai")),
    )
    stop_event = threading.Event()
    for signal_number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signal_number, lambda _signum, _frame: stop_event.set())

    worker = threading.Thread(
        target=collection_loop,
        args=(collector, interval_seconds, stop_event),
        name="freshness-collector",
        daemon=True,
    )
    worker.start()

    server = ThreadingHTTPServer(("0.0.0.0", port), make_handler(collector))
    server.timeout = 1
    LOGGER.info(
        "freshness exporter listening port=%s monitor_enabled=%s stale_after_seconds=%s",
        port,
        enabled,
        stale_after_seconds,
    )
    try:
        while not stop_event.is_set():
            server.handle_request()
    finally:
        stop_event.set()
        worker.join(timeout=max(1.0, timeout_seconds + 1.0))
        server.server_close()


if __name__ == "__main__":
    main()
