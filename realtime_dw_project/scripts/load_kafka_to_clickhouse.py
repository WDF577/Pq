import argparse
import base64
import hashlib
import json
import os
import re
import signal
import threading
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Consumer, Producer, TopicPartition


TOPIC_CONFIG = {
    "dwd_dirty_behavior": {
        "table": "dwd_dirty_behavior",
        "required_fields": ("error_reason",),
        "field_types": {
            "event_id": "string?", "session_id": "string?", "order_id": "string?",
            "event_sequence": "int32?", "event_version": "uint64?", "user_id": "int64?",
            "product_id": "int64?", "shop_id": "int64?", "event_type": "string?",
            "channel": "string?", "amount": "decimal:10:2?", "event_time": "string?",
            "error_reason": "string",
        },
    },
    "dwd_user_behavior": {
        "table": "dwd_user_behavior",
        "datetime_fields": ("event_ts",),
        "required_fields": (
            "event_id", "session_id", "event_sequence", "event_version", "user_id",
            "product_id", "shop_id", "event_type", "channel", "amount", "event_ts",
        ),
        "field_types": {
            "event_id": "string", "session_id": "string", "order_id": "string?",
            "event_sequence": "int32", "event_version": "uint64", "user_id": "int64",
            "product_id": "int64", "product_name": "string?", "category_id": "int64?",
            "category_name": "string?", "shop_id": "int64", "shop_name": "string?",
            "event_type": "string", "channel": "string", "amount": "decimal:10:2",
            "province": "string?", "city": "string?", "event_ts": "datetime",
        },
    },
    "ads_realtime_overview": {
        "table": "ads_realtime_overview",
        "datetime_fields": ("window_start", "window_end"),
        "required_fields": (
            "window_start", "window_end", "pv", "uv", "cart_users", "order_users",
            "pay_users", "pay_amount",
        ),
        "field_types": {
            "window_start": "datetime", "window_end": "datetime", "pv": "int64",
            "uv": "int64", "cart_users": "int64", "order_users": "int64",
            "pay_users": "int64", "pay_amount": "decimal:18:2",
        },
    },
    "ads_product_rank": {
        "table": "ads_product_rank",
        "datetime_fields": ("window_start", "window_end"),
        "required_fields": (
            "window_start", "window_end", "product_id", "pay_count", "pay_amount",
        ),
        "field_types": {
            "window_start": "datetime", "window_end": "datetime", "product_id": "int64",
            "product_name": "string?", "category_name": "string?", "pay_count": "int64",
            "pay_amount": "decimal:18:2",
        },
    },
    "ads_category_rank": {
        "table": "ads_category_rank",
        "datetime_fields": ("window_start", "window_end"),
        "required_fields": (
            "window_start", "window_end", "category_name", "pay_count", "pay_users",
            "pay_amount",
        ),
        "field_types": {
            "window_start": "datetime", "window_end": "datetime", "category_name": "string",
            "pay_count": "int64", "pay_users": "int64", "pay_amount": "decimal:18:2",
        },
    },
    "ads_channel_funnel": {
        "table": "ads_channel_funnel",
        "datetime_fields": ("window_start", "window_end"),
        "required_fields": (
            "window_start", "window_end", "channel", "view_users", "cart_users",
            "order_users", "pay_users",
        ),
        "field_types": {
            "window_start": "datetime", "window_end": "datetime", "channel": "string",
            "view_users": "int64", "cart_users": "int64", "order_users": "int64",
            "pay_users": "int64",
        },
    },
    "ods_dim_product_scd2": {
        "table": "dim_product_scd2",
        "datetime_fields": ("effective_from", "effective_to", "updated_at"),
        "required_fields": (
            "product_id", "version_no", "product_name", "category_id", "category_name",
            "price", "effective_from", "effective_to", "is_current", "updated_at",
        ),
        "delete_required_fields": ("product_id", "version_no"),
        "soft_delete": True,
        "field_types": {
            "product_id": "int64", "version_no": "int32", "product_name": "string",
            "category_id": "int64", "category_name": "string", "price": "decimal:10:2",
            "effective_from": "datetime", "effective_to": "datetime",
            "is_current": "int8", "updated_at": "datetime",
        },
    },
    "dwd_order_detail": {
        "table": "dwd_order_detail",
        "datetime_fields": (
            "payment_time", "refund_time", "order_create_time",
            "order_update_time", "detail_update_time",
        ),
        "required_fields": ("detail_id",),
        "delete_required_fields": ("detail_id",),
        "soft_delete": True,
        "field_types": {
            "detail_id": "string", "order_id": "string?", "user_id": "int64?",
            "shop_id": "int64?", "product_id": "int64?", "product_name": "string?",
            "category_id": "int64?", "category_name": "string?", "product_version": "int32?",
            "catalog_price": "decimal:10:2?", "quantity": "int32?",
            "unit_price": "decimal:10:2?", "detail_amount": "decimal:12:2?",
            "order_status": "string?", "order_amount": "decimal:12:2?",
            "channel": "string?", "promotion_code": "string?", "payment_id": "string?",
            "payment_status": "string?", "payment_method": "string?",
            "payment_amount": "decimal:12:2?", "payment_time": "datetime?",
            "refund_id": "string?", "refund_status": "string?",
            "refund_amount": "decimal:12:2?", "refund_reason": "string?",
            "refund_time": "datetime?", "order_create_time": "datetime?",
            "order_update_time": "datetime?", "detail_update_time": "datetime?",
            "order_version": "int64?", "detail_version": "int64?",
            "payment_version": "int64?", "refund_version": "int64?",
        },
    },
    "ads_order_lifecycle": {
        "table": "ads_order_lifecycle",
        "required_fields": ("channel",),
        "delete_required_fields": ("channel",),
        "soft_delete": True,
        "field_types": {
            "channel": "string", "total_orders": "int64?", "paid_orders": "int64?",
            "cancelled_orders": "int64?", "refunded_orders": "int64?",
            "order_amount": "decimal:22:2?", "paid_amount": "decimal:22:2?",
            "refund_amount": "decimal:22:2?",
        },
    },
    "ads_order_daily": {
        "table": "ads_order_daily",
        "required_fields": ("order_date", "channel"),
        "delete_required_fields": ("order_date", "channel"),
        "soft_delete": True,
        "field_types": {
            "order_date": "date", "channel": "string", "total_orders": "int64?",
            "paid_orders": "int64?", "cancelled_orders": "int64?",
            "refunded_orders": "int64?", "order_amount": "decimal:22:2?",
            "paid_amount": "decimal:22:2?", "refund_amount": "decimal:22:2?",
        },
    },
}

MAX_INSERT_RETRIES = 5
VERSION_EPOCH_BITS = 8
VERSION_OFFSET_BITS = 44
VERSION_PARTITION_BITS = 12
DEFAULT_VERSION_EPOCH = 128
MAX_VERSION_EPOCH = (1 << VERSION_EPOCH_BITS) - 1
MAX_VERSION_PARTITION = (1 << VERSION_PARTITION_BITS) - 1
MAX_VERSION_OFFSET = (1 << VERSION_OFFSET_BITS) - 1
DEFAULT_DLQ_TOPIC = "clickhouse_loader_dlq"
DLQ_SCHEMA_VERSION = 1
DLQ_PUBLISH_TIMEOUT_SECONDS = 10.0


class LoaderMetrics:
    """Thread-safe, dependency-free Prometheus metrics for the loader."""

    def __init__(self):
        self._lock = threading.Lock()
        self.start_unixtime = time.time()
        self.messages_consumed = 0
        self.rows_inserted = 0
        self.insert_failures = 0
        self.decode_failures = 0
        self.dlq_publish_successes = 0
        self.dlq_publish_failures = 0
        self.successful_batches = 0
        self.last_success_unixtime = 0.0
        self.last_batch_duration = 0.0

    def record_consumed(self):
        with self._lock:
            self.messages_consumed += 1

    def record_insert_failure(self):
        with self._lock:
            self.insert_failures += 1

    def record_decode_failure(self):
        with self._lock:
            self.decode_failures += 1

    def record_dlq_publish_success(self):
        with self._lock:
            self.dlq_publish_successes += 1

    def record_dlq_publish_failure(self):
        with self._lock:
            self.dlq_publish_failures += 1

    def record_insert_success(self, rows, duration_seconds):
        with self._lock:
            self.rows_inserted += rows
            self.successful_batches += 1
            self.last_success_unixtime = time.time()
            self.last_batch_duration = duration_seconds

    def render(self):
        with self._lock:
            values = {
                "messages": self.messages_consumed,
                "rows": self.rows_inserted,
                "failures": self.insert_failures,
                "decode_failures": self.decode_failures,
                "dlq_successes": self.dlq_publish_successes,
                "dlq_failures": self.dlq_publish_failures,
                "batches": self.successful_batches,
                "last_success": self.last_success_unixtime,
                "duration": self.last_batch_duration,
                "start": self.start_unixtime,
            }
        return "\n".join(
            (
                "# HELP clickhouse_loader_messages_consumed_total Kafka messages consumed by the loader.",
                "# TYPE clickhouse_loader_messages_consumed_total counter",
                f"clickhouse_loader_messages_consumed_total {values['messages']}",
                "# HELP clickhouse_loader_rows_inserted_total Rows successfully written to ClickHouse.",
                "# TYPE clickhouse_loader_rows_inserted_total counter",
                f"clickhouse_loader_rows_inserted_total {values['rows']}",
                "# HELP clickhouse_loader_insert_failures_total Failed ClickHouse HTTP insert attempts.",
                "# TYPE clickhouse_loader_insert_failures_total counter",
                f"clickhouse_loader_insert_failures_total {values['failures']}",
                "# HELP clickhouse_loader_decode_failures_total Kafka records rejected during decoding, validation, or enrichment.",
                "# TYPE clickhouse_loader_decode_failures_total counter",
                f"clickhouse_loader_decode_failures_total {values['decode_failures']}",
                "# HELP clickhouse_loader_dlq_publish_success_total Rejected records acknowledged by the Kafka DLQ.",
                "# TYPE clickhouse_loader_dlq_publish_success_total counter",
                f"clickhouse_loader_dlq_publish_success_total {values['dlq_successes']}",
                "# HELP clickhouse_loader_dlq_publish_failures_total Failed Kafka DLQ publish attempts.",
                "# TYPE clickhouse_loader_dlq_publish_failures_total counter",
                f"clickhouse_loader_dlq_publish_failures_total {values['dlq_failures']}",
                "# HELP clickhouse_loader_successful_batches_total Successful ClickHouse insert batches.",
                "# TYPE clickhouse_loader_successful_batches_total counter",
                f"clickhouse_loader_successful_batches_total {values['batches']}",
                "# HELP clickhouse_loader_last_success_unixtime_seconds Unix time of the last successful ClickHouse insert.",
                "# TYPE clickhouse_loader_last_success_unixtime_seconds gauge",
                f"clickhouse_loader_last_success_unixtime_seconds {values['last_success']:.6f}",
                "# HELP clickhouse_loader_last_batch_duration_seconds Duration of the last successful ClickHouse insert batch.",
                "# TYPE clickhouse_loader_last_batch_duration_seconds gauge",
                f"clickhouse_loader_last_batch_duration_seconds {values['duration']:.6f}",
                "# HELP clickhouse_loader_start_unixtime_seconds Unix time when this loader process started.",
                "# TYPE clickhouse_loader_start_unixtime_seconds gauge",
                f"clickhouse_loader_start_unixtime_seconds {values['start']:.6f}",
                "",
            )
        )


def start_metrics_server(metrics, host, port):
    if port == 0:
        return None

    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/metrics":
                payload = metrics.render().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            if self.path == "/healthz":
                payload = b"ok\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            self.send_error(404)

        def log_message(self, _format, *_args):
            return

    server = ThreadingHTTPServer((host, port), MetricsHandler)
    thread = threading.Thread(
        target=server.serve_forever,
        name="loader-metrics",
        daemon=True,
    )
    thread.start()
    print(f"loader metrics listening on http://{host}:{port}/metrics")
    return server


def normalize_datetime(value):
    if value is None:
        return None
    text = str(value).replace("T", " ")
    if "." in text:
        text = text.split(".", 1)[0]
    return text


def normalize_record(config, record):
    if not isinstance(record, dict):
        raise TypeError("Kafka JSON value must be an object")
    for field in config.get("datetime_fields", ()):
        record[field] = normalize_datetime(record.get(field))
    return record


def validate_required_fields(record, required_fields):
    missing = [field for field in required_fields if record.get(field) is None]
    if missing:
        raise ValueError(f"missing required field(s): {', '.join(missing)}")
    return record


INTEGER_RANGES = {
    "int8": (-(1 << 7), (1 << 7) - 1),
    "int32": (-(1 << 31), (1 << 31) - 1),
    "int64": (-(1 << 63), (1 << 63) - 1),
    "uint64": (0, (1 << 64) - 1),
}
INTEGER_PATTERN = re.compile(r"^[+-]?\d+$")


def _validate_field_value(field, value, type_spec):
    nullable = type_spec.endswith("?")
    base_type = type_spec[:-1] if nullable else type_spec
    if value is None:
        if nullable:
            return
        raise TypeError(f"field {field} must be {base_type}, got null")

    if base_type == "string":
        if not isinstance(value, str):
            raise TypeError(f"field {field} must be string, got {type(value).__name__}")
        return

    if base_type in INTEGER_RANGES:
        if isinstance(value, bool):
            raise TypeError(f"field {field} must be {base_type}, got bool")
        if isinstance(value, int):
            number = value
        elif isinstance(value, str) and INTEGER_PATTERN.fullmatch(value):
            number = int(value)
        else:
            raise TypeError(f"field {field} must be {base_type}, got {value!r}")
        lower, upper = INTEGER_RANGES[base_type]
        if not lower <= number <= upper:
            raise ValueError(f"field {field} is outside ClickHouse {base_type} range")
        return

    if base_type.startswith("decimal:"):
        _name, precision_text, scale_text = base_type.split(":")
        precision, scale = int(precision_text), int(scale_text)
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise TypeError(f"field {field} must be Decimal({precision},{scale}), got {value!r}")
        try:
            number = Decimal(str(value))
        except InvalidOperation as error:
            raise TypeError(
                f"field {field} must be Decimal({precision},{scale}), got {value!r}"
            ) from error
        if not number.is_finite():
            raise ValueError(f"field {field} must be a finite decimal")
        quantum = Decimal(1).scaleb(-scale)
        try:
            has_excess_scale = number != number.quantize(quantum)
        except InvalidOperation as error:
            raise ValueError(
                f"field {field} exceeds Decimal({precision},{scale}) precision"
            ) from error
        if has_excess_scale:
            raise ValueError(f"field {field} exceeds Decimal({precision},{scale}) scale")
        if abs(number) >= Decimal(10) ** (precision - scale):
            raise ValueError(f"field {field} exceeds Decimal({precision},{scale}) precision")
        return

    if base_type == "datetime":
        if not isinstance(value, str):
            raise TypeError(f"field {field} must be a timestamp string")
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"field {field} is not a valid ISO timestamp: {value!r}") from error
        return

    if base_type == "date":
        if not isinstance(value, str):
            raise TypeError(f"field {field} must be an ISO date string")
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as error:
            raise ValueError(f"field {field} is not a valid YYYY-MM-DD date: {value!r}") from error
        return

    raise ValueError(f"unsupported loader type specification for {field}: {type_spec}")


def validate_record_types(record, field_types):
    """Reject rows that ClickHouse would reject before they poison a batch."""
    unknown = sorted(set(record) - set(field_types))
    if unknown:
        raise ValueError(f"unknown field(s) for ClickHouse mapping: {', '.join(unknown)}")
    for field, value in record.items():
        _validate_field_value(field, value, field_types[field])
    return record


def decode_message(config, msg):
    """Decode an upsert value or turn a Kafka tombstone into a soft delete."""
    if msg.value() is not None:
        record = json.loads(msg.value().decode("utf-8"))
        record = normalize_record(config, record)
        validate_required_fields(record, config.get("required_fields", ()))
        validate_record_types(record, config.get("field_types", {}))
        if config.get("soft_delete"):
            record["is_deleted"] = 0
        return record

    if not config.get("soft_delete"):
        return None
    if msg.key() is None:
        raise ValueError(f"tombstone without key in topic {msg.topic()}")
    record = json.loads(msg.key().decode("utf-8"))
    if not isinstance(record, dict):
        raise TypeError("Kafka tombstone key must be a JSON object")
    validate_required_fields(record, config.get("delete_required_fields", ()))
    delete_field_types = {
        field: config["field_types"][field]
        for field in config.get("delete_required_fields", ())
    }
    validate_record_types(record, delete_field_types)
    record["is_deleted"] = 1
    return record


def _message_bytes(value):
    """Return Kafka key/value data as bytes without silently changing content."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    raise TypeError(f"Kafka key/value must be bytes or None, got {type(value).__name__}")


def dlq_message_id(msg):
    """Build a deterministic identity for one physical Kafka record."""
    key = _message_bytes(msg.key())
    value = _message_bytes(msg.value())
    digest = hashlib.sha256()
    for part in (
        msg.topic().encode("utf-8"),
        str(msg.partition()).encode("ascii"),
        str(msg.offset()).encode("ascii"),
        key,
        value,
    ):
        if part is None:
            digest.update(b"\xff")
        else:
            digest.update(len(part).to_bytes(8, "big"))
            digest.update(part)
    return digest.hexdigest()


def build_dlq_envelope(msg, error, failed_at=None):
    """Preserve an undecodable record losslessly in a versioned DLQ envelope."""
    key = _message_bytes(msg.key())
    value = _message_bytes(msg.value())
    if failed_at is None:
        failed_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        )
    return {
        "schema_version": DLQ_SCHEMA_VERSION,
        "message_id": dlq_message_id(msg),
        "status": "pending",
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "key_base64": None if key is None else base64.b64encode(key).decode("ascii"),
        "value_base64": None if value is None else base64.b64encode(value).decode("ascii"),
        "error_type": type(error).__name__,
        "error_message": str(error)[:2048],
        "failed_at": failed_at,
    }


def publish_record(producer, topic, *, key, value, timeout=DLQ_PUBLISH_TIMEOUT_SECONDS):
    """Synchronously publish one Kafka record and require broker acknowledgement."""
    delivery = {"called": False, "error": None}

    def on_delivery(error, _message):
        delivery["called"] = True
        delivery["error"] = error

    producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
    remaining = producer.flush(timeout)
    if remaining:
        raise TimeoutError(
            f"Kafka publish to {topic} timed out with {remaining} message(s) pending"
        )
    if not delivery["called"]:
        raise RuntimeError(f"Kafka publish to {topic} returned without delivery acknowledgement")
    if delivery["error"] is not None:
        raise RuntimeError(f"Kafka publish to {topic} failed: {delivery['error']}")


def publish_to_dlq(producer, dlq_topic, envelope, metrics=None):
    """Publish an envelope using its deterministic id as the compaction key."""
    try:
        publish_record(
            producer,
            dlq_topic,
            key=envelope["message_id"].encode("ascii"),
            value=json.dumps(envelope, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        )
    except Exception:
        if metrics is not None:
            metrics.record_dlq_publish_failure()
        raise
    if metrics is not None:
        metrics.record_dlq_publish_success()


def mark_handled_offset(pending_offsets, msg):
    """Record the next offset eligible for an explicit commit after durable handling."""
    key = (msg.topic(), msg.partition())
    next_offset = msg.offset() + 1
    pending_offsets[key] = max(next_offset, pending_offsets.get(key, next_offset))


def commit_handled_offsets(consumer, pending_offsets):
    """Commit only offsets known to be in ClickHouse or acknowledged by the DLQ."""
    if not pending_offsets:
        return
    offsets = [
        TopicPartition(topic, partition, next_offset)
        for (topic, partition), next_offset in sorted(pending_offsets.items())
    ]
    consumer.commit(offsets=offsets, asynchronous=False)
    pending_offsets.clear()


def quarantine_message(producer, dlq_topic, msg, error, pending_offsets, metrics=None):
    """Quarantine one poison record and only then make its offset commit-eligible."""
    if metrics is not None:
        metrics.record_decode_failure()
    envelope = build_dlq_envelope(msg, error)
    publish_to_dlq(producer, dlq_topic, envelope, metrics)
    mark_handled_offset(pending_offsets, msg)
    return envelope


def kafka_ingest_version(partition, offset, version_epoch=DEFAULT_VERSION_EPOCH):
    """Build a replay-stable UInt64 version from a Kafka record position.

    Upsert Kafka keeps a business key on one partition, so offset ordering is
    the authoritative order for all versions of that key. The high-bit epoch
    also makes this scheme supersede rows written by the legacy time_ns scheme.
    """
    if not 0 <= version_epoch <= MAX_VERSION_EPOCH:
        raise ValueError(f"Loader version epoch out of supported range: {version_epoch}")
    if not 0 <= partition <= MAX_VERSION_PARTITION:
        raise ValueError(f"Kafka partition out of supported range: {partition}")
    if not 0 <= offset <= MAX_VERSION_OFFSET:
        raise ValueError(f"Kafka offset out of supported range: {offset}")
    return (
        (version_epoch << (VERSION_OFFSET_BITS + VERSION_PARTITION_BITS))
        | (offset << VERSION_PARTITION_BITS)
        | partition
    )


def insert_json_each_rows(clickhouse_url, user, password, table, records, metrics=None):
    """Insert one batch with a single ClickHouse HTTP request."""
    if not records:
        return
    query = urllib.parse.urlencode({"query": f"INSERT INTO {table} FORMAT JSONEachRow"})
    payload = "\n".join(json.dumps(record, ensure_ascii=False) for record in records)
    data = (payload + "\n").encode("utf-8")
    auth = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    request = urllib.request.Request(
        f"{clickhouse_url}/?{query}",
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {auth}",
        },
    )
    for attempt in range(1, MAX_INSERT_RETRIES + 1):
        started_at = time.perf_counter()
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                response.read()
            if metrics is not None:
                metrics.record_insert_success(len(records), time.perf_counter() - started_at)
            return
        except Exception:
            if metrics is not None:
                metrics.record_insert_failure()
            if attempt == MAX_INSERT_RETRIES:
                raise
            delay = min(2 ** (attempt - 1), 15)
            print(f"insert retry {attempt}/{MAX_INSERT_RETRIES} for {table} in {delay}s")
            time.sleep(delay)


def parse_args():
    parser = argparse.ArgumentParser(description="Load Flink Kafka result topics into ClickHouse.")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--clickhouse-url",
        default=os.getenv("CLICKHOUSE_URL", "http://localhost:8123"),
    )
    parser.add_argument("--clickhouse-user", default=os.getenv("CLICKHOUSE_USER", "default"))
    parser.add_argument(
        "--clickhouse-password",
        default=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
    )
    parser.add_argument(
        "--group-id",
        default=os.getenv("CLICKHOUSE_LOADER_GROUP_ID", "clickhouse_loader"),
    )
    parser.add_argument(
        "--dlq-topic",
        default=os.getenv("CLICKHOUSE_LOADER_DLQ_TOPIC", DEFAULT_DLQ_TOPIC),
        help="Kafka topic used for broker-acknowledged poison-message quarantine.",
    )
    parser.add_argument(
        "--auto-offset-reset",
        choices=("earliest", "latest"),
        default=os.getenv("CLICKHOUSE_LOADER_AUTO_OFFSET_RESET", "earliest"),
        help="Use latest for a live benchmark consumer; normal recovery uses earliest.",
    )
    parser.add_argument("--max-messages", type=int, default=0, help="0 means keep running.")
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=int(os.getenv("CLICKHOUSE_LOADER_IDLE_TIMEOUT", "30")),
        help="Exit after this many idle seconds; 0 keeps the service running.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("CLICKHOUSE_LOADER_BATCH_SIZE", "500")),
    )
    parser.add_argument(
        "--flush-interval",
        type=float,
        default=float(os.getenv("CLICKHOUSE_LOADER_FLUSH_INTERVAL", "2.0")),
    )
    parser.add_argument(
        "--version-epoch",
        type=int,
        default=int(
            os.getenv("CLICKHOUSE_LOADER_VERSION_EPOCH", str(DEFAULT_VERSION_EPOCH))
        ),
        help="Increment only when Kafka offsets are reset while ClickHouse data is retained.",
    )
    parser.add_argument(
        "--metrics-host",
        default=os.getenv("CLICKHOUSE_LOADER_METRICS_HOST", "127.0.0.1"),
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=int(os.getenv("CLICKHOUSE_LOADER_METRICS_PORT", "0")),
        help="Prometheus HTTP port; 0 disables metrics for one-off batch runs.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.max_messages < 0:
        raise ValueError("--max-messages must be non-negative")
    if args.idle_timeout < 0:
        raise ValueError("--idle-timeout must be non-negative")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")
    if args.flush_interval <= 0:
        raise ValueError("--flush-interval must be positive")
    if not 0 <= args.version_epoch <= MAX_VERSION_EPOCH:
        raise ValueError(f"--version-epoch must be between 0 and {MAX_VERSION_EPOCH}")
    if args.auto_offset_reset not in ("earliest", "latest"):
        raise ValueError("--auto-offset-reset must be earliest or latest")
    if not 0 <= args.metrics_port <= 65535:
        raise ValueError("--metrics-port must be between 0 and 65535")
    if not args.dlq_topic.strip():
        raise ValueError("--dlq-topic must not be empty")
    if args.dlq_topic in TOPIC_CONFIG:
        raise ValueError("--dlq-topic must not be one of the loader source topics")

    stop_requested = False
    metrics = LoaderMetrics()
    metrics_server = start_metrics_server(metrics, args.metrics_host, args.metrics_port)

    def request_stop(signum, _frame):
        nonlocal stop_requested
        print(f"received signal {signum}; flushing buffered rows before shutdown")
        stop_requested = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": args.auto_offset_reset,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    consumer.subscribe(list(TOPIC_CONFIG.keys()))
    dlq_producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "clickhouse-loader-dlq",
        "enable.idempotence": True,
        "acks": "all",
    })

    buffers = defaultdict(list)
    pending_offsets = {}
    consumed = 0
    inserted = 0
    pending_messages = 0
    last_flush = time.monotonic()

    def flush_all():
        nonlocal inserted, pending_messages, last_flush
        batch_total = 0
        for table, records in list(buffers.items()):
            if not records:
                continue
            insert_json_each_rows(
                args.clickhouse_url,
                args.clickhouse_user,
                args.clickhouse_password,
                table,
                records,
                metrics,
            )
            batch_total += len(records)
            records.clear()
        if pending_messages:
            commit_handled_offsets(consumer, pending_offsets)
            inserted += batch_total
            print(
                f"flushed {batch_total} rows from {pending_messages} messages, "
                f"total inserted: {inserted}"
            )
            pending_messages = 0
        last_flush = time.monotonic()

    print("loading Kafka result topics into ClickHouse...")
    try:
        while not stop_requested:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if pending_messages and time.monotonic() - last_flush >= args.flush_interval:
                    flush_all()
                if args.idle_timeout > 0 and time.monotonic() - last_flush >= args.idle_timeout:
                    break
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            topic = msg.topic()
            config = TOPIC_CONFIG.get(topic)
            if not config:
                continue

            consumed += 1
            metrics.record_consumed()
            try:
                record = decode_message(config, msg)
                if record is not None:
                    record.update(
                        {
                            "source_topic": topic,
                            "source_partition": msg.partition(),
                            "source_offset": msg.offset(),
                            "ingest_version": kafka_ingest_version(
                                msg.partition(), msg.offset(), args.version_epoch
                            ),
                        }
                    )
            except Exception as error:
                envelope = quarantine_message(
                    dlq_producer,
                    args.dlq_topic,
                    msg,
                    error,
                    pending_offsets,
                    metrics,
                )
                pending_messages += 1
                print(
                    f"quarantined {topic}[{msg.partition()}]@{msg.offset()} "
                    f"as {envelope['message_id']}"
                )
                if (
                    pending_messages >= args.batch_size
                    or time.monotonic() - last_flush >= args.flush_interval
                ):
                    flush_all()
                if args.max_messages > 0 and consumed >= args.max_messages:
                    break
                continue

            if record is None:
                mark_handled_offset(pending_offsets, msg)
                pending_messages += 1
                if (
                    pending_messages >= args.batch_size
                    or time.monotonic() - last_flush >= args.flush_interval
                ):
                    flush_all()
                if args.max_messages > 0 and consumed >= args.max_messages:
                    break
                continue
            table = config["table"]
            buffers[table].append(record)
            mark_handled_offset(pending_offsets, msg)
            pending_messages += 1

            if (
                len(buffers[table]) >= args.batch_size
                or time.monotonic() - last_flush >= args.flush_interval
            ):
                flush_all()

            if args.max_messages > 0 and consumed >= args.max_messages:
                break
    finally:
        flush_all()
        consumer.close()
        if metrics_server is not None:
            metrics_server.shutdown()
            metrics_server.server_close()
    print(f"done, consumed {consumed} messages and inserted {inserted} rows.")


if __name__ == "__main__":
    main()
