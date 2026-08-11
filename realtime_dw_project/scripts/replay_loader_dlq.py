#!/usr/bin/env python3
"""Inspect, repair, and replay ClickHouse loader DLQ records safely.

The command is read-only by default. A write requires one exact message id,
one explicit repair mode, and --execute. Payloads are redacted unless the
operator asks to display them.
"""

import argparse
import base64
import binascii
import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition

from load_kafka_to_clickhouse import (
    DEFAULT_DLQ_TOPIC,
    DLQ_SCHEMA_VERSION,
    TOPIC_CONFIG,
    decode_message,
    publish_record,
    publish_to_dlq,
)


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def decode_base64_field(envelope, field):
    encoded = envelope.get(field)
    if encoded is None:
        return None
    if not isinstance(encoded, str):
        raise ValueError(f"DLQ {field} must be a base64 string or null")
    try:
        return base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as error:
        raise ValueError(f"DLQ {field} is not valid base64") from error


def validate_envelope(envelope):
    if not isinstance(envelope, dict):
        raise ValueError("DLQ value must be a JSON object")
    required = (
        "schema_version",
        "message_id",
        "source_topic",
        "source_partition",
        "source_offset",
        "key_base64",
        "value_base64",
        "error_type",
        "error_message",
        "failed_at",
    )
    missing = [field for field in required if field not in envelope]
    if missing:
        raise ValueError(f"DLQ envelope missing field(s): {', '.join(missing)}")
    if (
        type(envelope["schema_version"]) is not int
        or envelope["schema_version"] != DLQ_SCHEMA_VERSION
    ):
        raise ValueError(
            f"unsupported DLQ schema_version {envelope['schema_version']!r}; "
            f"expected {DLQ_SCHEMA_VERSION}"
        )
    message_id = envelope["message_id"]
    if not isinstance(message_id, str) or len(message_id) != 64:
        raise ValueError("DLQ message_id must be a 64-character SHA-256 hex string")
    try:
        int(message_id, 16)
    except ValueError as error:
        raise ValueError("DLQ message_id must be hexadecimal") from error
    if envelope["source_topic"] not in TOPIC_CONFIG:
        raise ValueError(f"refusing unknown source topic: {envelope['source_topic']}")
    decode_base64_field(envelope, "key_base64")
    decode_base64_field(envelope, "value_base64")
    return envelope


def apply_json_patch(raw_value, patch):
    if raw_value is None:
        raise ValueError("cannot JSON-patch a tombstone; use --replacement-value-file")
    try:
        value = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(
            "original value is not a UTF-8 JSON object; use --replacement-value-file"
        ) from error
    if not isinstance(value, dict):
        raise ValueError("JSON patch requires the original value to be an object")
    if not isinstance(patch, dict):
        raise ValueError("--patch-json must decode to a JSON object")
    value.update(patch)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class ReplayCandidateMessage:
    """Minimal Kafka message adapter used to reuse Loader validation rules."""

    def __init__(self, topic, key, value):
        self._topic = topic
        self._key = key
        self._value = value

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def value(self):
        return self._value


def validate_replay_candidate(topic, key, value):
    config = TOPIC_CONFIG.get(topic)
    if config is None:
        raise ValueError(f"refusing unknown replay target topic: {topic}")
    try:
        decode_message(config, ReplayCandidateMessage(topic, key, value))
    except Exception as error:
        raise ValueError(
            f"replay candidate still fails Loader validation for {topic}: {error}"
        ) from error


def prepare_replay_record(
    envelope,
    *,
    patch=None,
    replacement_value=None,
    replay_original=False,
    target_topic=None,
    allow_topic_override=False,
):
    validate_envelope(envelope)
    modes = int(patch is not None) + int(replacement_value is not None) + int(replay_original)
    if modes != 1:
        raise ValueError(
            "choose exactly one repair mode: --patch-json, "
            "--replacement-value-file, or --replay-original"
        )

    source_topic = envelope["source_topic"]
    topic = target_topic or source_topic
    if topic != source_topic and not allow_topic_override:
        raise ValueError("topic override requires --allow-topic-override")
    if topic == DEFAULT_DLQ_TOPIC:
        raise ValueError("refusing to replay a source record into the DLQ topic")

    key = decode_base64_field(envelope, "key_base64")
    original_value = decode_base64_field(envelope, "value_base64")
    if patch is not None:
        value = apply_json_patch(original_value, patch)
    elif replacement_value is not None:
        value = replacement_value
    else:
        value = original_value
    validate_replay_candidate(topic, key, value)
    return topic, key, value


def safe_summary(envelope, show_payload=False):
    validate_envelope(envelope)
    key = decode_base64_field(envelope, "key_base64")
    value = decode_base64_field(envelope, "value_base64")
    summary = {
        "message_id": envelope["message_id"],
        "status": envelope.get("status", "pending"),
        "source": (
            f"{envelope['source_topic']}[{envelope['source_partition']}]"
            f"@{envelope['source_offset']}"
        ),
        "error": f"{envelope['error_type']}: {envelope['error_message']}",
        "failed_at": envelope["failed_at"],
        "key_bytes": 0 if key is None else len(key),
        "value_bytes": 0 if value is None else len(value),
        "value_sha256": None if value is None else hashlib.sha256(value).hexdigest(),
    }
    if show_payload:
        summary["key_utf8"] = None if key is None else key.decode("utf-8", errors="replace")
        summary["value_utf8"] = (
            None if value is None else value.decode("utf-8", errors="replace")
        )
    return summary


def mark_replayed(producer, dlq_topic, envelope, replay_topic, replay_value):
    resolved = dict(envelope)
    resolved.update(
        {
            "status": "replayed",
            "replayed_at": utc_now(),
            "replay_topic": replay_topic,
            "replay_value_sha256": (
                None if replay_value is None else hashlib.sha256(replay_value).hexdigest()
            ),
        }
    )
    publish_to_dlq(producer, dlq_topic, resolved)
    return resolved


@dataclass(frozen=True)
class ScanResult:
    envelopes: dict
    scanned: int
    complete: bool
    truncated: bool


def collect_envelopes(consumer, dlq_topic, idle_timeout, scan_limit=None):
    """Scan a stable per-partition [low, high) snapshot of the compacted DLQ.

    ``scan_limit=None`` means no record-count truncation and is mandatory for
    execute mode. Completion is based on reaching every partition's captured
    high watermark, not on an idle poll, so an older pending envelope can never
    be mistaken for the latest state merely because scanning stopped early.
    """
    metadata = consumer.list_topics(dlq_topic, timeout=max(10.0, idle_timeout))
    topic_metadata = metadata.topics.get(dlq_topic)
    if topic_metadata is None or topic_metadata.error is not None:
        detail = "topic not found" if topic_metadata is None else str(topic_metadata.error)
        raise RuntimeError(f"cannot inspect DLQ topic {dlq_topic}: {detail}")

    starts = []
    snapshot_watermarks = {}
    for partition in sorted(topic_metadata.partitions):
        position = TopicPartition(dlq_topic, partition)
        low, high = consumer.get_watermark_offsets(position, timeout=10.0)
        starts.append(TopicPartition(dlq_topic, partition, low))
        snapshot_watermarks[(dlq_topic, partition)] = (low, high)
    consumer.assign(starts)

    latest_by_id = {}
    scanned = 0
    deadline = time.monotonic() + idle_timeout

    def reached_snapshot_end():
        positions = consumer.position(
            [TopicPartition(topic, partition) for topic, partition in snapshot_watermarks]
        )
        for item in positions:
            low, high = snapshot_watermarks[(item.topic, item.partition)]
            # librdkafka may report OFFSET_INVALID for an assigned partition
            # that has nothing to consume. Its captured [low, high) interval is
            # already empty, so position must not block snapshot completion.
            if low >= high:
                continue
            if item.offset < high:
                return False
        return True

    complete = reached_snapshot_end()
    truncated = False
    while not complete and time.monotonic() < deadline:
        if scan_limit is not None and scanned >= scan_limit:
            truncated = True
            break
        msg = consumer.poll(timeout=min(0.5, max(0.0, deadline - time.monotonic())))
        if msg is None:
            complete = reached_snapshot_end()
            continue
        if msg.error():
            continue
        deadline = time.monotonic() + idle_timeout
        scanned += 1
        if msg.value() is None:
            continue
        try:
            envelope = json.loads(msg.value().decode("utf-8"))
            validate_envelope(envelope)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            print(f"skipping malformed DLQ envelope at offset {msg.offset()}: {error}")
            complete = reached_snapshot_end()
            continue
        latest_by_id[envelope["message_id"]] = envelope
        complete = reached_snapshot_end()
    return ScanResult(latest_by_id, scanned, complete, truncated)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--dlq-topic",
        default=os.getenv("CLICKHOUSE_LOADER_DLQ_TOPIC", DEFAULT_DLQ_TOPIC),
    )
    parser.add_argument("--message-id", help="Select exactly one deterministic DLQ id.")
    parser.add_argument("--limit", type=int, default=20, help="Maximum records to display.")
    parser.add_argument(
        "--scan-limit",
        type=int,
        default=10_000,
        help="Maximum broker records in dry-run; --execute always scans the full snapshot.",
    )
    parser.add_argument("--idle-timeout", type=float, default=2.0)
    parser.add_argument("--include-resolved", action="store_true")
    parser.add_argument(
        "--show-payload",
        action="store_true",
        help="Display decoded key/value; off by default to reduce accidental PII exposure.",
    )
    parser.add_argument("--patch-json", help="Shallow JSON object merged into the value.")
    parser.add_argument("--replacement-value-file", type=Path)
    parser.add_argument("--replay-original", action="store_true")
    parser.add_argument("--target-topic")
    parser.add_argument("--allow-topic-override", action="store_true")
    parser.add_argument("--keep-pending", action="store_true")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually replay; without this flag the command is always read-only.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.limit <= 0 or args.scan_limit <= 0 or args.idle_timeout <= 0:
        raise ValueError("--limit, --scan-limit, and --idle-timeout must be positive")
    if args.dlq_topic in TOPIC_CONFIG:
        raise ValueError("--dlq-topic must not be a loader source topic")
    repair_requested = any(
        (
            args.patch_json is not None,
            args.replacement_value_file is not None,
            args.replay_original,
            args.target_topic is not None,
        )
    )
    if args.execute and not args.message_id:
        raise ValueError("--execute requires one exact --message-id")
    if args.execute and not repair_requested:
        raise ValueError(
            "--execute requires exactly one repair mode: --patch-json, "
            "--replacement-value-file, or --replay-original"
        )
    if repair_requested and not args.message_id:
        raise ValueError("a repair dry-run also requires one exact --message-id")

    patch = None
    if args.patch_json is not None:
        patch = json.loads(args.patch_json)
        if not isinstance(patch, dict):
            raise ValueError("--patch-json must decode to a JSON object")
    replacement = (
        None
        if args.replacement_value_file is None
        else args.replacement_value_file.read_bytes()
    )

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": f"clickhouse-loader-dlq-inspect-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    try:
        scan = collect_envelopes(
            consumer,
            args.dlq_topic,
            args.idle_timeout,
            None if args.execute else args.scan_limit,
        )
    finally:
        consumer.close()

    if scan.truncated:
        print(
            f"WARNING: dry-run scan was TRUNCATED at --scan-limit={args.scan_limit}; "
            "displayed states may not be the latest compacted envelopes"
        )
    elif not scan.complete:
        print(
            "WARNING: scan stopped before every partition reached its captured high "
            "watermark; displayed states may not be latest"
        )
    if args.execute and not scan.complete:
        raise RuntimeError(
            "refusing --execute because the DLQ snapshot scan did not reach every "
            "partition high watermark"
        )

    selected = []
    for message_id in sorted(scan.envelopes):
        envelope = scan.envelopes[message_id]
        if args.message_id and message_id != args.message_id:
            continue
        if not args.include_resolved and envelope.get("status", "pending") != "pending":
            continue
        selected.append(envelope)

    print(
        f"scanned {scan.scanned} DLQ records; selected {len(selected)} latest envelopes; "
        f"snapshot_complete={str(scan.complete).lower()}"
    )
    for envelope in selected[: args.limit]:
        print(json.dumps(safe_summary(envelope, args.show_payload), ensure_ascii=False))

    if (args.execute or repair_requested) and len(selected) != 1:
        raise ValueError(
            f"repair expected one pending envelope for {args.message_id}, found {len(selected)}"
        )

    if repair_requested:
        envelope = selected[0]
        topic, key, value = prepare_replay_record(
            envelope,
            patch=patch,
            replacement_value=replacement,
            replay_original=args.replay_original,
            target_topic=args.target_topic,
            allow_topic_override=args.allow_topic_override,
        )
    if not args.execute:
        if repair_requested:
            print(
                "DRY RUN: repair is valid; would replay "
                f"{envelope['message_id']} to {topic} "
                f"(key_bytes={0 if key is None else len(key)}, "
                f"value_bytes={0 if value is None else len(value)})"
            )
        print("DRY RUN: no Kafka records were published")
        return

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": "clickhouse-loader-dlq-replay",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    publish_record(producer, topic, key=key, value=value)
    if not args.keep_pending:
        mark_replayed(producer, args.dlq_topic, envelope, topic, value)
    print(f"replayed {envelope['message_id']} to {topic} with broker acknowledgement")


if __name__ == "__main__":
    main()
