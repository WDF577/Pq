import importlib.util
import base64
import json
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "load_kafka_to_clickhouse.py"
SPEC = importlib.util.spec_from_file_location("clickhouse_loader", MODULE_PATH)
loader = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(loader)


class FakeMessage:
    def __init__(self, *, topic, key, value, partition=2, offset=41):
        self._topic = topic
        self._key = key
        self._value = value
        self._partition = partition
        self._offset = offset

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class FakeProducer:
    def __init__(self, delivery_error=None, remaining=0):
        self.delivery_error = delivery_error
        self.remaining = remaining
        self.records = []
        self.callback = None

    def produce(self, topic, *, key, value, on_delivery):
        self.records.append({"topic": topic, "key": key, "value": value})
        self.callback = on_delivery

    def flush(self, _timeout):
        if self.remaining == 0 and self.callback is not None:
            self.callback(self.delivery_error, None)
        return self.remaining


class FakeConsumer:
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.commits = []

    def commit(self, *, offsets, asynchronous):
        if self.should_fail:
            raise RuntimeError("commit failed")
        self.commits.append((offsets, asynchronous))


def valid_behavior_record(**overrides):
    record = {
        "event_id": "E-1",
        "session_id": "S-1",
        "event_sequence": 1,
        "event_version": 1,
        "user_id": 101,
        "product_id": 201,
        "shop_id": 301,
        "event_type": "pay",
        "channel": "app",
        "amount": "19.90",
        "event_ts": "2026-08-11T12:00:00.123",
    }
    record.update(overrides)
    return record


def test_soft_delete_topic_decodes_upsert_and_tombstone():
    config = loader.TOPIC_CONFIG["dwd_order_detail"]
    upsert = FakeMessage(
        topic="dwd_order_detail",
        key=b'{"detail_id":"D-1"}',
        value=json.dumps(
            {"detail_id": "D-1", "order_update_time": "2026-08-11T12:00:00.123"}
        ).encode(),
    )
    tombstone = FakeMessage(
        topic="dwd_order_detail",
        key=b'{"detail_id":"D-1"}',
        value=None,
        offset=42,
    )

    assert loader.decode_message(config, upsert) == {
        "detail_id": "D-1",
        "order_update_time": "2026-08-11 12:00:00",
        "payment_time": None,
        "refund_time": None,
        "order_create_time": None,
        "detail_update_time": None,
        "is_deleted": 0,
    }
    assert loader.decode_message(config, tombstone) == {
        "detail_id": "D-1",
        "is_deleted": 1,
    }


def test_decode_rejects_non_object_and_missing_required_fields():
    config = loader.TOPIC_CONFIG["ads_order_daily"]
    non_object = FakeMessage(
        topic="ads_order_daily",
        key=None,
        value=b"[]",
    )
    missing_channel = FakeMessage(
        topic="ads_order_daily",
        key=None,
        value=b'{"order_date":"2026-08-11"}',
    )

    with pytest.raises(TypeError, match="object"):
        loader.decode_message(config, non_object)
    with pytest.raises(ValueError, match="channel"):
        loader.decode_message(config, missing_channel)


@pytest.mark.parametrize(
    ("overrides", "expected_field"),
    (
        ({"user_id": "abc"}, "user_id"),
        ({"amount": "bad"}, "amount"),
        ({"event_ts": "not-a-time"}, "event_ts"),
        ({"event_sequence": 1 << 40}, "event_sequence"),
    ),
)
def test_decode_rejects_clickhouse_type_errors_before_batch_insert(overrides, expected_field):
    msg = FakeMessage(
        topic="dwd_user_behavior",
        key=None,
        value=json.dumps(valid_behavior_record(**overrides)).encode(),
    )

    with pytest.raises((TypeError, ValueError), match=expected_field):
        loader.decode_message(loader.TOPIC_CONFIG["dwd_user_behavior"], msg)


def test_nullable_timestamp_and_decimal_values_accepted_without_false_rejection():
    msg = FakeMessage(
        topic="dwd_user_behavior",
        key=None,
        value=json.dumps(
            valid_behavior_record(
                order_id=None,
                product_name=None,
                category_id=None,
                amount="19.900",
                event_ts="2026-08-11T12:00:00.987",
            )
        ).encode(),
    )

    decoded = loader.decode_message(loader.TOPIC_CONFIG["dwd_user_behavior"], msg)

    assert decoded["order_id"] is None
    assert decoded["category_id"] is None
    assert decoded["amount"] == "19.900"
    assert decoded["event_ts"] == "2026-08-11 12:00:00"


def test_type_error_is_quarantined_and_following_valid_row_can_still_be_written(monkeypatch):
    bad = FakeMessage(
        topic="dwd_user_behavior",
        key=None,
        value=json.dumps(valid_behavior_record(user_id="abc")).encode(),
        partition=0,
        offset=10,
    )
    good = FakeMessage(
        topic="dwd_user_behavior",
        key=None,
        value=json.dumps(valid_behavior_record(event_id="E-2", user_id=102)).encode(),
        partition=0,
        offset=11,
    )
    producer = FakeProducer()
    pending_offsets = {}
    rows = []
    inserted = []
    monkeypatch.setattr(
        loader,
        "insert_json_each_rows",
        lambda _url, _user, _password, table, records, metrics=None: inserted.append(
            (table, list(records))
        ),
    )

    for msg in (bad, good):
        try:
            record = loader.decode_message(loader.TOPIC_CONFIG[msg.topic()], msg)
        except (TypeError, ValueError) as error:
            loader.quarantine_message(
                producer,
                loader.DEFAULT_DLQ_TOPIC,
                msg,
                error,
                pending_offsets,
            )
            continue
        record.update(
            source_topic=msg.topic(),
            source_partition=msg.partition(),
            source_offset=msg.offset(),
            ingest_version=loader.kafka_ingest_version(msg.partition(), msg.offset()),
        )
        rows.append(record)
        loader.mark_handled_offset(pending_offsets, msg)

    loader.insert_json_each_rows("url", "user", "password", "dwd_user_behavior", rows)

    assert len(producer.records) == 1
    assert json.loads(producer.records[0]["value"])["source_offset"] == 10
    assert inserted[0][0] == "dwd_user_behavior"
    assert [row["event_id"] for row in inserted[0][1]] == ["E-2"]
    assert pending_offsets == {("dwd_user_behavior", 0): 12}


def test_ingest_version_is_replay_stable_and_offset_ordered():
    first = loader.kafka_ingest_version(partition=2, offset=41)
    replay = loader.kafka_ingest_version(partition=2, offset=41)
    later = loader.kafka_ingest_version(partition=2, offset=42)

    assert first == replay
    assert later > first
    assert first >= (loader.DEFAULT_VERSION_EPOCH << 56)


def test_ingest_version_rejects_unsupported_positions():
    for partition, offset in (
        (-1, 0),
        (loader.MAX_VERSION_PARTITION + 1, 0),
        (0, -1),
        (0, loader.MAX_VERSION_OFFSET + 1),
    ):
        try:
            loader.kafka_ingest_version(partition, offset)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid Kafka position must be rejected")

    try:
        loader.kafka_ingest_version(0, 0, loader.MAX_VERSION_EPOCH + 1)
    except ValueError:
        pass
    else:
        raise AssertionError("invalid loader epoch must be rejected")


def test_higher_epoch_supersedes_offsets_from_a_previous_kafka_log():
    previous_log = loader.kafka_ingest_version(partition=2, offset=999_999, version_epoch=128)
    reset_log = loader.kafka_ingest_version(partition=2, offset=0, version_epoch=129)
    assert reset_log > previous_log


def test_ingest_version_uses_the_full_uint64_range_without_overflow():
    maximum = loader.kafka_ingest_version(
        loader.MAX_VERSION_PARTITION,
        loader.MAX_VERSION_OFFSET,
        loader.MAX_VERSION_EPOCH,
    )
    assert maximum == (1 << 64) - 1


def test_loader_metrics_render_required_prometheus_series():
    metrics = loader.LoaderMetrics()
    metrics.record_consumed()
    metrics.record_insert_failure()
    metrics.record_decode_failure()
    metrics.record_dlq_publish_success()
    metrics.record_dlq_publish_failure()
    metrics.record_insert_success(7, 0.125)

    rendered = metrics.render()
    assert "clickhouse_loader_messages_consumed_total 1" in rendered
    assert "clickhouse_loader_rows_inserted_total 7" in rendered
    assert "clickhouse_loader_insert_failures_total 1" in rendered
    assert "clickhouse_loader_decode_failures_total 1" in rendered
    assert "clickhouse_loader_dlq_publish_success_total 1" in rendered
    assert "clickhouse_loader_dlq_publish_failures_total 1" in rendered
    assert "clickhouse_loader_successful_batches_total 1" in rendered
    assert "clickhouse_loader_last_success_unixtime_seconds 0.000000" not in rendered
    assert "clickhouse_loader_last_batch_duration_seconds 0.125000" in rendered
    assert "clickhouse_loader_start_unixtime_seconds 0.000000" not in rendered


def test_dlq_envelope_is_lossless_and_message_id_is_deterministic():
    msg = FakeMessage(
        topic="dwd_order_detail",
        partition=1,
        offset=99,
        key=b'\x00{"detail_id":"D-1"}',
        value=b"not-json\xff",
    )
    first = loader.build_dlq_envelope(
        msg,
        ValueError("invalid payload"),
        failed_at="2026-08-11T12:34:56.000Z",
    )
    second = loader.build_dlq_envelope(
        msg,
        ValueError("a different diagnostic must not change identity"),
        failed_at="2026-08-12T00:00:00.000Z",
    )

    assert first["message_id"] == second["message_id"]
    assert len(first["message_id"]) == 64
    assert first["source_topic"] == "dwd_order_detail"
    assert first["source_partition"] == 1
    assert first["source_offset"] == 99
    assert base64.b64decode(first["key_base64"]) == msg.key()
    assert base64.b64decode(first["value_base64"]) == msg.value()
    assert first["error_type"] == "ValueError"
    assert first["error_message"] == "invalid payload"
    assert first["failed_at"] == "2026-08-11T12:34:56.000Z"

    changed_offset = FakeMessage(
        topic=msg.topic(), key=msg.key(), value=msg.value(), partition=1, offset=100
    )
    assert loader.dlq_message_id(changed_offset) != first["message_id"]


def test_quarantine_makes_offset_eligible_only_after_dlq_acknowledgement():
    msg = FakeMessage(
        topic="ads_order_daily",
        partition=2,
        offset=41,
        key=b'{"order_date":"2026-08-11","channel":"app"}',
        value=b"broken",
    )
    producer = FakeProducer()
    metrics = loader.LoaderMetrics()
    pending_offsets = {}

    envelope = loader.quarantine_message(
        producer,
        loader.DEFAULT_DLQ_TOPIC,
        msg,
        ValueError("bad JSON"),
        pending_offsets,
        metrics,
    )

    assert pending_offsets == {("ads_order_daily", 2): 42}
    assert producer.records[0]["topic"] == loader.DEFAULT_DLQ_TOPIC
    assert producer.records[0]["key"] == envelope["message_id"].encode("ascii")
    assert json.loads(producer.records[0]["value"])["source_offset"] == 41
    assert metrics.decode_failures == 1
    assert metrics.dlq_publish_successes == 1
    assert metrics.dlq_publish_failures == 0


@pytest.mark.parametrize(
    ("producer", "expected_error"),
    (
        (FakeProducer(delivery_error="broker unavailable"), RuntimeError),
        (FakeProducer(remaining=1), TimeoutError),
    ),
)
def test_dlq_publish_failure_never_advances_bad_message(producer, expected_error):
    msg = FakeMessage(
        topic="ads_order_daily",
        partition=0,
        offset=7,
        key=None,
        value=b"broken",
    )
    metrics = loader.LoaderMetrics()
    pending_offsets = {}

    with pytest.raises(expected_error):
        loader.quarantine_message(
            producer,
            loader.DEFAULT_DLQ_TOPIC,
            msg,
            ValueError("bad JSON"),
            pending_offsets,
            metrics,
        )

    assert pending_offsets == {}
    assert metrics.decode_failures == 1
    assert metrics.dlq_publish_successes == 0
    assert metrics.dlq_publish_failures == 1


def test_explicit_commit_uses_only_supplied_handled_offsets_and_clears_after_success():
    consumer = FakeConsumer()
    pending = {("topic-b", 2): 12, ("topic-a", 0): 4}

    loader.commit_handled_offsets(consumer, pending)

    offsets, asynchronous = consumer.commits[0]
    assert asynchronous is False
    assert [(item.topic, item.partition, item.offset) for item in offsets] == [
        ("topic-a", 0, 4),
        ("topic-b", 2, 12),
    ]
    assert pending == {}


def test_failed_explicit_commit_preserves_offsets_for_retry():
    consumer = FakeConsumer(should_fail=True)
    pending = {("topic-a", 0): 4}

    with pytest.raises(RuntimeError, match="commit failed"):
        loader.commit_handled_offsets(consumer, pending)

    assert pending == {("topic-a", 0): 4}
