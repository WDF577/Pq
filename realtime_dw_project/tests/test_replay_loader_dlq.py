import base64
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPTS_DIR = Path(__file__).parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))
MODULE_PATH = SCRIPTS_DIR / "replay_loader_dlq.py"
SPEC = importlib.util.spec_from_file_location("replay_loader_dlq", MODULE_PATH)
replay = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(replay)


class FakeProducer:
    def __init__(self):
        self.records = []
        self.callback = None

    def produce(self, topic, *, key, value, on_delivery):
        self.records.append({"topic": topic, "key": key, "value": value})
        self.callback = on_delivery

    def flush(self, _timeout):
        self.callback(None, None)
        return 0


class FakeDlqMessage:
    def __init__(self, envelope, offset):
        self._envelope = envelope
        self._offset = offset

    def value(self):
        return json.dumps(self._envelope).encode()

    def error(self):
        return None

    def offset(self):
        return self._offset

    def topic(self):
        return replay.DEFAULT_DLQ_TOPIC

    def partition(self):
        return 0


class FakeSnapshotConsumer:
    def __init__(self, messages):
        self.messages = list(messages)
        self.index = 0
        self.current_offset = 0

    def list_topics(self, topic, timeout):
        assert topic == replay.DEFAULT_DLQ_TOPIC
        assert timeout >= 10
        return SimpleNamespace(
            topics={topic: SimpleNamespace(error=None, partitions={0: object()})}
        )

    def get_watermark_offsets(self, topic_partition, timeout):
        assert topic_partition.partition == 0
        return 0, len(self.messages)

    def assign(self, starts):
        self.current_offset = starts[0].offset

    def position(self, partitions):
        return [
            replay.TopicPartition(item.topic, item.partition, self.current_offset)
            for item in partitions
        ]

    def poll(self, timeout):
        if self.index >= len(self.messages):
            return None
        message = self.messages[self.index]
        self.index += 1
        self.current_offset = message.offset() + 1
        return message


class FakeConsumerWithEmptyPartition:
    """Partition 2 is empty and keeps librdkafka's OFFSET_INVALID position."""

    def __init__(self, messages):
        self.messages = list(messages)
        self.index = 0
        self.positions = {0: 0, 1: 0, 2: -1001}

    def list_topics(self, topic, timeout):
        return SimpleNamespace(
            topics={
                topic: SimpleNamespace(
                    error=None,
                    partitions={0: object(), 1: object(), 2: object()},
                )
            }
        )

    def get_watermark_offsets(self, topic_partition, timeout):
        if topic_partition.partition == 2:
            return 0, 0
        return 0, 1

    def assign(self, starts):
        assert [item.partition for item in starts] == [0, 1, 2]

    def position(self, partitions):
        return [
            replay.TopicPartition(
                item.topic,
                item.partition,
                self.positions[item.partition],
            )
            for item in partitions
        ]

    def poll(self, timeout):
        if self.index >= len(self.messages):
            return None
        message = self.messages[self.index]
        self.index += 1
        self.positions[message.partition()] = message.offset() + 1
        return message


def make_envelope(value=b'{"order_date":"2026-08-11","channel":"app"}'):
    return {
        "schema_version": 1,
        "message_id": "a" * 64,
        "status": "pending",
        "source_topic": "ads_order_daily",
        "source_partition": 1,
        "source_offset": 18,
        "key_base64": base64.b64encode(
            b'{"order_date":"2026-08-11","channel":"app"}'
        ).decode("ascii"),
        "value_base64": None if value is None else base64.b64encode(value).decode("ascii"),
        "error_type": "ValueError",
        "error_message": "missing required field(s): paid_orders",
        "failed_at": "2026-08-11T12:00:00.000Z",
    }


def make_behavior_envelope(user_id="abc"):
    value = {
        "event_id": "E-1",
        "session_id": "S-1",
        "event_sequence": 1,
        "event_version": 1,
        "user_id": user_id,
        "product_id": 201,
        "shop_id": 301,
        "event_type": "pay",
        "channel": "app",
        "amount": "19.90",
        "event_ts": "2026-08-11T12:00:00.123",
    }
    envelope = make_envelope(json.dumps(value).encode())
    envelope["source_topic"] = "dwd_user_behavior"
    return envelope


def test_default_summary_redacts_payload_but_keeps_operational_metadata():
    summary = replay.safe_summary(make_envelope())

    assert summary["source"] == "ads_order_daily[1]@18"
    assert summary["status"] == "pending"
    assert summary["value_bytes"] > 0
    assert len(summary["value_sha256"]) == 64
    assert "value_utf8" not in summary
    assert "key_utf8" not in summary


def test_json_patch_repairs_value_without_changing_key_or_source_topic():
    envelope = make_envelope()
    topic, key, value = replay.prepare_replay_record(
        envelope,
        patch={"paid_orders": 9},
    )

    assert topic == "ads_order_daily"
    assert json.loads(key) == {"order_date": "2026-08-11", "channel": "app"}
    assert json.loads(value) == {
        "order_date": "2026-08-11",
        "channel": "app",
        "paid_orders": 9,
    }


@pytest.mark.parametrize(
    "kwargs",
    (
        {},
        {"patch": {}, "replay_original": True},
        {"replacement_value": b"{}", "replay_original": True},
    ),
)
def test_replay_requires_exactly_one_explicit_repair_mode(kwargs):
    with pytest.raises(ValueError, match="exactly one"):
        replay.prepare_replay_record(make_envelope(), **kwargs)


def test_execute_rejects_message_id_without_a_repair_mode(monkeypatch):
    args = SimpleNamespace(
        limit=20,
        scan_limit=100,
        idle_timeout=1.0,
        dlq_topic=replay.DEFAULT_DLQ_TOPIC,
        execute=True,
        message_id="a" * 64,
        patch_json=None,
        replacement_value_file=None,
        replay_original=False,
        target_topic=None,
    )
    monkeypatch.setattr(replay, "parse_args", lambda: args)

    with pytest.raises(ValueError, match="requires exactly one repair mode"):
        replay.main()


def test_tombstone_requires_replacement_instead_of_json_patch():
    with pytest.raises(ValueError, match="tombstone"):
        replay.prepare_replay_record(make_envelope(value=None), patch={"x": 1})

    topic, _key, value = replay.prepare_replay_record(
        make_envelope(value=None),
        replacement_value=b'{"order_date":"2026-08-11","channel":"app"}',
    )
    assert topic == "ads_order_daily"
    assert value == b'{"order_date":"2026-08-11","channel":"app"}'


def test_topic_override_requires_a_second_explicit_guard():
    with pytest.raises(ValueError, match="allow-topic-override"):
        replay.prepare_replay_record(
            make_envelope(), replay_original=True, target_topic="dwd_order_detail"
        )

    topic, _key, _value = replay.prepare_replay_record(
        make_envelope(),
        replacement_value=b'{"detail_id":"D-1"}',
        target_topic="dwd_order_detail",
        allow_topic_override=True,
    )
    assert topic == "dwd_order_detail"


def test_invalid_base64_and_unknown_source_topics_are_rejected():
    invalid_base64 = make_envelope()
    invalid_base64["value_base64"] = "!!!"
    with pytest.raises(ValueError, match="base64"):
        replay.validate_envelope(invalid_base64)

    unknown = make_envelope()
    unknown["source_topic"] = "untrusted-topic"
    with pytest.raises(ValueError, match="unknown source topic"):
        replay.validate_envelope(unknown)


def test_unknown_dlq_schema_version_is_rejected():
    envelope = make_envelope()
    envelope["schema_version"] = replay.DLQ_SCHEMA_VERSION + 1

    with pytest.raises(ValueError, match="schema_version"):
        replay.validate_envelope(envelope)


def test_replay_candidate_must_pass_loader_type_validation_after_repair():
    envelope = make_behavior_envelope(user_id="abc")

    with pytest.raises(ValueError, match="user_id"):
        replay.prepare_replay_record(envelope, replay_original=True)
    with pytest.raises(ValueError, match="user_id"):
        replay.prepare_replay_record(envelope, patch={"amount": "20.00"})

    topic, _key, value = replay.prepare_replay_record(
        envelope,
        patch={"user_id": 102},
    )
    assert topic == "dwd_user_behavior"
    assert json.loads(value)["user_id"] == 102


def test_mark_replayed_updates_compacted_dlq_record_after_acknowledged_replay():
    producer = FakeProducer()
    envelope = make_envelope()

    resolved = replay.mark_replayed(
        producer,
        replay.DEFAULT_DLQ_TOPIC,
        envelope,
        "ads_order_daily",
        b'{"fixed":true}',
    )

    published = json.loads(producer.records[0]["value"])
    assert producer.records[0]["key"] == ("a" * 64).encode("ascii")
    assert resolved["status"] == "replayed"
    assert published["status"] == "replayed"
    assert published["replay_topic"] == "ads_order_daily"
    assert len(published["replay_value_sha256"]) == 64


def test_snapshot_scan_reports_truncation_instead_of_treating_old_pending_as_latest():
    pending = make_envelope()
    replayed = dict(pending, status="replayed", replayed_at="2026-08-11T13:00:00.000Z")
    messages = [FakeDlqMessage(pending, 0), FakeDlqMessage(replayed, 1)]

    truncated = replay.collect_envelopes(
        FakeSnapshotConsumer(messages),
        replay.DEFAULT_DLQ_TOPIC,
        idle_timeout=1,
        scan_limit=1,
    )
    complete = replay.collect_envelopes(
        FakeSnapshotConsumer(messages),
        replay.DEFAULT_DLQ_TOPIC,
        idle_timeout=1,
        scan_limit=None,
    )

    assert truncated.truncated is True
    assert truncated.complete is False
    assert truncated.envelopes["a" * 64]["status"] == "pending"
    assert complete.truncated is False
    assert complete.complete is True
    assert complete.envelopes["a" * 64]["status"] == "replayed"


def test_empty_partition_with_invalid_position_does_not_block_complete_snapshot():
    first = FakeDlqMessage(make_envelope(), 0)
    second_envelope = make_envelope()
    second_envelope["message_id"] = "b" * 64
    second = FakeDlqMessage(second_envelope, 0)
    second.partition = lambda: 1
    consumer = FakeConsumerWithEmptyPartition([first, second])

    result = replay.collect_envelopes(
        consumer,
        replay.DEFAULT_DLQ_TOPIC,
        idle_timeout=0.1,
        scan_limit=None,
    )

    assert result.complete is True
    assert result.truncated is False
    assert result.scanned == 2
    assert set(result.envelopes) == {"a" * 64, "b" * 64}
    assert consumer.positions[2] == -1001


def test_execute_disables_scan_limit_and_refuses_incomplete_snapshot(monkeypatch):
    args = SimpleNamespace(
        limit=20,
        scan_limit=1,
        idle_timeout=1.0,
        bootstrap_servers="localhost:9092",
        dlq_topic=replay.DEFAULT_DLQ_TOPIC,
        execute=True,
        message_id="a" * 64,
        patch_json='{"paid_orders": 1}',
        replacement_value_file=None,
        replay_original=False,
        target_topic=None,
    )
    scan_limits = []

    class ConsumerStub:
        def __init__(self, _config):
            pass

        def close(self):
            pass

    def incomplete_scan(_consumer, _topic, _idle_timeout, scan_limit):
        scan_limits.append(scan_limit)
        return replay.ScanResult({"a" * 64: make_envelope()}, 1, False, False)

    monkeypatch.setattr(replay, "parse_args", lambda: args)
    monkeypatch.setattr(replay, "Consumer", ConsumerStub)
    monkeypatch.setattr(replay, "collect_envelopes", incomplete_scan)

    with pytest.raises(RuntimeError, match="high watermark"):
        replay.main()

    assert scan_limits == [None]
