import argparse
import base64
import json
import time
import urllib.parse
import urllib.request
from collections import defaultdict

from confluent_kafka import Consumer


TOPIC_TO_TABLE = {
    "dwd_user_behavior": "dwd_user_behavior",
    "ads_realtime_overview": "ads_realtime_overview",
    "ads_product_rank": "ads_product_rank",
    "ads_category_rank": "ads_category_rank",
    "ads_channel_funnel": "ads_channel_funnel",
}


def normalize_datetime(value):
    if value is None:
        return None
    text = str(value).replace("T", " ")
    if "." in text:
        text = text.split(".", 1)[0]
    return text


def normalize_record(topic, record):
    if topic == "dwd_user_behavior":
        record["event_ts"] = normalize_datetime(record.get("event_ts"))
    elif topic in ("ads_realtime_overview", "ads_product_rank", "ads_category_rank", "ads_channel_funnel"):
        record["window_start"] = normalize_datetime(record.get("window_start"))
        record["window_end"] = normalize_datetime(record.get("window_end"))
    return record


def insert_json_each_rows(clickhouse_url, user, password, table, records):
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
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()


def parse_args():
    parser = argparse.ArgumentParser(description="Load Flink Kafka result topics into ClickHouse.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--clickhouse-url", default="http://localhost:8123")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="clickhouse")
    parser.add_argument("--group-id", default="clickhouse_loader")
    parser.add_argument("--max-messages", type=int, default=0, help="0 means keep running.")
    parser.add_argument("--idle-timeout", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--flush-interval", type=float, default=2.0)
    return parser.parse_args()


def main():
    args = parse_args()
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(list(TOPIC_TO_TABLE.keys()))

    buffers = defaultdict(list)
    consumed = 0
    inserted = 0
    last_flush = time.monotonic()

    def flush_all():
        nonlocal inserted, last_flush
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
            )
            batch_total += len(records)
            records.clear()
        if batch_total:
            consumer.commit(asynchronous=False)
            inserted += batch_total
            print(f"flushed {batch_total} rows, total inserted: {inserted}")
        last_flush = time.monotonic()

    print("loading Kafka result topics into ClickHouse...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if any(buffers.values()) and time.monotonic() - last_flush >= args.flush_interval:
                    flush_all()
                if args.idle_timeout > 0 and time.monotonic() - last_flush >= args.idle_timeout:
                    break
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            topic = msg.topic()
            table = TOPIC_TO_TABLE.get(topic)
            if not table:
                continue

            record = normalize_record(topic, json.loads(msg.value().decode("utf-8")))
            buffers[table].append(record)
            consumed += 1

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
    print(f"done, consumed {consumed} messages and inserted {inserted} rows.")


if __name__ == "__main__":
    main()
