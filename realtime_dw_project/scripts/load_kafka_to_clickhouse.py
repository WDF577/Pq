import argparse
import json
import urllib.parse
import urllib.request
import base64

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


def insert_json_each_row(clickhouse_url, user, password, table, record):
    query = urllib.parse.urlencode({"query": f"INSERT INTO {table} FORMAT JSONEachRow"})
    data = (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")
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
    return parser.parse_args()


def main():
    args = parse_args()
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(list(TOPIC_TO_TABLE.keys()))

    inserted = 0
    print("loading Kafka result topics into ClickHouse...")
    while True:
        msg = consumer.poll(timeout=args.idle_timeout if args.idle_timeout > 0 else 1.0)
        if msg is None:
            if args.idle_timeout > 0:
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
        insert_json_each_row(
            args.clickhouse_url,
            args.clickhouse_user,
            args.clickhouse_password,
            table,
            record,
        )
        inserted += 1
        print(f"inserted {topic} -> {table}: {record}")
        if args.max_messages > 0 and inserted >= args.max_messages:
            break

    consumer.close()
    print(f"done, inserted {inserted} rows.")


if __name__ == "__main__":
    main()
