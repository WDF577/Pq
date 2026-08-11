"""Exercise MySQL DELETE -> Kafka tombstone -> ClickHouse soft delete.

The drill removes one order header while leaving its detail/payment/refund rows
in place. The Flink changelog join must retract every related DWD detail. By
default the source header is restored after the deletion has been observed, so
the demo data returns to its original logical state.
"""

import argparse
import base64
import json
import os
import time
import urllib.parse
import urllib.request
import uuid

import mysql.connector
from confluent_kafka import Consumer, TopicPartition


ORDER_COLUMNS = (
    "order_id", "user_id", "shop_id", "order_status", "channel", "order_amount",
    "promotion_code", "create_time", "update_time", "version_no",
)


def sql_literal(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def clickhouse_query(url, user, password, sql):
    query = urllib.parse.urlencode({"query": f"{sql} FORMAT JSON"})
    auth = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
    request = urllib.request.Request(
        f"{url}/?{query}",
        headers={"Authorization": f"Basic {auth}"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))["data"]


def detail_state(args, detail_ids):
    keys = ", ".join(sql_literal(value) for value in detail_ids)
    rows = clickhouse_query(
        args.clickhouse_url,
        args.clickhouse_user,
        args.clickhouse_password,
        (
            "SELECT countIf(is_deleted = 0) AS active, "
            "countIf(is_deleted = 1) AS deleted "
            f"FROM dwd_order_detail FINAL WHERE detail_id IN ({keys})"
        ),
    )
    return int(rows[0]["active"]), int(rows[0]["deleted"])


def wait_for_state(args, detail_ids, *, deleted):
    expected = len(detail_ids)
    deadline = time.monotonic() + args.timeout
    last_state = None
    while time.monotonic() < deadline:
        last_state = detail_state(args, detail_ids)
        active, soft_deleted = last_state
        if deleted and active == 0 and soft_deleted == expected:
            return last_state
        if not deleted and active == expected and soft_deleted == 0:
            return last_state
        time.sleep(args.poll_interval)
    state_name = "soft-deleted" if deleted else "restored"
    raise TimeoutError(
        f"DWD details did not become {state_name} within {args.timeout}s; "
        f"last state active/deleted={last_state}. Check CDC, Flink DWD and "
        "the clickhouse-loader service logs."
    )


def start_ods_observer(args):
    """Pin a manual consumer to every ODS partition's current end offset."""
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": f"tombstone_drill_{uuid.uuid4().hex}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    metadata = consumer.list_topics(args.ods_topic, timeout=15)
    topic_metadata = metadata.topics.get(args.ods_topic)
    if topic_metadata is None or topic_metadata.error is not None:
        consumer.close()
        raise RuntimeError(f"Cannot inspect ODS topic {args.ods_topic}: {topic_metadata}")
    positions = []
    for partition_id in sorted(topic_metadata.partitions):
        partition = TopicPartition(args.ods_topic, partition_id)
        _low, high = consumer.get_watermark_offsets(partition, timeout=10, cached=False)
        positions.append(TopicPartition(args.ods_topic, partition_id, high))
    if not positions:
        consumer.close()
        raise RuntimeError(f"ODS topic {args.ods_topic} has no partitions")
    consumer.assign(positions)
    return consumer


def wait_for_ods_change(args, consumer, order_id, *, tombstone):
    """Wait for the exact order key to appear as a tombstone or restored value."""
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        msg = consumer.poll(timeout=min(args.poll_interval, 1.0))
        if msg is None:
            continue
        if msg.error():
            raise RuntimeError(f"ODS consumer error: {msg.error()}")
        if msg.key() is None:
            continue
        try:
            key = json.loads(msg.key().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if str(key.get("order_id")) != str(order_id):
            continue
        if tombstone and msg.value() is None:
            return msg.partition(), msg.offset()
        if not tombstone and msg.value() is not None:
            value = json.loads(msg.value().decode("utf-8"))
            if str(value.get("order_id")) == str(order_id):
                return msg.partition(), msg.offset()
    expected = "tombstone" if tombstone else "restored upsert"
    raise TimeoutError(
        f"Order {order_id} did not emit an ODS {expected} within {args.timeout}s"
    )


def candidate_order(cursor, args):
    if args.order_id:
        cursor.execute(
            f"SELECT {', '.join(ORDER_COLUMNS)} FROM order_info WHERE order_id = %s",
            (args.order_id,),
        )
    else:
        cursor.execute(
            f"SELECT {', '.join(ORDER_COLUMNS)} FROM order_info "
            "ORDER BY create_time DESC LIMIT 200"
        )
    headers = cursor.fetchall()
    for header in headers:
        cursor.execute(
            "SELECT detail_id FROM order_detail WHERE order_id = %s ORDER BY detail_id",
            (header["order_id"],),
        )
        detail_ids = [row["detail_id"] for row in cursor.fetchall()]
        if not detail_ids:
            continue
        active, deleted = detail_state(args, detail_ids)
        if active == len(detail_ids) and deleted == 0:
            return header, detail_ids
    raise RuntimeError(
        "No source order with fully active ClickHouse DWD details was found. "
        "Generate orders and wait for the realtime pipeline first."
    )


def restore_header(cursor, connection, header):
    placeholders = ", ".join(["%s"] * len(ORDER_COLUMNS))
    cursor.execute(
        f"INSERT INTO order_info ({', '.join(ORDER_COLUMNS)}) VALUES ({placeholders})",
        tuple(header[column] for column in ORDER_COLUMNS),
    )
    connection.commit()


def parse_args():
    parser = argparse.ArgumentParser(description="Verify order DELETE tombstone propagation.")
    parser.add_argument("--mysql-host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--mysql-port", type=int, default=int(os.getenv("MYSQL_PORT", "3307")))
    parser.add_argument("--mysql-user", default=os.getenv("MYSQL_USER", "root"))
    parser.add_argument("--mysql-password", default=os.getenv("MYSQL_ROOT_PASSWORD", "root"))
    parser.add_argument("--mysql-database", default=os.getenv("MYSQL_DATABASE", "ecommerce"))
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    )
    parser.add_argument("--ods-topic", default="ods_order_info")
    parser.add_argument("--clickhouse-url", default=os.getenv("CLICKHOUSE_URL", "http://127.0.0.1:8123"))
    parser.add_argument("--clickhouse-user", default=os.getenv("CLICKHOUSE_USER", "default"))
    parser.add_argument("--clickhouse-password", default=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"))
    parser.add_argument("--order-id", help="Optional source order to exercise.")
    parser.add_argument("--timeout", type=float, default=90.0)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument(
        "--keep-deleted",
        action="store_true",
        help="Do not restore the source order after verification.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    connection = mysql.connector.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
        autocommit=False,
    )
    cursor = connection.cursor(dictionary=True)
    header = None
    detail_ids = []
    source_deleted = False
    ods_observer = None
    try:
        header, detail_ids = candidate_order(cursor, args)
        print(
            f"selected order {header['order_id']} with {len(detail_ids)} active DWD details"
        )
        ods_observer = start_ods_observer(args)

        cursor.execute("DELETE FROM order_info WHERE order_id = %s", (header["order_id"],))
        if cursor.rowcount != 1:
            connection.rollback()
            raise RuntimeError("Source DELETE did not affect exactly one order header.")
        connection.commit()
        source_deleted = True
        print("source DELETE committed; waiting for the ODS tombstone...")
        tombstone_position = wait_for_ods_change(
            args, ods_observer, header["order_id"], tombstone=True
        )
        print(
            f"verified: ODS tombstone at partition/offset={tombstone_position[0]}/"
            f"{tombstone_position[1]}"
        )
        print("waiting for DWD soft-delete versions...")
        wait_for_state(args, detail_ids, deleted=True)
        print("verified: every related detail is_deleted=1 in ClickHouse FINAL")

        if args.keep_deleted:
            print("--keep-deleted selected; source order remains deleted")
            source_deleted = False
            return

        restore_header(cursor, connection, header)
        source_deleted = False
        print("source order restored; waiting for the ODS upsert...")
        restore_position = wait_for_ods_change(
            args, ods_observer, header["order_id"], tombstone=False
        )
        print(
            f"verified: ODS restore upsert at partition/offset={restore_position[0]}/"
            f"{restore_position[1]}"
        )
        print("waiting for DWD rows to become active again...")
        wait_for_state(args, detail_ids, deleted=False)
        print("verified: restored order is active again; drill completed")
    finally:
        if source_deleted and header is not None and not args.keep_deleted:
            try:
                restore_header(cursor, connection, header)
                print("cleanup: restored source order after an interrupted/failed drill")
            except Exception as cleanup_error:
                print(f"cleanup failed; restore order {header['order_id']} manually: {cleanup_error}")
        if ods_observer is not None:
            ods_observer.close()
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()
