import json
import random
import time
import uuid
import argparse
from datetime import datetime, timedelta

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ods_user_behavior"

EVENT_TYPES = ["view", "cart", "order", "pay"]
CHANNELS = ["app", "h5", "wechat", "search", "ad"]
PRODUCT_IDS = list(range(1001, 1101))    # 100 个商品
SHOP_IDS = list(range(1, 21))            # 20 个店铺
USER_ID_RANGE = (1, 50000)

# 脏数据概率
DIRTY_EVENT_ID_NULL = 0.01      # 1%
DIRTY_USER_ID_NULL = 0.005      # 0.5%
DIRTY_EVENT_TYPE_INVALID = 0.005  # 0.5%
DIRTY_PRODUCT_ID_INVALID = 0.01   # 1%
DIRTY_SHOP_ID_INVALID = 0.005     # 0.5%


def build_event(event_time_offset=0, time_span_minutes=0, event_time=None):
    """生成一条用户行为事件，包含正常数据和脏数据"""
    event_type = random.choices(EVENT_TYPES, weights=[70, 15, 10, 5], k=1)[0]
    amount = 0.0
    if event_type in ("order", "pay"):
        amount = round(random.uniform(29, 799), 2)

    event_id = str(uuid.uuid4())
    user_id = random.randint(*USER_ID_RANGE)
    product_id = random.choice(PRODUCT_IDS)
    shop_id = random.choice(SHOP_IDS)

    # === 脏数据注入 ===

    # 1% 概率：event_id 为空
    if random.random() < DIRTY_EVENT_ID_NULL:
        event_id = None

    # 0.5% 概率：user_id 为 None
    if random.random() < DIRTY_USER_ID_NULL:
        user_id = None

    # 0.5% 概率：event_type 为非法值
    if random.random() < DIRTY_EVENT_TYPE_INVALID:
        event_type = random.choice(["delete", "update", "login", "logout"])

    # 1% 概率：product_id 为不存在的值
    if random.random() < DIRTY_PRODUCT_ID_INVALID:
        product_id = random.choice([9999, 8888, 0, -1])

    # 0.5% 概率：shop_id 为不存在的值
    if random.random() < DIRTY_SHOP_ID_INVALID:
        shop_id = random.choice([99, 88, 0])

    if event_time is None:
        # Continuous mode keeps the original relative-time behavior.
        ts_offset = event_time_offset
        if time_span_minutes > 0:
            ts_offset = event_time_offset + random.randint(-time_span_minutes * 60, 0)
        event_time = datetime.now() + timedelta(seconds=ts_offset)

    session_id = str(uuid.uuid4())
    order_id = str(uuid.uuid4()) if event_type in ("order", "pay") else None
    return {
        "event_id": event_id,
        "session_id": session_id,
        "order_id": order_id,
        "event_sequence": 1,
        "event_version": 1,
        "user_id": user_id,
        "product_id": product_id,
        "shop_id": shop_id,
        "event_type": event_type,
        "channel": random.choice(CHANNELS),
        "amount": amount,
        "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Generate mock ecommerce behavior events.")
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--count", type=int, default=0, help="0 means keep running.")
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument("--event-time-offset", type=int, default=0)
    parser.add_argument(
        "--time-span-minutes",
        type=int,
        default=0,
        help="For finite runs, spread event time in order across the past N minutes.",
    )
    parser.add_argument(
        "--out-of-order-seconds",
        type=int,
        default=3,
        help="Bounded event-time jitter for finite time-span runs.",
    )
    parser.add_argument("--no-dirty", action="store_true", help="Disable dirty data generation.")
    return parser.parse_args()


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    args = parse_args()
    producer = Producer({"bootstrap.servers": args.bootstrap_servers})

    if args.no_dirty:
        global DIRTY_EVENT_ID_NULL, DIRTY_USER_ID_NULL, DIRTY_EVENT_TYPE_INVALID
        global DIRTY_PRODUCT_ID_INVALID, DIRTY_SHOP_ID_INVALID
        DIRTY_EVENT_ID_NULL = 0
        DIRTY_USER_ID_NULL = 0
        DIRTY_EVENT_TYPE_INVALID = 0
        DIRTY_PRODUCT_ID_INVALID = 0
        DIRTY_SHOP_ID_INVALID = 0

    print(f"writing mock events to Kafka topic: {args.topic}")
    print(f"products: {len(PRODUCT_IDS)}, shops: {len(SHOP_IDS)}, users: {USER_ID_RANGE}")
    span_end = datetime.now() + timedelta(seconds=args.event_time_offset)
    span_start = span_end - timedelta(minutes=args.time_span_minutes)
    sent = 0
    while args.count <= 0 or sent < args.count:
        event_time = None
        if args.count > 0 and args.time_span_minutes > 0:
            progress = sent / max(args.count - 1, 1)
            event_time = span_start + (span_end - span_start) * progress
            jitter = random.uniform(
                -args.out_of_order_seconds,
                args.out_of_order_seconds,
            )
            event_time = min(span_end, event_time + timedelta(seconds=jitter))
        event = build_event(
            args.event_time_offset,
            args.time_span_minutes,
            event_time=event_time,
        )
        producer.produce(
            args.topic,
            value=json.dumps(event, ensure_ascii=False, default=str).encode("utf-8"),
            callback=delivery_report,
        )
        if sent % 5000 == 0:
            print(f"  sent {sent} events...")
        sent += 1
        producer.poll(0)
        if args.interval > 0:
            time.sleep(args.interval)
    producer.flush()
    print(f"done, sent {sent} events.")


if __name__ == "__main__":
    main()
