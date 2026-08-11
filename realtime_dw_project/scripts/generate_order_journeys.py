"""Generate traceable ecommerce journeys for strict funnel computation."""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta

from confluent_kafka import Producer


EVENT_STAGES = ("view", "cart", "order", "pay")
CHANNELS = ("app", "h5", "wechat", "search", "ad")
PRODUCT_IDS = tuple(range(1001, 1101))
SHOP_IDS = tuple(range(1, 21))
USER_ID_RANGE = (1, 50000)


def _event(
    *,
    session_id,
    order_id,
    sequence,
    user_id,
    product_id,
    shop_id,
    channel,
    event_type,
    event_time,
    amount,
):
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "order_id": order_id,
        "event_sequence": sequence,
        "event_version": 1,
        "user_id": user_id,
        "product_id": product_id,
        "shop_id": shop_id,
        "event_type": event_type,
        "channel": channel,
        "amount": amount if event_type in ("order", "pay") else 0.0,
        "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def build_journey(start_time, rng=random):
    """Return one ordered view->cart->order->pay journey with realistic drop-off."""
    session_id = str(uuid.uuid4())
    order_id = str(uuid.uuid4())
    user_id = rng.randint(*USER_ID_RANGE)
    product_id = rng.choice(PRODUCT_IDS)
    shop_id = rng.choice(SHOP_IDS)
    channel = rng.choice(CHANNELS)
    amount = round(rng.uniform(29, 799), 2)

    stages = ["view"]
    if rng.random() < 0.62:
        stages.append("cart")
        if rng.random() < 0.58:
            stages.append("order")
            if rng.random() < 0.76:
                stages.append("pay")

    events = []
    current_time = start_time
    for sequence, stage in enumerate(stages, start=1):
        if sequence > 1:
            current_time += timedelta(seconds=rng.randint(1, 20))
        events.append(
            _event(
                session_id=session_id,
                order_id=order_id if stage in ("order", "pay") else None,
                sequence=sequence,
                user_id=user_id,
                product_id=product_id,
                shop_id=shop_id,
                channel=channel,
                event_type=stage,
                event_time=current_time,
                amount=amount,
            )
        )
    return events


def inject_business_dirty_event(events, rng=random):
    if not events:
        return
    event = rng.choice(events)
    dirty_type = rng.choice(("empty_event_id", "empty_user_id", "invalid_type"))
    if dirty_type == "empty_event_id":
        event["event_id"] = None
    elif dirty_type == "empty_user_id":
        event["user_id"] = None
    else:
        event["event_type"] = "unknown"


def bounded_disorder(events, max_lateness_seconds, rng=random):
    """Sort by event time then shuffle only within a bounded time bucket."""
    if max_lateness_seconds <= 0:
        return sorted(events, key=lambda row: row["event_time"])
    buckets = {}
    for event in events:
        ts = datetime.strptime(event["event_time"], "%Y-%m-%d %H:%M:%S")
        bucket = int(ts.timestamp()) // max_lateness_seconds
        buckets.setdefault(bucket, []).append(event)
    result = []
    for bucket in sorted(buckets):
        rows = buckets[bucket]
        rng.shuffle(rows)
        result.extend(rows)
    return result


def parse_args():
    parser = argparse.ArgumentParser(description="Generate traceable order journeys.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="ods_user_behavior")
    parser.add_argument("--journeys", type=int, default=30000)
    parser.add_argument("--time-span-minutes", type=int, default=120)
    parser.add_argument("--out-of-order-seconds", type=int, default=3)
    parser.add_argument("--dirty-rate", type=float, default=0.02)
    parser.add_argument("--interval", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=202608)
    return parser.parse_args()


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    span_end = datetime.now()
    span_start = span_end - timedelta(minutes=args.time_span_minutes)
    events = []
    for index in range(args.journeys):
        progress = index / max(args.journeys - 1, 1)
        journey_start = span_start + (span_end - span_start) * progress
        journey = build_journey(journey_start, rng)
        if rng.random() < args.dirty_rate:
            inject_business_dirty_event(journey, rng)
        events.extend(journey)

    events = bounded_disorder(events, args.out_of_order_seconds, rng)
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
        }
    )
    started = time.perf_counter()
    for index, event in enumerate(events, start=1):
        producer.produce(
            args.topic,
            key=(event.get("session_id") or "dirty").encode("utf-8"),
            value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
        )
        producer.poll(0)
        if index % 10000 == 0:
            print(f"sent {index:,}/{len(events):,} events")
        if args.interval > 0:
            time.sleep(args.interval)
    producer.flush()
    elapsed = max(time.perf_counter() - started, 0.001)
    print(
        f"done: journeys={args.journeys:,}, events={len(events):,}, "
        f"producer_rate={len(events) / elapsed:,.0f} events/s"
    )


if __name__ == "__main__":
    main()
