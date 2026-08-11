"""Generate staged MySQL order, payment, refund and SCD2 transactions."""

import argparse
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import mysql.connector


CHANNELS = ("app", "h5", "wechat", "search", "ad")
PAYMENT_METHODS = ("alipay", "wechat_pay", "bank_card")
PROMOTIONS = (None, None, None, "NEW10", "VIP20", "SUMMER")


@dataclass(frozen=True)
class Product:
    product_id: int
    price: Decimal


def build_order(order_index, created_at, products, rng=random):
    """Build one deterministic order header and one-to-three detail rows."""
    # Microseconds keep IDs unique when the repeatable demo is invoked twice
    # within the same wall-clock second.
    order_id = f"ORD{created_at:%Y%m%d%H%M%S%f}{order_index:06d}"
    chosen = rng.sample(products, rng.randint(1, min(3, len(products))))
    details = []
    order_amount = Decimal("0.00")
    for line_no, product in enumerate(chosen, start=1):
        quantity = rng.randint(1, 3)
        detail_amount = (product.price * quantity).quantize(Decimal("0.01"))
        order_amount += detail_amount
        details.append(
            {
                "detail_id": f"{order_id}-D{line_no}",
                "order_id": order_id,
                "product_id": product.product_id,
                "quantity": quantity,
                "unit_price": product.price,
                "detail_amount": detail_amount,
                "create_time": created_at,
                "update_time": created_at,
                "version_no": 1,
            }
        )
    header = {
        "order_id": order_id,
        "user_id": rng.randint(1, 50000),
        "shop_id": rng.randint(1, 20),
        "order_status": "CREATED",
        "channel": rng.choice(CHANNELS),
        "order_amount": order_amount,
        "promotion_code": rng.choice(PROMOTIONS),
        "create_time": created_at,
        "update_time": created_at,
        "version_no": 1,
    }
    return header, details


def fetch_products(cursor):
    cursor.execute("SELECT product_id, price FROM dim_product ORDER BY product_id")
    return [Product(int(product_id), Decimal(str(price))) for product_id, price in cursor]


def insert_created_order(cursor, header, details):
    cursor.execute(
        """
        INSERT INTO order_info (
          order_id, user_id, shop_id, order_status, channel, order_amount,
          promotion_code, create_time, update_time, version_no
        ) VALUES (%(order_id)s, %(user_id)s, %(shop_id)s, %(order_status)s,
                  %(channel)s, %(order_amount)s, %(promotion_code)s,
                  %(create_time)s, %(update_time)s, %(version_no)s)
        """,
        header,
    )
    cursor.executemany(
        """
        INSERT INTO order_detail (
          detail_id, order_id, product_id, quantity, unit_price, detail_amount,
          create_time, update_time, version_no
        ) VALUES (%(detail_id)s, %(order_id)s, %(product_id)s, %(quantity)s,
                  %(unit_price)s, %(detail_amount)s, %(create_time)s,
                  %(update_time)s, %(version_no)s)
        """,
        details,
    )


def advance_to_paid_or_cancelled(cursor, header, rng=random):
    """Apply the second lifecycle phase and return an optional refund plan."""
    order_id = header["order_id"]
    order_amount = header["order_amount"]
    created_at = header["create_time"]
    if rng.random() < 0.15:
        changed_at = created_at + timedelta(seconds=rng.randint(10, 120))
        cursor.execute(
            "UPDATE order_info SET order_status='CANCELLED', update_time=%s, version_no=2 WHERE order_id=%s",
            (changed_at, order_id),
        )
        return "CANCELLED", None

    paid_at = created_at + timedelta(seconds=rng.randint(5, 90))
    payment_id = f"PAY-{order_id}"
    cursor.execute(
        "UPDATE order_info SET order_status='PAID', update_time=%s, version_no=2 WHERE order_id=%s",
        (paid_at, order_id),
    )
    cursor.execute(
        """
        INSERT INTO payment_info (
          payment_id, order_id, payment_status, payment_method, payment_amount,
          payment_time, update_time, version_no
        ) VALUES (%s, %s, 'SUCCESS', %s, %s, %s, %s, 1)
        """,
        (payment_id, order_id, rng.choice(PAYMENT_METHODS), order_amount, paid_at, paid_at),
    )

    if rng.random() < 0.12:
        refund_at = paid_at + timedelta(seconds=rng.randint(30, 300))
        refund_amount = (order_amount * Decimal(str(rng.choice((0.25, 0.5, 1.0))))).quantize(
            Decimal("0.01")
        )
        return "PAID", {
            "refund_id": f"REF-{order_id}",
            "order_id": order_id,
            "payment_id": payment_id,
            "refund_amount": refund_amount,
            "refund_reason": rng.choice(
                ("customer_request", "quality_issue", "late_delivery")
            ),
            "refund_time": refund_at,
        }
    return "PAID", None


def apply_refund(cursor, plan):
    """Apply the third lifecycle phase in a transaction separate from payment."""
    cursor.execute(
        "UPDATE order_info SET order_status='REFUNDED', update_time=%s, version_no=3 WHERE order_id=%s",
        (plan["refund_time"], plan["order_id"]),
    )
    cursor.execute(
        """
        INSERT INTO refund_info (
          refund_id, order_id, payment_id, refund_status, refund_amount,
          refund_reason, refund_time, update_time, version_no
        ) VALUES (%s, %s, %s, 'SUCCESS', %s, %s, %s, %s, 1)
        """,
        (
            plan["refund_id"], plan["order_id"], plan["payment_id"],
            plan["refund_amount"], plan["refund_reason"],
            plan["refund_time"], plan["refund_time"],
        ),
    )


def commit_progress(connection, completed, total, phase):
    connection.commit()
    print(f"{phase}: committed {completed:,}/{total:,} orders")


def parse_args():
    parser = argparse.ArgumentParser(description="Generate staged ecommerce transactions.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3307)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="root")
    parser.add_argument("--database", default="ecommerce")
    parser.add_argument("--orders", type=int, default=500)
    parser.add_argument("--seed", type=int, default=202608)
    parser.add_argument("--commit-interval", type=int, default=25)
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument(
        "--phase-delay",
        type=float,
        default=0.5,
        help="Pause between committed CREATED, payment/cancellation and refund phases.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.orders < 0:
        raise ValueError("--orders must be non-negative")
    if args.commit_interval <= 0:
        raise ValueError("--commit-interval must be positive")
    if args.phase_delay < 0:
        raise ValueError("--phase-delay must be non-negative")

    rng = random.Random(args.seed)
    connection = mysql.connector.connect(
        host=args.host, port=args.port, user=args.user, password=args.password,
        database=args.database, autocommit=False,
    )
    cursor = connection.cursor()
    products = fetch_products(cursor)
    started = time.perf_counter()
    counts = {"CREATED": 0, "PAID": 0, "CANCELLED": 0, "REFUNDED": 0}
    price_change_index = args.orders // 2 if args.orders >= 2 else None
    headers = []
    refund_plans = []

    try:
        # Phase 1: publish durable CREATED rows first. Lifecycle updates are not
        # allowed into these transactions, so CDC can observe the initial state.
        for index in range(args.orders):
            if index == price_change_index:
                if index % args.commit_interval:
                    commit_progress(connection, index, args.orders, "created")
                # Separate the SCD2 boundary from the previous millisecond so
                # old-price rows cannot be truncated onto the new validity range.
                time.sleep(0.005)
                price_change_at = datetime.now()
                for product_id in (1001, 1021, 1076):
                    product = next(item for item in products if item.product_id == product_id)
                    new_price = (product.price * Decimal("1.08")).quantize(Decimal("0.01"))
                    cursor.callproc("sp_change_product_price", (product_id, new_price, price_change_at))
                connection.commit()
                products = fetch_products(cursor)

            # Use actual insertion time on every invocation. Backdating rows
            # with today's current price would select an older SCD2 version on
            # repeat runs and create a false historical-price mismatch.
            created_at = datetime.now()

            header, details = build_order(index + 1, created_at, products, rng)
            insert_created_order(cursor, header, details)
            headers.append(header)
            counts["CREATED"] += 1

            if (index + 1) % args.commit_interval == 0:
                commit_progress(connection, index + 1, args.orders, "created")
            if args.sleep > 0:
                time.sleep(args.sleep)
        connection.commit()

        if headers and args.phase_delay:
            time.sleep(args.phase_delay)

        # Phase 2: payment or cancellation is committed independently from
        # creation. Refund decisions are planned now but executed in phase 3.
        for index, header in enumerate(headers, start=1):
            status, refund_plan = advance_to_paid_or_cancelled(cursor, header, rng)
            if status == "CANCELLED":
                counts["CANCELLED"] += 1
            elif refund_plan is None:
                counts["PAID"] += 1
            else:
                refund_plans.append(refund_plan)
            if index % args.commit_interval == 0:
                commit_progress(connection, index, len(headers), "paid-or-cancelled")
        connection.commit()

        if refund_plans and args.phase_delay:
            time.sleep(args.phase_delay)

        # Phase 3: refunds have their own commit boundary, so downstream sees
        # CREATED -> PAID -> REFUNDED rather than one collapsed source commit.
        for index, plan in enumerate(refund_plans, start=1):
            apply_refund(cursor, plan)
            counts["REFUNDED"] += 1
            if index % args.commit_interval == 0:
                commit_progress(connection, index, len(refund_plans), "refunded")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

    elapsed = max(time.perf_counter() - started, 0.001)
    print(
        f"done: orders={args.orders:,}, paid={counts['PAID']:,}, "
        f"cancelled={counts['CANCELLED']:,}, refunded={counts['REFUNDED']:,}, "
        f"rate={args.orders / elapsed:,.0f} orders/s"
    )


if __name__ == "__main__":
    main()
