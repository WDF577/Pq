import importlib.util
import random
from datetime import datetime
from decimal import Decimal
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "generate_order_transactions.py"
SPEC = importlib.util.spec_from_file_location("order_transactions", MODULE_PATH)
transactions = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(transactions)


def test_build_order_balances_header_and_details():
    products = [
        transactions.Product(1001, Decimal("10.00")),
        transactions.Product(1002, Decimal("20.00")),
        transactions.Product(1003, Decimal("30.00")),
    ]
    header, details = transactions.build_order(
        7, datetime(2026, 8, 11, 12, 0, 0), products, random.Random(9)
    )
    assert header["order_status"] == "CREATED"
    assert 1 <= len(details) <= 3
    assert {row["order_id"] for row in details} == {header["order_id"]}
    assert sum(row["detail_amount"] for row in details) == header["order_amount"]
    assert all(row["detail_amount"] == row["unit_price"] * row["quantity"] for row in details)


def test_order_ids_are_stable_and_unique_by_index():
    products = [transactions.Product(1001, Decimal("10.00"))]
    created_at = datetime(2026, 8, 11, 12, 0, 0)
    first, _ = transactions.build_order(1, created_at, products, random.Random(1))
    second, _ = transactions.build_order(2, created_at, products, random.Random(1))
    assert first["order_id"] != second["order_id"]
    assert first["order_id"].startswith("ORD20260811120000")


class RecordingCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((" ".join(sql.split()), params))


class RefundRng:
    def __init__(self):
        self.random_values = iter((0.5, 0.01))

    def random(self):
        return next(self.random_values)

    def randint(self, lower, _upper):
        return lower

    def choice(self, values):
        return values[0]


def test_refund_is_planned_but_not_written_in_payment_phase():
    cursor = RecordingCursor()
    header = {
        "order_id": "ORD-1",
        "order_amount": Decimal("100.00"),
        "create_time": datetime(2026, 8, 11, 12, 0, 0),
    }

    status, refund_plan = transactions.advance_to_paid_or_cancelled(
        cursor, header, RefundRng()
    )

    assert status == "PAID"
    assert refund_plan["refund_amount"] == Decimal("25.00")
    assert any("order_status='PAID'" in sql for sql, _ in cursor.calls)
    assert any("INSERT INTO payment_info" in sql for sql, _ in cursor.calls)
    assert all("refund_info" not in sql for sql, _ in cursor.calls)

    transactions.apply_refund(cursor, refund_plan)
    assert any("order_status='REFUNDED'" in sql for sql, _ in cursor.calls)
    assert any("INSERT INTO refund_info" in sql for sql, _ in cursor.calls)
