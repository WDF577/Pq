import importlib.util
from datetime import datetime
from pathlib import Path
import random


MODULE_PATH = Path(__file__).parents[1] / "scripts" / "generate_order_journeys.py"
SPEC = importlib.util.spec_from_file_location("journeys", MODULE_PATH)
journeys = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(journeys)


def test_journey_has_ordered_prefix_and_consistent_ids():
    rows = journeys.build_journey(datetime(2026, 8, 1, 12, 0, 0), random.Random(7))
    stages = [row["event_type"] for row in rows]
    assert stages == list(journeys.EVENT_STAGES[: len(stages)])
    assert [row["event_sequence"] for row in rows] == list(range(1, len(rows) + 1))
    assert len({row["session_id"] for row in rows}) == 1
    assert all(row["order_id"] is None for row in rows if row["event_type"] in ("view", "cart"))
    order_ids = {row["order_id"] for row in rows if row["event_type"] in ("order", "pay")}
    assert len(order_ids) <= 1


def test_bounded_disorder_preserves_all_events():
    rng = random.Random(9)
    rows = []
    base = datetime(2026, 8, 1, 12, 0, 0)
    for _ in range(20):
        rows.extend(journeys.build_journey(base, rng))
    shuffled = journeys.bounded_disorder(rows, 3, rng)
    assert {row["event_id"] for row in shuffled} == {row["event_id"] for row in rows}
    assert len(shuffled) == len(rows)
