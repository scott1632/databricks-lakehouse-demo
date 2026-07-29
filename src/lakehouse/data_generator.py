"""Synthetic e-commerce order event generator.

Produces the same shape of data a real storefront's event stream would emit,
so the pipeline is fully self-contained: no external API or dataset download
is required to run the demo end to end.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

PRODUCTS = [
    ("SKU-1001", "Trail Running Shoes", "Footwear", 129.99),
    ("SKU-1002", "Insulated Water Bottle", "Accessories", 24.50),
    ("SKU-1003", "Packable Rain Jacket", "Apparel", 89.00),
    ("SKU-1004", "Merino Wool Socks", "Apparel", 18.75),
    ("SKU-1005", "Ultralight Tent", "Camping", 349.00),
    ("SKU-1006", "Trekking Poles", "Camping", 64.99),
    ("SKU-1007", "Headlamp", "Electronics", 39.99),
    ("SKU-1008", "Sunglasses", "Accessories", 54.00),
]

STATUSES = ["placed", "confirmed", "shipped", "delivered", "cancelled"]
_STATUS_WEIGHTS = [0.35, 0.25, 0.2, 0.15, 0.05]


def _random_timestamp(start: datetime, end: datetime) -> datetime:
    delta_seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta_seconds))


def generate_orders(
    n: int,
    start: datetime | None = None,
    end: datetime | None = None,
    duplicate_rate: float = 0.05,
    seed: int | None = None,
) -> list[dict[str, Any]]:
    """Generate `n` synthetic order events (bronze layer raw shape).

    A small fraction of events are duplicated with a later `event_time` to
    simulate at-least-once delivery from an upstream event bus, which is what
    the silver layer's dedup/upsert logic is exercised against.
    """
    if seed is not None:
        random.seed(seed)

    end = end or datetime.now(timezone.utc)
    start = start or (end - timedelta(days=7))

    events: list[dict[str, Any]] = []
    for _ in range(n):
        order_id = str(uuid.uuid4())
        sku, name, category, price = random.choice(PRODUCTS)
        quantity = random.randint(1, 4)
        event_time = _random_timestamp(start, end)

        event = {
            "order_id": order_id,
            "customer_id": f"CUST-{random.randint(1, 500):04d}",
            "sku": sku,
            "product_name": name,
            "category": category,
            "unit_price": price,
            "quantity": quantity,
            "status": random.choices(STATUSES, weights=_STATUS_WEIGHTS)[0],
            "event_time": event_time.isoformat(),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
        events.append(event)

        if random.random() < duplicate_rate:
            replay = dict(event)
            replay["event_time"] = (event_time + timedelta(minutes=random.randint(1, 30))).isoformat()
            replay["status"] = random.choices(STATUSES, weights=_STATUS_WEIGHTS)[0]
            replay["ingested_at"] = datetime.now(timezone.utc).isoformat()
            events.append(replay)

    return events
