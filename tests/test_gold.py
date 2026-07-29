from lakehouse.bronze import events_to_bronze_df
from lakehouse.gold import customer_lifetime_value, daily_category_revenue
from lakehouse.silver import clean_bronze


def _silver_df(spark):
    events = [
        {
            "order_id": "o1",
            "customer_id": "c1",
            "sku": "SKU-1",
            "product_name": "Widget",
            "category": "Footwear",
            "unit_price": 100.0,
            "quantity": 1,
            "status": "delivered",
            "event_time": "2026-01-01T00:00:00+00:00",
            "ingested_at": "2026-01-01T00:00:00+00:00",
        },
        {
            "order_id": "o2",
            "customer_id": "c1",
            "sku": "SKU-2",
            "product_name": "Gadget",
            "category": "Footwear",
            "unit_price": 50.0,
            "quantity": 2,
            "status": "shipped",
            "event_time": "2026-01-01T05:00:00+00:00",
            "ingested_at": "2026-01-01T05:00:00+00:00",
        },
        {
            "order_id": "o3",
            "customer_id": "c2",
            "sku": "SKU-3",
            "product_name": "Gizmo",
            "category": "Camping",
            "unit_price": 30.0,
            "quantity": 1,
            "status": "cancelled",  # excluded from revenue
            "event_time": "2026-01-01T06:00:00+00:00",
            "ingested_at": "2026-01-01T06:00:00+00:00",
        },
    ]
    return clean_bronze(events_to_bronze_df(spark, events))


def test_daily_category_revenue_excludes_cancelled_and_sums_correctly(spark):
    result = daily_category_revenue(_silver_df(spark)).collect()

    assert len(result) == 1  # only the Footwear/2026-01-01 group qualifies
    row = result[0]
    assert row["category"] == "Footwear"
    assert row["revenue"] == 200.0  # 100*1 + 50*2
    assert row["order_count"] == 2


def test_customer_lifetime_value_aggregates_per_customer(spark):
    result = {r["customer_id"]: r for r in customer_lifetime_value(_silver_df(spark)).collect()}

    assert result["c1"]["lifetime_value"] == 200.0
    assert result["c1"]["total_orders"] == 2
    assert "c2" not in result  # cancelled-only customer excluded
