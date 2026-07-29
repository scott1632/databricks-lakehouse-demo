from lakehouse.bronze import events_to_bronze_df
from lakehouse.silver import clean_bronze, upsert_silver


def test_clean_bronze_dedups_to_latest_event_per_order(spark):
    events = [
        {
            "order_id": "o1",
            "customer_id": "c1",
            "sku": "SKU-1",
            "product_name": "Widget",
            "category": "Cat",
            "unit_price": 10.0,
            "quantity": 2,
            "status": "placed",
            "event_time": "2026-01-01T00:00:00+00:00",
            "ingested_at": "2026-01-01T00:00:00+00:00",
        },
        {
            "order_id": "o1",
            "customer_id": "c1",
            "sku": "SKU-1",
            "product_name": "Widget",
            "category": "Cat",
            "unit_price": 10.0,
            "quantity": 2,
            "status": "shipped",
            "event_time": "2026-01-01T01:00:00+00:00",
            "ingested_at": "2026-01-01T01:00:00+00:00",
        },
    ]
    bronze_df = events_to_bronze_df(spark, events)
    silver_df = clean_bronze(bronze_df)

    rows = silver_df.collect()
    assert len(rows) == 1
    assert rows[0]["status"] == "shipped"
    assert rows[0]["total_amount"] == 20.0


def test_clean_bronze_drops_invalid_rows(spark):
    events = [
        {
            "order_id": "o1",
            "customer_id": "c1",
            "sku": "SKU-1",
            "product_name": "Widget",
            "category": "Cat",
            "unit_price": 10.0,
            "quantity": 0,  # invalid: non-positive quantity
            "status": "placed",
            "event_time": "2026-01-01T00:00:00+00:00",
            "ingested_at": "2026-01-01T00:00:00+00:00",
        }
    ]
    bronze_df = events_to_bronze_df(spark, events)
    silver_df = clean_bronze(bronze_df)

    assert silver_df.count() == 0


def test_upsert_silver_merges_on_later_event_time(spark, tmp_table_name):
    initial_events = [
        {
            "order_id": "o1",
            "customer_id": "c1",
            "sku": "SKU-1",
            "product_name": "Widget",
            "category": "Cat",
            "unit_price": 10.0,
            "quantity": 1,
            "status": "placed",
            "event_time": "2026-01-01T00:00:00+00:00",
            "ingested_at": "2026-01-01T00:00:00+00:00",
        }
    ]
    silver_df = clean_bronze(events_to_bronze_df(spark, initial_events))
    upsert_silver(spark, silver_df, tmp_table_name)

    updated_events = [
        {
            "order_id": "o1",
            "customer_id": "c1",
            "sku": "SKU-1",
            "product_name": "Widget",
            "category": "Cat",
            "unit_price": 10.0,
            "quantity": 1,
            "status": "delivered",
            "event_time": "2026-01-02T00:00:00+00:00",
            "ingested_at": "2026-01-02T00:00:00+00:00",
        }
    ]
    updated_silver_df = clean_bronze(events_to_bronze_df(spark, updated_events))
    upsert_silver(spark, updated_silver_df, tmp_table_name)

    result = spark.table(tmp_table_name)
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["status"] == "delivered"
