from lakehouse.bronze import events_to_bronze_df, write_bronze
from lakehouse.data_generator import generate_orders


def test_events_to_bronze_df_types_and_row_count(spark):
    events = generate_orders(n=50, seed=42)
    df = events_to_bronze_df(spark, events)

    assert df.count() == len(events)
    assert dict(df.dtypes)["event_time"] == "timestamp"
    assert dict(df.dtypes)["unit_price"] == "decimal(10,2)"


def test_write_bronze_appends_across_batches(spark, tmp_table_name):
    first_batch = events_to_bronze_df(spark, generate_orders(n=20, seed=1))
    write_bronze(first_batch, tmp_table_name)

    second_batch = events_to_bronze_df(spark, generate_orders(n=20, seed=2))
    write_bronze(second_batch, tmp_table_name)

    result = spark.table(tmp_table_name)
    assert result.count() == first_batch.count() + second_batch.count()


def test_generate_orders_produces_duplicate_replays():
    events = generate_orders(n=200, duplicate_rate=1.0, seed=7)
    order_ids = [e["order_id"] for e in events]
    assert len(order_ids) > 200
    assert len(set(order_ids)) < len(order_ids)
