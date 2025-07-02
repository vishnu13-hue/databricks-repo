from pyspark.sql.functions import *
import dlt

# ---------------------------------------
# 🥉 BRONZE LAYER: Auto Loader + Raw Ingest
# ---------------------------------------
@dlt.table(
  comment="Raw CDC transaction data loaded from JSON via Auto Loader"
)
@dlt.expect("valid_transaction_id", "transaction_id IS NOT NULL")
def dlt_bronze_transactions():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(spark.conf.get("input_path"))
            .withColumn("ingested_at", current_timestamp())
    )
# ---------------------------------------
# 🥈 SILVER LAYER: Filter + Transformation + SCD2 Fields
# ---------------------------------------
@dlt.table(
  comment="Cleaned transaction records with enrichment and SCD2 tracking"
)
@dlt.expect_or_drop("valid_status", "status IN ('insert', 'update', 'delete')")
@dlt.expect("positive_quantity", "quantity > 0")
@dlt.expect("positive_price", "price > 0")
def dlt_silver_transactions_scd2():
    df = dlt.read_stream("dlt_bronze_transactions")

    return (
      df.select(
            col("transaction_id").cast("string"),
            col("order_id").cast("string"),
            col("customer_id").cast("string"),
            col("product_id").cast("string"),
            col("status").cast("string"),
            col("quantity").cast("int"),
            col("price").cast("double"),
            col("event_ts").cast("timestamp"),
            col("payment_method").cast("string"),
            col("currency").cast("string"),
            col("location").cast("string"),
            col("tax").cast("double"),
            col("discount").cast("double")
        )
          .filter("status != 'delete'")
          .withColumn("is_active", lit(True))
          .withColumn("effective_start", col("event_ts"))
          .withColumn("effective_end", lit(None).cast("timestamp"))
    )

# ---------------------------------------
# 🥇 GOLD LAYER: Final Cleaned Data for Dashboard
# ---------------------------------------
@dlt.table(
  comment="Dashboard-ready transaction dataset with all essential fields"
)
def dlt_gold_transaction_summary():
    df = dlt.read("dlt_silver_transactions_scd2")

    return (
        df.filter("is_active = true AND status IN ('insert', 'update')")
          .withColumn("total_amount", col("quantity") * col("price"))
          .select(
              "transaction_id", "order_id", "customer_id", "product_id",
              "quantity", "price", "total_amount",
              "discount", "tax", "currency", "payment_method",
              "location", "event_ts", "status",
              "effective_start", "effective_end", "is_active"
          )
    )
