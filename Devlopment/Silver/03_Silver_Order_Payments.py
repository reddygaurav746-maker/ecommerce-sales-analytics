# Databricks notebook source
# MAGIC %run ../Common/01_Config
# MAGIC

# COMMAND ----------

# MAGIC %run ../Common/03_Logger
# MAGIC

# COMMAND ----------

# MAGIC %run ../Common/02_Utilities
# MAGIC

# COMMAND ----------

# MAGIC %run ../Common/04_Slack_Notifications

# COMMAND ----------

spark.sql(f"USE CATALOG {SILVER_CATALOG}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

logger.info("Reading Bronze Order Payments Table")

payments_df = spark.table(
    f"{CATALOG}.{SCHEMA}.bronze_order_payments"
)

display(payments_df)

payments_df = payments_df.dropDuplicates(
    ["order_id", "payment_sequential"]
)

payments_df = payments_df.filter(
    col("order_id").isNotNull()
)

payments_df = payments_df.fillna({
    "payment_type": "UNKNOWN",
    "payment_installments": 0,
    "payment_value": 0
})

payments_df = (
    payments_df
    .withColumn(
        "payment_type",
        upper(trim(col("payment_type")))
    )
    .withColumn(
        "payment_installments",
        col("payment_installments")
        .cast("double")
        .cast("int")
    )
    .withColumn(
        "payment_value",
        regexp_replace(
            col("payment_value"),
            ",",
            ""
        ).cast("double")
    )
)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy(
    "order_id",
    "payment_sequential"
)

payments_df = payments_df.withColumn(
    "payment_sk",
    row_number().over(window_spec)
)

payments_df = (
    payments_df
    .withColumn(
        "silver_load_timestamp",
        current_timestamp()
    )
    .withColumn(
        "silver_load_date",
        current_date()
    )
)

logger.info("Writing Silver Order Payments Table")

payments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_order_payments"
    )

logger.info("Silver Order Payments Loaded Successfully")

print("=" * 60)
print("Silver Order Payments Validation")
print("=" * 60)

print(f"Row Count : {payments_df.count()}")

try:

    # Existing Silver notebook logic

    send_success_notification(
        layer="Silver",
        notebook="03_Silver_Order_Payments",
        pipeline="Silver_Order_Payments_Load"
    )

except Exception as e:

    logger.error(str(e))

    send_failure_notification(
        layer="Silver",
        notebook="03_Silver_Order_Payments",
        pipeline="Silver__Order_Payments_Load",
        error=str(e)
    )

    raise

display(
    spark.table(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_order_payments"
    )
)