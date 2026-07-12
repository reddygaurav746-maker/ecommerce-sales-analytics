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

from pyspark.sql.functions import col, upper, to_timestamp, current_timestamp, current_date

spark.sql(f"USE CATALOG {SILVER_CATALOG}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

logger.info("Reading Bronze Orders Table")

bronze_orders = spark.table(
    f"{CATALOG}.{SCHEMA}.bronze_orders"
)

display(bronze_orders)

logger.info("Removing Duplicate Records")

orders_df = bronze_orders.dropDuplicates(["order_id"])

logger.info("Removing Invalid Records")

orders_df = orders_df.filter(
    col("order_id").isNotNull()
)

logger.info("Handling Null Values")

orders_df = orders_df.fillna({
    "order_status": "Unknown"
})

orders_df = orders_df.replace("not_available", None)

logger.info("Standardizing Data")

orders_df = (
    orders_df
    .withColumn(
        "order_status",
        upper(col("order_status"))
    )
    .withColumn(
        "order_purchase_timestamp",
        to_timestamp(col("order_purchase_timestamp"))
    )
    .withColumn(
        "order_approved_at",
        to_timestamp(col("order_approved_at"))
    )
    .withColumn(
        "order_delivered_carrier_date",
        to_timestamp(col("order_delivered_carrier_date"))
    )
    .withColumn(
        "order_delivered_customer_date",
        to_timestamp(col("order_delivered_customer_date"))
    )
    .withColumn(
        "order_estimated_delivery_date",
        to_timestamp(col("order_estimated_delivery_date"))
    )
)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("order_id")

orders_df = orders_df.withColumn(
    "order_sk",
    row_number().over(window_spec)
)

orders_df = (
    orders_df
    .withColumn(
        "silver_load_timestamp",
        current_timestamp()
    )
    .withColumn(
        "silver_load_date",
        current_date()
    )
)

logger.info("Writing Silver Orders Table")

orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_orders"
    )

logger.info("Silver Orders Loaded Successfully")

print("=" * 60)
print("Silver Orders Validation")
print("=" * 60)

print(f"Row Count : {orders_df.count()}")

try:

    # Existing Silver notebook logic

    send_success_notification(
        layer="Silver",
        notebook="02_Silver_Orders",
        pipeline="Silver_Orders_Load"
    )

except Exception as e:

    logger.error(str(e))

    send_failure_notification(
        layer="Silver",
        notebook="02_Silver_Orders",
        pipeline="Silver_Orders_Load",
        error=str(e)
    )

    raise

display(
    spark.table(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_orders"
    )
)
