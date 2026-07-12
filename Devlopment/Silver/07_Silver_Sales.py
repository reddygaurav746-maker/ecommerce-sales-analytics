# Databricks notebook source
# MAGIC %run ../Common/01_Config

# COMMAND ----------

# MAGIC %run ../Common/03_Logger

# COMMAND ----------

# MAGIC %run ../Common/02_Utilities

# COMMAND ----------

# MAGIC %run ../Common/04_Slack_Notifications

# COMMAND ----------

spark.sql(f"USE CATALOG {SILVER_CATALOG}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

customers = spark.table(
    f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_customers"
)

orders = spark.table(
    f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_orders"
)

payments = spark.table(
    f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_order_payments"
)

sales_df = (
    orders.alias("o")
    .join(
        customers.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    )
    .join(
        payments.alias("p"),
        col("o.order_id") == col("p.order_id"),
        "left"
    )
    .select(
        col("o.order_sk"),
        col("c.customer_sk"),
        col("p.payment_sk"),
        col("o.order_id"),
        col("o.customer_id"),
        col("o.order_status"),
        col("o.order_purchase_timestamp"),
        col("p.payment_type"),
        col("p.payment_installments"),
        col("p.payment_value")
    )
)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("order_id")

sales_df = sales_df.withColumn(
    "sales_sk",
    row_number().over(window_spec)
)

sales_df = (
    sales_df
    .withColumn("silver_load_timestamp", current_timestamp())
    .withColumn("silver_load_date", current_date())
)

sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sales"
    )

print(f"Row Count : {sales_df.count()}")

try:

    # Existing Silver notebook logic

    send_success_notification(
        layer="Silver",
        notebook="07_Silver_Sales",
        pipeline="Silver_Sales_Load"
    )

except Exception as e:

    logger.error(str(e))

    send_failure_notification(
        layer="Silver",
        notebook="07_Silver_Sales",
        pipeline="Silver_Sales_Load",
        error=str(e)
    )

    raise

display(
    spark.table(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sales"
    )
)