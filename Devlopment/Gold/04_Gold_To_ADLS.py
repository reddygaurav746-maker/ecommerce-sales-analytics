# Databricks notebook source
# MAGIC %run ../Common/01_Config

# COMMAND ----------

# MAGIC %run ../Common/02_Utilities

# COMMAND ----------

# MAGIC %run ../Common/03_Logger

# COMMAND ----------

# MAGIC %run ../Common/04_Slack_Notifications

# COMMAND ----------

spark.sql(f"USE CATALOG {GOLD_CATALOG}")
spark.sql(f"USE SCHEMA {GOLD_SCHEMA}")

logger.info("Gold Layer Export to ADLS Started")

gold_tables = [
    "dim_customer",
    "dim_product",
    "dim_seller",
    "dim_date",
    "fact_sales",
    "daily_sales",
    "monthly_sales",
    "yearly_sales",
    "customer_summary",
    "order_summary",
    "seller_summary",
    "category_summary",
    "sales_summary",
    "customer_kpi",
    "product_kpi",
    "seller_kpi",
    "executive_dashboard"
]

for table in gold_tables:

    logger.info(f"Writing {table} to ADLS")

    df = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.{table}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{GOLD_BASE_PATH}{table}")
    )

    logger.info(f"{table} exported successfully.")

logger.info("All Gold Tables Successfully Written to ADLS")

try:

    # Existing Silver notebook logic

    send_success_notification(
        layer="Silver",
        notebook="04_Gold_To_ADLS",
        pipeline="Gold_To_ADLS_Load"
    )

except Exception as e:

    logger.error(str(e))

    send_failure_notification(
        layer="Silver",
        notebook="04_Gold_To_ADLS",
        pipeline="Gold_To_ADLS_Load",
        error=str(e)
    )

    raise