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

logger.info("Gold Layer Validation Started")

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

print("=" * 80)
print("Gold Layer Validation")
print("=" * 80)

for table in gold_tables:

    df = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.{table}")

    print(f"\nTable : {table}")
    print(f"Row Count : {df.count()}")
    print(f"Columns : {len(df.columns)}")

    display(df.limit(5))

try:

    # Existing Silver notebook logic

    send_success_notification(
        layer="Silver",
        notebook="02_Gold_Validation",
        pipeline="Gold_Validation_Load"
    )

except Exception as e:

    logger.error(str(e))

    send_failure_notification(
        layer="Silver",
        notebook="02_Gold_Validation",
        pipeline="Gold_Validation_Load",
        error=str(e)
    )

    raise

logger.info("Gold Layer Validation Completed Successfully")