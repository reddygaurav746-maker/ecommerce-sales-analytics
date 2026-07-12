# Databricks notebook source
# MAGIC %run ../Common/01_Config

# COMMAND ----------

spark.sql(f"USE CATALOG {SILVER_CATALOG}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

tables = [
    "silver_customers",
    "silver_orders",
    "silver_order_payments",
    "silver_products",
    "silver_product_categories",
    "silver_sellers",
    "silver_sales"
]

print("=" * 70)
print("Silver Layer Validation")
print("=" * 70)

for table in tables:

    df = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{table}")

    print(f"\nTable : {table}")
    print(f"Rows   : {df.count()}")
    print(f"Columns: {len(df.columns)}")

    display(df.limit(5))