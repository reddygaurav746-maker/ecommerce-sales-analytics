# Databricks notebook source
# MAGIC %run ../Common/01_Config

# COMMAND ----------



print("=" * 60)
print("Bronze Layer Validation")
print("=" * 60)

for dataset in datasets:

    table = dataset["table"]

    df = spark.table(f"{CATALOG}.{SCHEMA}.{table}")

    print(f"\nTable : {table}")
    print(f"Row Count : {df.count()}")
    print(f"Columns   : {len(df.columns)}")

display(
    spark.table(f"{CATALOG}.{SCHEMA}.bronze_audit_log")
)

display(
    spark.table(f"{CATALOG}.{SCHEMA}.bronze_error_log")
)

display(
    spark.table(f"{CATALOG}.{SCHEMA}.bronze_watermark")
)
