# Databricks notebook source
# MAGIC %run ../Common/01_Config

# COMMAND ----------

# MAGIC %run ../Common/03_Logger

# COMMAND ----------

# MAGIC %run ../Common/02_Utilities

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *

spark.sql(f"USE CATALOG {SILVER_CATALOG}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

source_df = (
    spark.table(f"{CATALOG}.{SCHEMA}.bronze_customers")
    .dropDuplicates(["customer_id"])
    .withColumn("customer_city", initcap(trim(col("customer_city"))))
    .withColumn("customer_state", upper(trim(col("customer_state"))))
)

target_table = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_customers"

delta_table = DeltaTable.forName(spark, target_table)

(
    delta_table.alias("target")
    .merge(
        source_df.alias("source"),
        """
        target.customer_id = source.customer_id
        AND target.is_current = true
        """
    )
    .whenMatchedUpdate(
        condition="""
        target.customer_city <> source.customer_city
        OR target.customer_state <> source.customer_state
        """,
        set={
            "is_current": "false",
            "expiry_date": "current_timestamp()"
        }
    )
    .whenNotMatchedInsert(
        values={
            "customer_id": "source.customer_id",
            "customer_unique_id": "source.customer_unique_id",
            "customer_zip_code_prefix": "source.customer_zip_code_prefix",
            "customer_city": "source.customer_city",
            "customer_state": "source.customer_state",
            "random_val": "source.random_val",
            "bronze_load_timestamp": "source.bronze_load_timestamp",
            "bronze_source_file": "source.bronze_source_file",
            "bronze_load_date": "source.bronze_load_date",
            "customer_sk": "NULL",
            "effective_date": "current_timestamp()",
            "expiry_date": "NULL",
            "is_current": "true"
        }
    )
    .execute()
)

display(
    spark.table(target_table)
)