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

logger.info("Reading Bronze Sellers Table")

sellers_df = spark.table(
    f"{CATALOG}.{SCHEMA}.bronze_sellers"
)

display(sellers_df)

sellers_df = sellers_df.dropDuplicates(["seller_id"])

sellers_df = sellers_df.filter(
    col("seller_id").isNotNull()
)

sellers_df = sellers_df.fillna({
    "seller_city": "UNKNOWN",
    "seller_state": "UNKNOWN"
})

sellers_df = (
    sellers_df
    .withColumn(
        "seller_city",
        initcap(trim(col("seller_city")))
    )
    .withColumn(
        "seller_state",
        upper(trim(col("seller_state")))
    )
)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("seller_id")

sellers_df = sellers_df.withColumn(
    "seller_sk",
    row_number().over(window_spec)
)

sellers_df = (
    sellers_df
    .withColumn(
        "silver_load_timestamp",
        current_timestamp()
    )
    .withColumn(
        "silver_load_date",
        current_date()
    )
)

logger.info("Writing Silver Sellers Table")

sellers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sellers"
    )

logger.info("Silver Sellers Loaded Successfully")

print("=" * 60)
print("Silver Sellers Validation")
print("=" * 60)

print(f"Row Count : {sellers_df.count()}")

try:

    # Existing Silver notebook logic

    send_success_notification(
        layer="Silver",
        notebook="06_Silver_Sellers",
        pipeline="Silver_Sellers_Load"
    )

except Exception as e:

    logger.error(str(e))

    send_failure_notification(
        layer="Silver",
        notebook="06_Silver_Sellers",
        pipeline="Silver_Sellers_Load",
        error=str(e)
    )

    raise

display(
    spark.table(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sellers"
    )
)