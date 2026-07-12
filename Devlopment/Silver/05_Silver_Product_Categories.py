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

logger.info("Reading Bronze Product Categories Table")

categories_df = spark.table(
    f"{CATALOG}.{SCHEMA}.bronze_product_categories"
)

display(categories_df)

categories_df = categories_df.dropDuplicates(
    ["product_category_name"]
)

categories_df = categories_df.filter(
    col("product_category_name").isNotNull()
)

categories_df = categories_df.fillna({
    "product_category_name_english": "UNKNOWN"
})

categories_df = (
    categories_df
    .withColumn(
        "product_category_name",
        upper(trim(col("product_category_name")))
    )
    .withColumn(
        "product_category_name_english",
        initcap(trim(col("product_category_name_english")))
    )
)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("product_category_name")

categories_df = categories_df.withColumn(
    "category_sk",
    row_number().over(window_spec)
)

categories_df = (
    categories_df
    .withColumn(
        "silver_load_timestamp",
        current_timestamp()
    )
    .withColumn(
        "silver_load_date",
        current_date()
    )
)

logger.info("Writing Silver Product Categories Table")

categories_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_product_categories"
    )

logger.info("Silver Product Categories Loaded Successfully")

print("=" * 60)
print("Silver Product Categories Validation")
print("=" * 60)

print(f"Row Count : {categories_df.count()}")

try:

    # Existing Silver notebook logic

    send_success_notification(
        layer="Silver",
        notebook="05_Silver_Product_Categories",
        pipeline="Silver_Products_Categories_Load"
    )

except Exception as e:

    logger.error(str(e))

    send_failure_notification(
        layer="Silver",
        notebook="05_Silver_Product_Categories",
        pipeline="Silver_Product_Categories_Load",
        error=str(e)
    )

    raise

display(
    spark.table(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_product_categories"
    )
)