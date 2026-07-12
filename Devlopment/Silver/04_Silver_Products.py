# Databricks notebook source
# MAGIC %run ../Common/01_Config

# COMMAND ----------

# MAGIC %run ../Common/03_Logger

# COMMAND ----------

# MAGIC %run ../Common/02_Utilities

# COMMAND ----------

# MAGIC %run ../Common/04_Slack_Notifications

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark.sql(f"USE CATALOG {SILVER_CATALOG}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

logger.info("Reading Bronze Products Table")

products_df = spark.table(
    f"{CATALOG}.{SCHEMA}.bronze_products"
)

display(products_df)

products_df = products_df.dropDuplicates(["product_id"])

products_df = products_df.filter(
    col("product_id").isNotNull()
)

products_df = products_df.fillna({
    "product_category_name": "UNKNOWN",
    "product_name_lenght": "0",
    "product_description_lenght": "0",
    "product_photos_qty": "0",
    "product_weight_g": "0",
    "product_length_cm": "0",
    "product_height_cm": "0",
    "product_width_cm": "0"
})

products_df = (
    products_df
    .withColumn(
        "product_category_name",
        upper(trim(col("product_category_name")))
    )
    .withColumn(
        "product_name_lenght",
        regexp_replace(
            col("product_name_lenght").cast("string"),
            "\\.0$",
            ""
        ).cast("int")
    )
    .withColumn(
        "product_description_lenght",
        regexp_replace(
            col("product_description_lenght").cast("string"),
            "\\.0$",
            ""
        ).cast("int")
    )
    .withColumn(
        "product_photos_qty",
        regexp_replace(
            col("product_photos_qty").cast("string"),
            "\\.0$",
            ""
        ).cast("int")
    )
    .withColumn(
        "product_weight_g",
        regexp_replace(
            col("product_weight_g").cast("string"),
            ",",
            ""
        ).cast("double")
    )
    .withColumn(
        "product_length_cm",
        regexp_replace(
            col("product_length_cm").cast("string"),
            ",",
            ""
        ).cast("double")
    )
    .withColumn(
        "product_height_cm",
        regexp_replace(
            col("product_height_cm").cast("string"),
            ",",
            ""
        ).cast("double")
    )
    .withColumn(
        "product_width_cm",
        regexp_replace(
            col("product_width_cm").cast("string"),
            ",",
            ""
        ).cast("double")
    )
)

window_spec = Window.orderBy("product_id")

products_df = products_df.withColumn(
    "product_sk",
    row_number().over(window_spec)
)

products_df = (
    products_df
    .withColumn(
        "silver_load_timestamp",
        current_timestamp()
    )
    .withColumn(
        "silver_load_date",
        current_date()
    )
)

logger.info("Writing Silver Products Table")

products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_products"
    )

logger.info("Silver Products Loaded Successfully")

print("=" * 60)
print("Silver Products Validation")
print("=" * 60)

print(f"Row Count : {products_df.count()}")

try:

    # Existing Silver notebook logic

    send_success_notification(
        layer="Silver",
        notebook="04_Silver_Products",
        pipeline="Silver_Products_Load"
    )

except Exception as e:

    logger.error(str(e))

    send_failure_notification(
        layer="Silver",
        notebook="01_Silver_Products",
        pipeline="Silver_Products_Load",
        error=str(e)
    )

    raise

display(
    spark.table(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_products"
    )
)