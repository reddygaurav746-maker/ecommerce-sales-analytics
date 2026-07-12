# Databricks notebook source
# MAGIC %run ../Common/01_Config

# COMMAND ----------

# MAGIC %run ../Common/03_Logger

# COMMAND ----------

# MAGIC %run ../Common/02_Utilities

# COMMAND ----------

# MAGIC %run ../Common/04_Slack_Notifications

# COMMAND ----------

# ==========================================================
# Project      : E-Commerce Sales Analytics Platform
# Notebook     : 01_Silver_Load
# Layer        : Silver
# ==========================================================

spark.sql(f"USE CATALOG {SILVER_CATALOG}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

bronze_customers = spark.table(
    f"{CATALOG}.{SCHEMA}.bronze_customers"
)

display(bronze_customers)

# Remove Duplicate Customers
customers_df = bronze_customers.dropDuplicates(["customer_id"])

# Handle Null Values
customers_df = customers_df.fillna({
    "customer_city": "Unknown",
    "customer_state": "Unknown"
})

# Remove Invalid Customer IDs
customers_df = customers_df.filter(
    col("customer_id").isNotNull()
)

# Standardization
customers_df = (

    customers_df

    .withColumn(
        "customer_city",
        initcap(col("customer_city"))
    )

    .withColumn(
        "customer_state",
        upper(col("customer_state"))
    )

)

# Generate Surrogate Key
from pyspark.sql.window import Window

window = Window.orderBy("customer_id")

customers_df = (

    customers_df

    .withColumn(
        "customer_sk",
        row_number().over(window)
    )

)

# SCD Type 2 Columns
customers_df = (

    customers_df

    .withColumn(
        "effective_date",
        current_timestamp()
    )

    .withColumn(
        "expiry_date",
        lit(None).cast("timestamp")
    )

    .withColumn(
        "is_current",
        lit(True)
    )

)

# Create Silver Customers Table
customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_customers"
    )

try:

    # Existing Silver notebook logic

    send_success_notification(
        layer="Silver",
        notebook="01_Silver_Customers",
        pipeline="Silver_Customers_Load"
    )

except Exception as e:

    logger.error(str(e))

    send_failure_notification(
        layer="Silver",
        notebook="01_Silver_Customers",
        pipeline="Silver_Customers_Load",
        error=str(e)
    )

    raise

# Validation
display(
    spark.table(
        f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_customers"
    )
)

print(customers_df.count())

