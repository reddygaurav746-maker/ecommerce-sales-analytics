# Databricks notebook source
# MAGIC %run ../Common/01_Config

# COMMAND ----------

# MAGIC %run ../Common/03_Logger

# COMMAND ----------

# MAGIC %run ../Common/02_Utilities

# COMMAND ----------

spark.sql(f"USE CATALOG {SILVER_CATALOG}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

# OPTIMIZE
tables = [
    "silver_customers",
    "silver_orders",
    "silver_order_payments",
    "silver_products",
    "silver_product_categories",
    "silver_sellers",
    "silver_sales"
]

for table in tables:
    spark.sql(f"OPTIMIZE {SILVER_CATALOG}.{SILVER_SCHEMA}.{table}")
    print(f"{table} Optimized Successfully")

# ZORDER
spark.sql(f"""
OPTIMIZE {SILVER_CATALOG}.{SILVER_SCHEMA}.silver_orders
ZORDER BY (order_id)
""")

spark.sql(f"""
OPTIMIZE {SILVER_CATALOG}.{SILVER_SCHEMA}.silver_customers
ZORDER BY (customer_id)
""")

spark.sql(f"""
OPTIMIZE {SILVER_CATALOG}.{SILVER_SCHEMA}.silver_products
ZORDER BY (product_id)
""")

# VACUUM
for table in tables:
    spark.sql(f"""
    VACUUM {SILVER_CATALOG}.{SILVER_SCHEMA}.{table}
    RETAIN 168 HOURS
    """)
    print(f"{table} Vacuum Completed")

# TABLE HISTORY
display(
    spark.sql(f"""
    DESCRIBE HISTORY
    {SILVER_CATALOG}.{SILVER_SCHEMA}.silver_orders
    """)
)

# TIME TRAVEL(VERSION)
display(
    spark.sql(f"""
    SELECT *
    FROM {SILVER_CATALOG}.{SILVER_SCHEMA}.silver_orders
    VERSION AS OF 0
    """)
)

# TIME TRAVEL(TIMESTAMP)
display(
    spark.sql(f"""
    SELECT *
    FROM {SILVER_CATALOG}.{SILVER_SCHEMA}.silver_orders
    VERSION AS OF 0
    """)
)

