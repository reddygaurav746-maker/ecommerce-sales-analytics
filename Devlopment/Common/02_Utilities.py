# Databricks notebook source
# ==========================================================
# Project      : E-Commerce Sales Analytics Platform
# Notebook     : 02_Utilities
# Description  : Common Utility Functions
# Author       : Sakshi Bejagmwar
# ==========================================================

# Load Configuration
%run /Workspace/E-Commerce-ETL/Common/01_Config.ipynb
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime

# ==========================================================
# Create Metadata Tables
# ==========================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_catalog.bronze_sch.bronze_audit_log(
execution_id STRING,
pipeline_name STRING,
table_name STRING,
file_name STRING,
start_time TIMESTAMP,
end_time TIMESTAMP,
rows_read BIGINT,
rows_loaded BIGINT,
status STRING,
error_message STRING
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_catalog.bronze_sch.bronze_error_log(
execution_id STRING,
notebook_name STRING,
table_name STRING,
error_type STRING,
error_message STRING,
error_timestamp TIMESTAMP
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_catalog.bronze_sch.bronze_watermark(
table_name STRING,
last_load_time TIMESTAMP,
last_modified_time TIMESTAMP
)
USING DELTA
""")

# ==========================================================
# Audit Schema
# ==========================================================

audit_schema = StructType([
    StructField("execution_id", StringType(), True),
    StructField("pipeline_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("rows_read", LongType(), True),
    StructField("rows_loaded", LongType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True)
])

# ==========================================================
# Error Schema
# ==========================================================

error_schema = StructType([
    StructField("execution_id", StringType(), True),
    StructField("notebook_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("error_type", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("error_timestamp", TimestampType(), True)
])

# ==========================================================
# Audit Function
# ==========================================================

def write_audit(
    table_name,
    file_name,
    start_time,
    end_time,
    rows_read,
    rows_loaded,
    status,
    error_message=""
):

    audit_df = spark.createDataFrame(
        [(
            EXECUTION_ID,
            PIPELINE_NAME,
            table_name,
            file_name,
            start_time,
            end_time,
            rows_read,
            rows_loaded,
            status,
            error_message
        )],
        schema=audit_schema
    )

    audit_df.write.mode("append").saveAsTable(
        f"{CATALOG}.{SCHEMA}.bronze_audit_log"
    )

    # ==========================================================
# Error Function
# ==========================================================

def write_error(table_name, exception):

    error_df = spark.createDataFrame(
        [(
            EXECUTION_ID,
            NOTEBOOK_NAME,
            table_name,
            type(exception).__name__,
            str(exception),
            datetime.now()
        )],
        schema=error_schema
    )

    error_df.write.mode("append").saveAsTable(
        f"{CATALOG}.{SCHEMA}.bronze_error_log"
    )

    # ==========================================================
# Watermark Function
# ==========================================================

def update_watermark(table_name):

    spark.sql(f"""
        DELETE FROM {CATALOG}.{SCHEMA}.bronze_watermark
        WHERE table_name='{table_name}'
    """)

    spark.sql(f"""
        INSERT INTO {CATALOG}.{SCHEMA}.bronze_watermark
        VALUES(
            '{table_name}',
            current_timestamp(),
            current_timestamp()
        )
    """)