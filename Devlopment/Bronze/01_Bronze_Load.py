# Databricks notebook source
# MAGIC %run /Workspace/E-Commerce-ETL/Common/01_Config

# COMMAND ----------

# MAGIC %run /Workspace/E-Commerce-ETL/Common/02_Utilities

# COMMAND ----------

# MAGIC %run /Workspace/E-Commerce-ETL/Common/03_Logger

# COMMAND ----------

# MAGIC %run /Workspace/E-Commerce-ETL/Common/04_Slack_Notifications

# COMMAND ----------

# ==========================================================
# Project      : E-Commerce Sales Analytics Platform
# Notebook     : 01_Bronze_Load
# Description  : Bronze Layer Data Load
# Author       : Sakshi Bejagmwar
# ==========================================================
def bronze_load(dataset):

    file_name = dataset["file"]
    table_name = dataset["table"]
    primary_key = dataset["primary_key"]

    source_path = BASE_PATH + file_name

    start_time = datetime.now()
    rows_read = 0
    rows_loaded = 0

    logger.info(f"Started loading {table_name}")

    try:

        df = (
            spark.read
            .format("parquet")
            .option("mergeSchema", "true")
            .load(source_path)
        )

        rows_read = df.count()

        df = (
            df
            .withColumn(
                "bronze_load_timestamp",
                current_timestamp()
            )
            .withColumn(
                "bronze_load_date",
                current_date()
            )
            .withColumn(
                "bronze_source_file",
                lit(file_name)
            )
        )

        table_path = f"{CATALOG}.{SCHEMA}.{table_name}"

        if isinstance(primary_key, list):

            merge_condition = " AND ".join(
                [
                    f"target.{col}=source.{col}"
                    for col in primary_key
                ]
            )

        else:

            merge_condition = (
                f"target.{primary_key}=source.{primary_key}"
            )

        if spark.catalog.tableExists(table_path):

            delta_table = DeltaTable.forName(
                spark,
                table_path
            )

            (
                delta_table.alias("target")
                .merge(
                    df.alias("source"),
                    merge_condition
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        else:

            (
                df.write
                .format("delta")
                .option(
                    "mergeSchema",
                    "true"
                )
                .saveAsTable(table_path)
            )

        rows_loaded = spark.table(table_path).count()

        end_time = datetime.now()

        write_audit(
            table_name,
            file_name,
            start_time,
            end_time,
            rows_read,
            rows_loaded,
            "SUCCESS"
        )

        update_watermark(table_name)

        logger.info(f"{table_name} loaded successfully")

    except Exception as e:

        end_time = datetime.now()

        logger.error(str(e))

        write_error(
            table_name,
            e
        )

        write_audit(
            table_name,
            file_name,
            start_time,
            end_time,
            rows_read,
            rows_loaded,
            "FAILED",
            str(e)
        )

        logger.error(f"{table_name} failed")

        raise


try:

    logger.info("Bronze Pipeline Started")

    for dataset in datasets:

        bronze_load(dataset)

    logger.info("Bronze Pipeline Completed Successfully")
    logger.info("Bronze Pipeline Finished")

    send_success_notification(
        layer="Bronze",
        notebook="01_Bronze_Load",
        pipeline="Bronze_Load"
    )

except Exception as e:

    logger.error(str(e))

    send_failure_notification(
        layer="Bronze",
        notebook="01_Bronze_Load",
        pipeline="Bronze_Load",
        error=str(e)
    )

    raise


display(
    spark.sql(
        f"""
        SHOW TABLES IN {CATALOG}.{SCHEMA}
        """
    )
)