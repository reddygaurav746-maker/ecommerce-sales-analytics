import pytest

CATALOG = "bronze_catalog"
SCHEMA = "bronze_sch"

TABLES = [
    "bronze_customers",
    "bronze_orders",
    "bronze_products",
    "bronze_order_payments",
    "bronze_product_categories",
    "bronze_sellers"
]


def test_bronze_tables_exist(spark):

    for table in TABLES:
        assert spark.catalog.tableExists(
            f"{CATALOG}.{SCHEMA}.{table}"
        ), f"{table} does not exist"


def test_bronze_tables_have_data(spark):

    for table in TABLES:

        row_count = spark.table(
            f"{CATALOG}.{SCHEMA}.{table}"
        ).count()

        assert row_count > 0, f"{table} is empty"


def test_bronze_audit_table_exists(spark):

    assert spark.catalog.tableExists(
        f"{CATALOG}.{SCHEMA}.bronze_audit_log"
    )


def test_bronze_error_table_exists(spark):

    assert spark.catalog.tableExists(
        f"{CATALOG}.{SCHEMA}.bronze_error_log"
    )


def test_bronze_watermark_table_exists(spark):

    assert spark.catalog.tableExists(
        f"{CATALOG}.{SCHEMA}.bronze_watermark"
    )


def test_bronze_audit_contains_records(spark):

    count = spark.table(
        f"{CATALOG}.{SCHEMA}.bronze_audit_log"
    ).count()

    assert count > 0


def test_bronze_watermark_contains_records(spark):

    count = spark.table(
        f"{CATALOG}.{SCHEMA}.bronze_watermark"
    ).count()

    assert count > 0