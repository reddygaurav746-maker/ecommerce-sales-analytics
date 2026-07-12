import pytest

def test_audit_table():

    assert spark.catalog.tableExists(
        "bronze_catalog.bronze_sch.bronze_audit_log"
    )

def test_error_table():

    assert spark.catalog.tableExists(
        "bronze_catalog.bronze_sch.bronze_error_log"
    )

def test_watermark_table():

    assert spark.catalog.tableExists(
        "bronze_catalog.bronze_sch.bronze_watermark"
    )