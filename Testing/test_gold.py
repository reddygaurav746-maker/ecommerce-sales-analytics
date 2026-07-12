import pytest

CATALOG="gold_catalog"
SCHEMA="gold_sch"

tables=[

"dim_customer",
"dim_product",
"dim_seller",
"dim_date",
"fact_sales",
"daily_sales",
"monthly_sales",
"yearly_sales",
"sales_summary",
"customer_kpi",
"product_kpi",
"seller_kpi",
"executive_dashboard"

]

def test_gold_tables_exist():

    for table in tables:

        assert spark.catalog.tableExists(
            f"{CATALOG}.{SCHEMA}.{table}"
        )

def test_fact_sales():

    assert spark.table(
        f"{CATALOG}.{SCHEMA}.fact_sales"
    ).count()>0

def test_negative_revenue():

    count=spark.sql(f"""

    select *

    from {CATALOG}.{SCHEMA}.fact_sales

    where revenue<0

    """).count()

    assert count==0