import pytest

CATALOG="silver_catalog"
SCHEMA="silver_sch"

tables=[

"silver_customers",
"silver_orders",
"silver_products",
"silver_order_payments",
"silver_product_categories",
"silver_sellers",
"silver_sales"

]

def test_silver_tables_exist():

    for table in tables:

        assert spark.catalog.tableExists(
            f"{CATALOG}.{SCHEMA}.{table}"
        )

def test_silver_tables_have_data():

    for table in tables:

        assert spark.table(
            f"{CATALOG}.{SCHEMA}.{table}"
        ).count()>0

def test_duplicate_customer():

    duplicates=spark.sql(f"""

    select customer_id

    from {CATALOG}.{SCHEMA}.silver_customers

    group by customer_id

    having count(*)>1

    """).count()

    assert duplicates==0

def test_customer_sk():

    df=spark.table(
        f"{CATALOG}.{SCHEMA}.silver_customers"
    )

    assert "customer_sk" in df.columns