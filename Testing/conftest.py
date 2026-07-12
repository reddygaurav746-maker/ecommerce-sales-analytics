import pytest
from pyspark.sql import SparkSession

print("✅ conftest.py loaded")

@pytest.fixture(scope="session")
def spark():
    print("✅ Spark fixture called")

    spark = SparkSession.builder.getOrCreate()

    return spark