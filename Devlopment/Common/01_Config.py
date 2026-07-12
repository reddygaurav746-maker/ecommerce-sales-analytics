# Databricks notebook source
# ==========================================================
# Project      : E-Commerce Sales Analytics Platform
# Notebook     : 01_Config
# Description  : Project Configuration File
# Author       : Sakshi Bejagmwar
# ==========================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

from datetime import datetime
import uuid

# ==========================================================
# Unity Catalog Configuration
# ==========================================================

CATALOG = "bronze_catalog"
SCHEMA = "bronze_sch"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# ==========================================================
# Silver Configuration
# ==========================================================

SILVER_CATALOG = "silver_catalog"
SILVER_SCHEMA = "silver_sch"

# ==========================================================
# Gold Configuration
# ==========================================================

# Gold Layer

GOLD_CATALOG = "gold_catalog"
GOLD_SCHEMA = "gold_sch"

GOLD_STORAGE_ACCOUNT = "stgecomdatalake001"
GOLD_CONTAINER = "tgt-files"
GOLD_FOLDER = "gold"

GOLD_BASE_PATH = f"abfss://{GOLD_CONTAINER}@{GOLD_STORAGE_ACCOUNT}.dfs.core.windows.net/{GOLD_FOLDER}/"
# ==========================================================
# Azure Storage Configuration
# ==========================================================

STORAGE_ACCOUNT = "stgecomdatalake001"

CONTAINER = "tgt-files"

SOURCE_FOLDER = "target_files"

BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{SOURCE_FOLDER}/"

# ==========================================================
# Pipeline Configuration
# ==========================================================

PIPELINE_NAME = "Bronze_Load_Pipeline"

NOTEBOOK_NAME = "01_Bronze_Load"

EXECUTION_ID = str(uuid.uuid4())

# ==========================================================
# Dataset Configuration
# ==========================================================

datasets = [

    {
        "file": "customer_dataset.parquet",
        "table": "bronze_customers",
        "primary_key": "customer_id"
    },

    {
        "file": "orders_datasets.parquet",
        "table": "bronze_orders",
        "primary_key": "order_id"
    },

    {
        "file": "order_payment_dataset.parquet",
        "table": "bronze_order_payments",
        "primary_key": ["order_id", "payment_sequential"]
    },

    {
        "file": "product_datasets.parquet",
        "table": "bronze_products",
        "primary_key": "product_id"
    },

    {
        "file": "product_category_name_translation.parquet",
        "table": "bronze_product_categories",
        "primary_key": "product_category_name"
    },

    {
        "file": "sellers_datasets.parquet",
        "table": "bronze_sellers",
        "primary_key": "seller_id"
    }

]