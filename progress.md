# E-Commerce ETL Pipeline - Project Progress

---

## ✅ Phase 1 – Azure Foundation

- Created Azure Resource Group
- Configured ADLS Gen2 Storage Account
- Created `src-files` & `tgt-files` containers
- Uploaded source CSV files
-------------------------------------------------------------------------------------
## ✅ Phase 2 – Azure Data Factory

- Created Azure Data Factory
- Configured Linked Service
- Built Copy Data Pipeline
- Converted CSV files to Parquet
- Stored Parquet files in ADLS Gen2
---------------------------------------------------------------------------------------
## ✅ Phase 3 – Databricks Setup

- Created Azure Databricks Workspace
- Configured Unity Catalog
- Created Bronze & Silver Catalogs/Schemas
- Configured Storage Credential & External Location
- Connected Databricks with ADLS Gen2
----------------------------------------------------------------------------------------
## ✅ Phase 4 – Bronze Layer

- Loaded Parquet files into Bronze Delta Tables
- Implemented Incremental Loading (MERGE)
- Implemented Audit Logging, Error Logging & Watermark
- Enabled Schema Evolution
- Validated Bronze Tables

**Tables:** Customers, Orders, Products, Payments, Categories, Sellers
----------------------------------------------------------------------------------------
## ✅ Phase 5 – Silver Layer

- Cleansed and standardized Bronze data
- Performed Data Validation & Deduplication
- Generated Surrogate Keys
- Created Business-ready Silver Tables
- Implemented SCD Type 2 (Customer History)
- Applied Delta Optimization (OPTIMIZE, ZORDER, VACUUM)
- Demonstrated Time Travel

**Tables:** Customers, Orders, Products, Payments, Categories, Sellers, Sales
----------------------------------------------------------------------------------------