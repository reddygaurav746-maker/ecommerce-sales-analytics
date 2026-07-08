# E-Commerce Sales Analytics Platform
### Azure Databricks • PySpark • Delta Lake • Medallion Architecture

---

## Project Overview

This project implements an **end-to-end E-Commerce Sales Analytics Platform** using **Azure Databricks**, **PySpark**, **Delta Lake**, and the **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline ingests raw e-commerce datasets, performs scalable ETL transformations, and prepares business-ready data for analytics and reporting.

The project follows industry-standard Data Engineering practices including:

- Incremental data ingestion
- Data validation
- Data quality checks
- Schema enforcement
- Logging & auditing
- Layered Medallion Architecture
- Delta Lake storage

---

## Project Status

| Layer | Status |
|---------|---------|
| Bronze Layer | ✅ Completed |
| Silver Layer | ✅ Completed |
| Gold Layer | 🚧 In Progress |
| Dashboard | 🚧 Planned |

---

# High Level Architecture

The High Level Design illustrates the complete end-to-end data engineering workflow.

<p align="center">
<img src="diagrams/HLD.png" width="100%">
</p>

---

# Low Level Architecture

The Low Level Design shows every processing stage inside the platform.

<p align="center">
<img src="diagrams/LLD.png" width="100%">
</p>

---

# Database Design (Table Structure)

This diagram represents all tables designed across the Bronze, Silver and Gold layers.

<p align="center">
<img src="diagrams/LOT.png" width="100%">
</p>

---

# Star Schema Design

The Gold Layer follows a **Star Schema** model for fast analytical querying.

<p align="center">
<img src="diagrams/DMD.png" width="100%">
</p>

---

# Objectives

- Build an enterprise-grade ETL pipeline
- Implement Medallion Architecture
- Process raw e-commerce datasets
- Clean and standardize incoming data
- Create analytics-ready datasets
- Improve data quality
- Support future reporting and dashboards
- Follow scalable Data Engineering practices

---

# Dataset

## Source Files

- customers_dataset.csv
- orders_dataset.csv
- products_dataset.csv
- payments_dataset.csv
- sellers_dataset.csv
- category_translation.csv

These datasets simulate a real-world online retail platform.

---

# Medallion Architecture

## Bronze Layer (Raw)

**Purpose**

- Store raw data exactly as received
- Preserve original records
- Maintain ingestion history
- Add metadata columns
- Enable traceability

**Tables**

- bronze_customers
- bronze_orders
- bronze_products
- bronze_order_payments
- bronze_product_categories
- bronze_sellers

**Status:** ✅ Completed

---

## Silver Layer (Cleaned)

**Purpose**

- Clean incoming datasets
- Remove duplicate records
- Standardize data types
- Handle missing values
- Prepare business-ready datasets

**Output Tables**

- silver_customers
- silver_orders
- silver_products
- silver_order_payments
- silver_product_categories
- silver_sellers

**Status:** ✅ Completed

---

## Gold Layer (Analytics)

**Planned**

- Fact Sales
- Customer Dimension
- Product Dimension
- Seller Dimension
- Category Dimension
- Date Dimension

**Status:** 🚧 Under Development

---

# Data Quality Checks

- Schema Validation
- Null Value Checks
- Duplicate Detection
- Data Type Validation
- Record Count Validation

---

# Logging & Auditing

Audit tables include:

- Pipeline Logs
- Error Logs
- Audit Pipeline
- Data Quality Results

Tracked fields include Pipeline Name, Batch ID, Execution Time, Status, Rows Processed, Error Message, and Source File.

---

# Project Structure

```text
ecommerce-sales-analytics
│
├── databricks
│   ├── bronze
│   ├── silver
│   ├── common
│   └── gold
│
├── datasets
├── diagrams
│   ├── HLD.png
│   ├── LLD.png
│   ├── LOT.png
│   └── DMD.png
│
├── README.md
└── .gitignore
```

---

# Technologies Used

| Category | Technology |
|-----------|------------|
| Cloud | Microsoft Azure |
| Processing | Azure Databricks |
| Language | Python |
| Framework | PySpark |
| Storage | Delta Lake |
| Catalog | Unity Catalog |
| Version Control | Git & GitHub |

---

# Future Enhancements

- Gold Layer
- Star Schema
- Power BI Dashboard
- Databricks Workflows
- CI/CD
- Performance Optimization

---

# Author

**Gaurav Reddy**

Azure Data Engineer | Databricks | PySpark | Delta Lake

⭐ If you found this repository useful, consider giving it a Star.
