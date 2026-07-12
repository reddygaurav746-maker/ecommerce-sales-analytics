-- Databricks notebook source
SELECT * FROM bronze_catalog.bronze_sch.bronze_customers
LIMIT 10;

SELECT * FROM bronze_catalog.bronze_sch.bronze_orders
LIMIT 10;

SELECT * FROM bronze_catalog.bronze_sch.bronze_order_payments
LIMIT 10;

SELECT * FROM bronze_catalog.bronze_sch.bronze_product_categories
LIMIT 10;

SELECT * FROM bronze_catalog.bronze_sch.bronze_products
LIMIT 10;

SELECT * FROM bronze_catalog.bronze_sch.bronze_sellers
LIMIT 10;

SELECT * FROM bronze_catalog.bronze_sch.bronze_audit_log
LIMIT 10;

SELECT * FROM bronze_catalog.bronze_sch.bronze_error_log
LIMIT 10;

SELECT * FROM bronze_catalog.bronze_sch.bronze_watermark
LIMIT 10;
