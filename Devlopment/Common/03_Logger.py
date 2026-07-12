# Databricks notebook source
# ==========================================================
# Project      : E-Commerce Sales Analytics Platform
# Notebook     : 03_Logger
# Description  : Common Logger Configuration
# Author       : Sakshi Bejagmwar
# ==========================================================

import logging

# ==========================================================
# Configure Logger
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("ECommercePipeline")

logger.info("Logger Initialized Successfully")