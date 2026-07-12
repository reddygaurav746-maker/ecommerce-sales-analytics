# Databricks notebook source
import json
import requests
from datetime import datetime

SLACK_WEBHOOK_URL = dbutils.secrets.get(
    scope="ecommerce-secrets",
    key="slack-webhook-url"
)


def send_success_notification(layer, notebook, pipeline):

    payload = {
        "text": f"""
🟢 *E-Commerce ETL Pipeline*

*Status:* SUCCESS
*Layer:* {layer}
*Pipeline:* {pipeline}
*Notebook:* {notebook}
*Timestamp:* {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Message:
{layer} Layer completed successfully.
"""
    }

    requests.post(
        SLACK_WEBHOOK_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )


def send_failure_notification(layer, notebook, pipeline, error):

    payload = {
        "text": f"""
🔴 *E-Commerce ETL Pipeline*

*Status:* FAILED
*Layer:* {layer}
*Pipeline:* {pipeline}
*Notebook:* {notebook}
*Timestamp:* {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Error:
{error}
"""
    }

    requests.post(
        SLACK_WEBHOOK_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )