# Databricks notebook source
# MAGIC %run ../Common/04_Slack_Notifications

# COMMAND ----------

send_slack_notification(
    status="SUCCESS",
    layer="Testing",
    notebook="Slack_Test",
    pipeline="Slack Pipeline",
    message="Slack Integration Working Successfully"
)

send_success_notification(
    layer="Testing",
    notebook="Slack_Test",
    pipeline="Slack_Test"
)