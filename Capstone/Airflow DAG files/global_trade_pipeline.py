from airflow import DAG
from datetime import datetime
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

with DAG(
    dag_id="global_trade_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Bronze -> Silver -> Gold Databricks Pipeline",
    tags=["capstone", "databricks"]
) as dag:

    bronze_task = DatabricksRunNowOperator(
        task_id="bronze_layer_ingestion",
        databricks_conn_id="databricks_capstone",
        job_id=931572949178308
    )

    silver_task = DatabricksRunNowOperator(
        task_id="silver_layer_processing",
        databricks_conn_id="databricks_capstone",
        job_id=594986230574895
    )

    gold_task = DatabricksRunNowOperator(
        task_id="gold_layer_processing",
        databricks_conn_id="databricks_capstone",
        job_id=410376799527426
    )

    bronze_task >> silver_task >> gold_task

