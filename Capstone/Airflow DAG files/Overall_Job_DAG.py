from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

with DAG(
    dag_id="run_databricks_job",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["databricks"],
):

    run_job = DatabricksRunNowOperator(
        task_id="run_capstone_job",
        job_id=437509040263354,       
        databricks_conn_id="databricks_default"
    )

