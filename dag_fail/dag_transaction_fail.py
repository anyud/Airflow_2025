from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_transaction_fail",
    default_args=default_args,
    description="Move transaction file to received_failed_119 if load fails",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["gcs", "fail", "transaction"],
) as dag:

    fail_transaction = GCSToGCSOperator(
        task_id="fail_transaction",
        source_bucket="asia-southeast1-etl-43e23097-bucket",
        source_object="incoming_119/transaction/*.csv",
        destination_bucket="asia-southeast1-etl-43e23097-bucket",
        destination_object="received_failed_119/transaction/",
        move_object=True,
    )

    fail_transaction
