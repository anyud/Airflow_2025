# dag_orchestrator_run_all.py
from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_DAGS = [
    "dag_customer_success",
    "dag_transaction_success",
    "dag_city_success",
    "dag_product_success",
]

STATUS_DAG = "dag_notification_status"   # DAG thứ 13 bạn đã tạo

with DAG(
    dag_id="dag_orchestrator_run_all",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["orchestrator","run_all"],
) as dag:

    # Trigger 4 DAG success song song
    triggers = [
        TriggerDagRunOperator(
            task_id=f"trigger_{d}",
            trigger_dag_id=d,
            reset_dag_run=True,                # nếu có run cùng execution_date thì reset
            wait_for_completion=False,         # cho chạy song song, không chờ tại đây
        )
        for d in SUCCESS_DAGS
    ]

    # Đợi cả 4 DAG success kết thúc (success hoặc failed)
    waits = [
        ExternalTaskSensor(
            task_id=f"wait_{d}",
            external_dag_id=d,
            external_task_id=None,            # đợi cả DAG
            allowed_states=["success", "failed"],
            failed_states=[],                  # không fail orchestrator khi DAG kia fail
            mode="reschedule",
            poke_interval=60,
            timeout=60*60*6,                  # 6h
        )
        for d in SUCCESS_DAGS
    ]

    # Gửi mail tổng hợp sau khi 4 DAG đã xong (dù thành công hay thất bại)
    notify = TriggerDagRunOperator(
        task_id="trigger_status_mail",
        trigger_dag_id=STATUS_DAG,
        reset_dag_run=True,
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE,    # chạy kể cả khi có DAG kia fail
    )

    for t, w in zip(triggers, waits):
        t >> w
    waits >> notify
