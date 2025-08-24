# dag_notification_status.py
from datetime import datetime, date
from airflow import DAG
from airflow.decorators import task
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# ====== Cấu hình ======
MAIL_TO = ["nhai37995@gmail.com"]  # thêm/bớt người nhận ở đây

BUCKET_SUCCESS       = "success_119"
BUCKET_RECEIVED_FAIL = "received_failed_119"

# Prefix cho từng domain (đúng mapping các DAG bạn đã tách)
DOMAINS = {
    "customer":   {"success_prefix": "CDP/customer/",   "fail_prefix": "customer/"},
    "transaction":{"success_prefix": "OMS/transaction/","fail_prefix": "transaction/"},
    "city":       {"success_prefix": "CDP/city/",       "fail_prefix": "city/"},
    "product":    {"success_prefix": "WMS/product/",    "fail_prefix": "product/"},
}

def _list_today(blobs, target_day: date):
    """Lọc blob theo ngày tạo (UTC) đúng ngày target_day."""
    out = []
    for b in blobs:
        if b.name.endswith("/"):
            continue
        # time_created là UTC
        if b.time_created.date() == target_day:
            out.append(b.name)
    return sorted(out)

with DAG(
    dag_id="dag_notification_status",
    start_date=datetime(2024, 1, 1),
    schedule=None,               # bạn có thể đặt cron nếu muốn chạy định kỳ
    catchup=False,
    tags=["notify","status","summary"],
) as dag:

    @task
    def collect_status(execution_date_str: str):
        """
        Quét success_119 và received_failed_119, gom theo domain cho ngày chạy.
        execution_date_str là '{{ ds }}' (YYYY-MM-DD, UTC).
        """
        hook = GCSHook(); client = hook.get_conn()
        target_day = datetime.strptime(execution_date_str, "%Y-%m-%d").date()

        summary = {"date": execution_date_str, "success": {}, "failed": {}}

        # Success per domain
        for dom, cfg in DOMAINS.items():
            prefix = cfg["success_prefix"]
            blobs = list(client.list_blobs(BUCKET_SUCCESS, prefix=prefix))
            today = _list_today(blobs, target_day)
            summary["success"][dom] = today

        # Failed per domain
        for dom, cfg in DOMAINS.items():
            prefix = cfg["fail_prefix"]
            blobs = list(client.list_blobs(BUCKET_RECEIVED_FAIL, prefix=prefix))
            today = _list_today(blobs, target_day)
            summary["failed"][dom] = today

        return summary

    def _render_html(summary: dict) -> str:
        def li(items):
            if not items:
                return "<li><i>Không có</i></li>"
            return "".join(f"<li>{x}</li>" for x in items)

        # ghép HTML cho từng domain
        sections = []
        for dom in DOMAINS.keys():
            ok_list   = summary["success"].get(dom, [])
            fail_list = summary["failed"].get(dom, [])
            sections.append(f"""
            <h4>Domain: {dom}</h4>
            <p><b>Success ({len(ok_list)}):</b></p>
            <ul>{li(ok_list)}</ul>
            <p><b>Failed ({len(fail_list)}):</b></p>
            <ul>{li(fail_list)}</ul>
            <hr/>
            """)
        body = f"""
        <h3>Airflow status report — {summary['date']}</h3>
        {''.join(sections)}
        """
        return body

    @task
    def build_email(summary: dict):
        # trả về dict để EmailOperator dùng templates không cần Jinja thêm
        subject = f"[Airflow] Daily Status — {summary['date']}"
        html = _render_html(summary)
        return {"subject": subject, "html": html}

    data = collect_status(execution_date_str="{{ ds }}")
    email_payload = build_email(data)

    send_status = EmailOperator(
        task_id="send_status_email",
        to=MAIL_TO,
        subject="{{ ti.xcom_pull(task_ids='build_email')['subject'] }}",
        html_content="{{ ti.xcom_pull(task_ids='build_email')['html'] }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    data >> email_payload >> send_status
