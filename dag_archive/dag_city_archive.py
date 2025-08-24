from datetime import datetime
import os
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.hooks.gcs import GCSHook

BUCKET_SUCCESS = "success_119"
BUCKET_ARCHIVE = "archive_119"

SRC_PREFIX = "CDP/city/"
DST_PREFIX = "CDP/city/"

def _list_objs(client, bucket, prefix):
    return [b.name for b in client.list_blobs(bucket, prefix=prefix) if not b.name.endswith("/")]

def copy_idempotent_no_dup(src_bucket, src_obj, dst_bucket, dst_obj):
    hook = GCSHook()
    if hook.exists(dst_bucket, dst_obj):
        return "skipped_exists"
    hook.copy(src_bucket, src_obj, dst_bucket, dst_obj)
    return "copied"

with DAG(
    dag_id="dag_city_archive",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["city", "archive", "gcs"],
) as dag:

    @task
    def build_plan():
        hook = GCSHook(); client = hook.get_conn()
        entries, seen_dst = [], set()

        for src_obj in _list_objs(client, BUCKET_SUCCESS, SRC_PREFIX):
            base = os.path.basename(src_obj)
            dst  = f"{DST_PREFIX}{base}"
            if dst in seen_dst: 
                continue
            seen_dst.add(dst)
            entries.append({"src": src_obj, "dst": dst})

        return {"entries": entries}

    @task
    def move_to_archive(plan: dict):
        hook = GCSHook(); client = hook.get_conn()
        for e in plan["entries"]:
            status = copy_idempotent_no_dup(BUCKET_SUCCESS, e["src"], BUCKET_ARCHIVE, e["dst"])
            if status in ("copied", "skipped_exists"):
                try: client.bucket(BUCKET_SUCCESS).blob(e["src"]).delete()
                except Exception: pass
            else:
                raise RuntimeError(f"Archive failed for {e['src']}: {status}")
        return {"ok": True, "count": len(plan["entries"])}

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)
    move_to_archive(build_plan()) >> end
