from datetime import datetime
import os, re
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.hooks.gcs import GCSHook

BUCKET_INCOMING = "incoming_119"
BUCKET_SUCCESS  = "success_119"

SRC_PREFIX = "customer/"
DST_PREFIX = "CDP/customer/"
_SUFFIX_RE = re.compile(r".*_\d{6}$")  # ..._yyyymm

def _list_objs(client, bucket, prefix):
    return [b.name for b in client.list_blobs(bucket, prefix=prefix) if not b.name.endswith("/")]

def copy_idempotent_no_dup(src_bucket, src_obj, dst_bucket, dst_obj):
    """
    Không tạo duplicate:
    - Nếu đích đã tồn tại: SKIP (không overwrite), coi như thành công để dọn nguồn.
    - Nếu chưa có: copy.
    """
    hook = GCSHook(); client = hook.get_conn()
    src_blob = client.bucket(src_bucket).get_blob(src_obj)
    if not src_blob:
        return "skip_missing_src"

    if hook.exists(dst_bucket, dst_obj):
        return "skipped_exists"  # không ghi đè, tránh duplicate

    hook.copy(src_bucket, src_obj, dst_bucket, dst_obj)
    return "copied"

with DAG(
    dag_id="dag_customer_success",
    start_date=datetime(2024, 1, 1),
    schedule=None, catchup=False,
    tags=["customer", "success", "gcs"],
) as dag:

    @task
    def build_plan():
        hook = GCSHook(); client = hook.get_conn()
        yyyymm = "{{ ds_nodash[:6] }}"
        entries = []
        seen_dst = set()

        for src_obj in _list_objs(client, BUCKET_INCOMING, SRC_PREFIX):
            blob = client.bucket(BUCKET_INCOMING).get_blob(src_obj)
            base = os.path.basename(src_obj); stem, ext = os.path.splitext(base)
            yyyymm_from_create = blob.time_created.strftime("%Y%m")
            base_new = base if _SUFFIX_RE.match(stem) else f"{stem}_{yyyymm_from_create}{ext}"
            dst = f"{DST_PREFIX}{yyyymm}/{base_new}"

            if dst in seen_dst:
                continue  # tránh trùng đích trong cùng 1 run
            seen_dst.add(dst)
            entries.append({"src": src_obj, "dst": dst})

        return {"yyyymm": yyyymm, "entries": entries}

    @task
    def move_to_success(plan: dict):
        hook = GCSHook(); client = hook.get_conn()
        for e in plan["entries"]:
            status = copy_idempotent_no_dup(BUCKET_INCOMING, e["src"], BUCKET_SUCCESS, e["dst"])
            if status in ("copied", "skipped_exists"):
                # Xoá nguồn để tránh xử lý lại lần sau (idempotent)
                try: client.bucket(BUCKET_INCOMING).blob(e["src"]).delete()
                except Exception: pass
            else:
                raise RuntimeError(f"Copy failed for {e['src']}: {status}")
        return {"ok": True, "count": len(plan["entries"])}

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)
    move_to_success(build_plan()) >> end
