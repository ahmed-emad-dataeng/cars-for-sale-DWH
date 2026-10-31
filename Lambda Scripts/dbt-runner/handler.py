"""
DbtRunner Lambda
────────────────────────────────────────────────────────────────────────────────
Invoked asynchronously by Silver-to-Gold after a successful Gold save.
Runs `dbt build` (analytics view + all data tests) and `dbt source freshness`
against Athena, then prints one parseable line per result plus a summary —
EmailNotify greps these lines out of CloudWatch for the daily digest:

    DBT PASS  not_null_id
    DBT WARN  assert_km_plausible_for_year (471 rows)
    DBT FAIL  assert_one_active_row_per_id (3 rows)
    DBT SUMMARY: passed=25 warned=5 failed=0 errored=0 skipped=0

Never raises on test failures — async invokes retry twice, and a red test
should be reported once, not re-queried three times. Only invocation-level
problems (bad profile, Athena unreachable) surface as ERROR lines.

Environment variables (all optional, defaults baked into profiles.yml):
    DBT_ATHENA_REGION       e.g. eu-north-1
    DBT_ATHENA_SCHEMA       Glue database of the gold table (dubizzle)
    DBT_ATHENA_S3_STAGING   s3:// URI for Athena query results
"""

import os
import threading

# ── Lambda multiprocessing shim ───────────────────────────────────────────────
# Lambda has no /dev/shm, so POSIX semaphores (multiprocessing.SemLock) fail
# with FileNotFoundError. dbt only ever coordinates *threads* (its "processes"
# are a ThreadPool), so swapping the semaphore-based primitives for threading
# equivalents is safe. Pipes work fine in Lambda, so mp queues (which dbt's
# ThreadPool relies on, including their internal ._reader pipe) are left
# untouched — they pick up these patched locks through the context.
from multiprocessing import context as _mp_context

_mp_context.BaseContext.RLock = lambda self: threading.RLock()
_mp_context.BaseContext.Lock = lambda self: threading.Lock()
_mp_context.BaseContext.Semaphore = lambda self, value=1: threading.Semaphore(value)
_mp_context.BaseContext.BoundedSemaphore = (
    lambda self, value=1: threading.BoundedSemaphore(value)
)
_mp_context.BaseContext.Condition = lambda self, lock=None: threading.Condition(lock)
_mp_context.BaseContext.Event = lambda self: threading.Event()

from dbt.cli.main import dbtRunner  # noqa: E402  (must come after the shim)

TASK_ROOT = os.environ.get("LAMBDA_TASK_ROOT", "/var/task")
PROJECT_DIR = f"{TASK_ROOT}/dubizzle_dbt"
PROFILES_DIR = f"{TASK_ROOT}/profiles"

# Only /tmp is writable inside Lambda
COMMON_ARGS = [
    "--project-dir",
    PROJECT_DIR,
    "--profiles-dir",
    PROFILES_DIR,
    "--target-path",
    "/tmp/dbt-target",
    "--log-path",
    "/tmp/dbt-logs",
]


def summarize(invocation, counters: dict) -> None:
    """Print one DBT line per node result and accumulate counters."""
    result = getattr(invocation, "result", None)
    node_results = getattr(result, "results", None) or []

    for r in node_results:
        status = str(r.status).lower()
        name = r.node.name
        failures = getattr(r, "failures", None)

        if status in ("pass", "success"):
            counters["passed"] += 1
            print(f"DBT PASS  {name}")
        elif status == "warn":
            counters["warned"] += 1
            print(f"DBT WARN  {name} ({failures} rows)")
        elif status == "fail":
            counters["failed"] += 1
            print(f"DBT FAIL  {name} ({failures} rows)")
        elif status == "skipped":
            counters["skipped"] += 1
            print(f"DBT SKIP  {name}")
        else:  # error / runtime error
            counters["errored"] += 1
            msg = getattr(r, "message", "") or ""
            print(f"DBT ERROR {name} — {msg[:200]}")

    if invocation.exception:
        counters["errored"] += 1
        print(f"DBT ERROR invocation — {str(invocation.exception)[:300]}")


def lambda_handler(event, context):
    counters = {"passed": 0, "warned": 0, "failed": 0, "errored": 0, "skipped": 0}
    runner = dbtRunner()

    print("Running dbt build ...")
    build = runner.invoke(["build", *COMMON_ARGS])
    summarize(build, counters)

    print("Running dbt source freshness ...")
    freshness = runner.invoke(["source", "freshness", *COMMON_ARGS])
    summarize(freshness, counters)

    summary = (
        f"DBT SUMMARY: passed={counters['passed']} warned={counters['warned']} "
        f"failed={counters['failed']} errored={counters['errored']} "
        f"skipped={counters['skipped']}"
    )
    print(summary)

    return {"statusCode": 200, "body": summary}
