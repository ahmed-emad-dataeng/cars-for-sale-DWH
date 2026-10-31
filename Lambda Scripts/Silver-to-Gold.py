import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone, timedelta
import os
import io

s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")


def invoke_next(function_name: str, payload: dict):
    """Fire-and-forget async invoke of the next Lambda in the chain."""
    lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="Event",  # async — Silver-to-Gold doesn't wait
        Payload=json.dumps(payload).encode(),
    )
    print(f"  → Invoked {function_name} asynchronously")


# ── Constants ─────────────────────────────────────────────────────────────────
STATUS_ACTIVE = "active"
STATUS_UPDATED = "updated"
STATUS_DELETED = "deleted"

GOLD_FILE_KEY = "cars_scd.parquet"  # single file, overwritten each run

# Columns that are part of the SCD tracking metadata, not car attributes
SCD_COLS = ["first_seen_date", "scd_valid_from", "scd_valid_to", "status"]


# ── Extract ───────────────────────────────────────────────────────────────────
def read_silver(day_string: str) -> pd.DataFrame:
    """Read today's Silver Parquet partition into a DataFrame."""
    bucket = os.environ["SILVER_BUCKET_NAME"]
    key = f"ingestion_date={day_string}/cars.parquet"

    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    df = pd.read_parquet(buf)
    return df


def read_gold() -> pd.DataFrame | None:
    """Read the current Gold table. Returns None if it doesn't exist yet."""
    bucket = os.environ["GOLD_BUCKET_NAME"]
    prefix = os.environ["GOLD_DATA_PATH"]  # e.g. "gold"
    key = f"{prefix}/{GOLD_FILE_KEY}"

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        return pd.read_parquet(buf)
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        # Catch 404-style ClientErrors from boto3
        if "NoSuchKey" in str(e) or "404" in str(e):
            return None
        raise


# ── Schema helpers ────────────────────────────────────────────────────────────
def align_schemas(
    df_new: pd.DataFrame, df_gold: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Ensure both DataFrames have the same columns.
    New feature_* columns in Silver are added to Gold (filled False).
    Columns in Gold but missing from Silver are added to Silver (filled None/False).
    SCD columns are excluded from this alignment — they are managed separately.
    """
    new_car_cols = [c for c in df_new.columns if c not in SCD_COLS]
    gold_car_cols = [c for c in df_gold.columns if c not in SCD_COLS]

    all_car_cols = list(
        dict.fromkeys(gold_car_cols + new_car_cols)
    )  # preserve order, gold first

    for col in all_car_cols:
        if col not in df_new.columns:
            # column existed in gold but not in today's silver
            if col.startswith("feature_"):
                df_new[col] = pd.array([False] * len(df_new), dtype=pd.BooleanDtype())
            else:
                df_new[col] = None

        if col not in df_gold.columns:
            # new column appeared in today's silver
            if col.startswith("feature_"):
                df_gold[col] = pd.array([False] * len(df_gold), dtype=pd.BooleanDtype())
            else:
                df_gold[col] = None

    return df_new, df_gold


# ── Merge / SCD logic ─────────────────────────────────────────────────────────
def apply_scd(
    df_silver: pd.DataFrame, df_gold: pd.DataFrame | None, today: str
) -> pd.DataFrame:
    """
    Merge today's Silver snapshot into the Gold SCD table.

    Rules:
      - New ID (not in Gold)          → insert, status=active
      - Existing ID, updatedAt changed → close old row (status=updated, scd_valid_to=today)
                                         insert new row (status=active, first_seen_date preserved)
      - Existing ID, updatedAt same   → no change needed (row stays as-is)
      - Active ID missing from Silver → close row (status=deleted, scd_valid_to=today)
      - Active row already opened today, updatedAt changed (same-day re-run)
                                       → replace it in place instead of closing it,
                                         so (id, scd_valid_from) stays unique
    """

    silver_ids = set(df_silver["id"].astype(str))

    # ── Bootstrap: first ever run ─────────────────────────────────────────────
    if df_gold is None:
        df_silver, _ = align_schemas(df_silver, df_silver.copy())  # no-op alignment
        df_silver["first_seen_date"] = today
        df_silver["scd_valid_from"] = today
        df_silver["scd_valid_to"] = None
        df_silver["status"] = STATUS_ACTIVE
        print(f"  Bootstrap: inserting {len(df_silver)} new listings")
        return df_silver

    # ── Align schemas before any comparison ──────────────────────────────────
    df_silver, df_gold = align_schemas(df_silver, df_gold)

    # Build lookup: id → current active row in Gold
    gold_active = df_gold[df_gold["status"] == STATUS_ACTIVE].copy()
    gold_active_map = gold_active.set_index(gold_active["id"].astype(str))

    rows_to_close = []  # indices in df_gold to update (scd_valid_to, status)
    rows_to_insert = []  # new rows to append
    rows_to_replace = []  # indices in df_gold opened today, superseded by a re-run

    for _, silver_row in df_silver.iterrows():
        sid = str(silver_row["id"])

        if sid not in gold_active_map.index:
            # ── New listing ───────────────────────────────────────────────────
            new_row = silver_row.copy()
            new_row["first_seen_date"] = today
            new_row["scd_valid_from"] = today
            new_row["scd_valid_to"] = None
            new_row["status"] = STATUS_ACTIVE
            rows_to_insert.append(new_row)

        else:
            gold_row = gold_active_map.loc[sid]

            # Handle duplicate IDs in gold_active_map (take the most recent)
            if isinstance(gold_row, pd.DataFrame):
                gold_row = gold_row.sort_values("scd_valid_from").iloc[-1]

            silver_updated = str(silver_row.get("updatedAt", ""))
            gold_updated = str(gold_row.get("updatedAt", ""))

            if silver_updated != gold_updated:
                # ── Listing was updated: close old row, open new one ──────────
                gold_row_idx = df_gold[
                    (df_gold["id"].astype(str) == sid)
                    & (df_gold["status"] == STATUS_ACTIVE)
                ].index[
                    -1
                ]  # take the most recent if duplicates
                if str(gold_row["scd_valid_from"]) == today:
                    # Same-day re-run: the active row was opened earlier today.
                    # Closing it would leave a zero-length version and a second
                    # row with the same (id, scd_valid_from), so swap it instead.
                    rows_to_replace.append(gold_row_idx)
                else:
                    rows_to_close.append(gold_row_idx)
                new_row = silver_row.copy()
                new_row["first_seen_date"] = gold_row["first_seen_date"]  # preserve
                new_row["scd_valid_from"] = today
                new_row["scd_valid_to"] = None
                new_row["status"] = STATUS_ACTIVE
                rows_to_insert.append(new_row)
            # else: unchanged — leave gold row untouched

    # ── Mark deleted listings ─────────────────────────────────────────────────
    gold_active_ids = set(gold_active_map.index)
    deleted_ids = gold_active_ids - silver_ids
    deleted_indices = df_gold[
        (df_gold["id"].astype(str).isin(deleted_ids))
        & (df_gold["status"] == STATUS_ACTIVE)
    ].index.tolist()

    print(
        f"  New listings   : {len([r for r in rows_to_insert if r['status'] == STATUS_ACTIVE and r['scd_valid_from'] == today and str(r['id']) not in gold_active_ids])}"
    )
    print(f"  Updated rows   : {len(rows_to_close)}")
    print(f"  Deleted rows   : {len(deleted_ids)}")
    if rows_to_replace:
        print(f"  Same-day rewrites: {len(rows_to_replace)} (re-run of {today})")

    # ── Apply closes (updated + deleted) to df_gold ───────────────────────────
    all_close_indices = list(set(rows_to_close + deleted_indices))
    if all_close_indices:
        df_gold.loc[rows_to_close, "status"] = STATUS_UPDATED
        df_gold.loc[rows_to_close, "scd_valid_to"] = today
        df_gold.loc[deleted_indices, "status"] = STATUS_DELETED
        df_gold.loc[deleted_indices, "scd_valid_to"] = today

    # ── Drop rows superseded by a same-day re-run (replacements are inserted) ─
    if rows_to_replace:
        df_gold = df_gold.drop(index=rows_to_replace)

    # ── Append new rows ───────────────────────────────────────────────────────
    if rows_to_insert:
        df_insert = pd.DataFrame(rows_to_insert)
        df_gold = pd.concat([df_gold, df_insert], ignore_index=True)

    return df_gold


# ── Enforce Datatypes ──────────────────────────────────────────────────────────────────────
def enforce_datatypes(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure key columns have the correct types for storage and querying."""
    for col in df.columns:
        if col.startswith("feature_"):
            df[col] = df[col].astype(pd.BooleanDtype())
    return df


# ── Save ──────────────────────────────────────────────────────────────────────
def save_gold(df: pd.DataFrame) -> str:
    """Overwrite the Gold Parquet file on S3."""
    bucket = os.environ["GOLD_BUCKET_NAME"]
    prefix = os.environ["GOLD_DATA_PATH"]
    key = f"{prefix}/{GOLD_FILE_KEY}"

    # Ensure scd_valid_to is stored as string so Parquet/Athena handles nulls cleanly
    df["scd_valid_to"] = df["scd_valid_to"].where(
        df["scd_valid_to"].notna(), other=None
    )

    buf = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    return f"s3://{bucket}/{key}"


# ── Lambda handler ────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Normal daily run : event = {}
    Backfill         : event = {"start_date": "2026-03-01", "end_date": "2026-03-28"}
    Single day       : event = {"start_date": "2026-03-10", "end_date": "2026-03-10"}

    For backfills the Gold table is built incrementally day-by-day in
    chronological order, so the SCD history is reconstructed correctly.
    """
    today_utc = datetime.now(timezone.utc).date()

    start_date = datetime.strptime(
        event.get("start_date", str(today_utc)), "%Y-%m-%d"
    ).date()
    end_date = datetime.strptime(
        event.get("end_date", str(today_utc)), "%Y-%m-%d"
    ).date()

    if start_date > end_date:
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")

    # Build list of dates in chronological order (important for correct SCD history)
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(str(current))
        current += timedelta(days=1)

    print(f"Processing {len(dates)} day(s): {dates[0]} → {dates[-1]}")

    results = []
    for day_string in dates:
        print(f"\n── {day_string} ──────────────────────────────────────────")
        try:
            df_silver = read_silver(day_string)
            df_silver = df_silver.sort_values(
                "updatedAt", ascending=False
            ).drop_duplicates(subset="id", keep="first")
            print(f"  Silver rows : {len(df_silver)}")

            df_gold = read_gold()
            if df_gold is not None:
                print(f"  Gold rows   : {len(df_gold)}")
            else:
                print(f"  Gold rows   : 0 (first run)")

            df_gold = apply_scd(df_silver, df_gold, day_string)
            df_gold = enforce_datatypes(df_gold)
            gold_path = save_gold(df_gold)

            results.append(
                {
                    "date": day_string,
                    "status": "success",
                    "silver_rows": len(df_silver),
                    "gold_rows": len(df_gold),
                    "gold_path": gold_path,
                }
            )
            print(f"  ✓ Gold saved → {gold_path}  ({len(df_gold)} total rows)")

        except Exception as e:
            results.append(
                {
                    "date": day_string,
                    "status": "failed",
                    "error": str(e),
                }
            )
            print(f"  ✗ {day_string} — {str(e)}")
            # For backfills we continue; for single-day runs this surfaces naturally
            continue

    # Chain to the dbt data-quality runner once per invocation, only if at
    # least one day saved successfully — no point testing unchanged Gold.
    dbt_runner = os.environ.get("DBT_RUNNER_FUNCTION")
    any_success = any(r["status"] == "success" for r in results)
    if dbt_runner and any_success:
        invoke_next(dbt_runner, {"trigger": "silver-to-gold"})
    elif not dbt_runner:
        print("  DBT_RUNNER_FUNCTION not set — skipping data-quality run")

    return {
        "statusCode": 200,
        "body": {
            "processed": len([r for r in results if r["status"] == "success"]),
            "failed": len([r for r in results if r["status"] == "failed"]),
            "results": results,
        },
    }
