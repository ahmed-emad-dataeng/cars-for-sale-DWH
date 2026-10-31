"""
Gold Layer Data Quality Tests — cars-for-sale-DWH
===================================================
Run locally against a downloaded gold Parquet snapshot, or in Lambda
after the Silver-to-Gold job by pointing GOLD_PARQUET_PATH at the S3 file.

Usage:
    # Local
    python "Lambda Scripts\Test_gold_layer.py" --path "Raw Data\cars_scd-11-05.parquet"

    # Inside Lambda / CI (set env var)
    GOLD_PARQUET_PATH=s3://your-bucket/gold/cars_scd.parquet python test_gold_layer.py
"""

import os
import sys
import io
import argparse
import textwrap
from datetime import date
from typing import Callable

import pandas as pd

# ── Optional S3 support ───────────────────────────────────────────────────────
try:
    import boto3

    _HAS_BOTO3 = True
except ImportError:
    _HAS_BOTO3 = False

# ── Constants ──────────────────────────────────────────────────────────────────
VALID_STATUSES = {"active", "updated", "deleted"}
DATE_FMT = "%Y-%m-%d"
MIN_LAUNCH_DATE = "2020-01-01"  # no listing can pre-date the pipeline launch era
MAX_FUTURE_DAYS = 1  # scd_valid_from can't be more than 1 day ahead of today

STATUS_ACTIVE = "active"
STATUS_UPDATED = "updated"
STATUS_DELETED = "deleted"

# Columns that must always be present
REQUIRED_COLS = [
    "id",
    "externalID",
    "title",
    "Price",
    "Year",
    "Kilometers",
    "Brand",
    "Model",
    "Transmission Type",
    "Body Type",
    "Fuel Type",
    "createdAt",
    "updatedAt",
    "_ingestionDate",
    "first_seen_date",
    "scd_valid_from",
    "scd_valid_to",
    "status",
]

# Reasonable bounds for Egyptian used-car market
PRICE_MIN_EGP = 10_000  # below this is almost certainly junk data
PRICE_MAX_EGP = 50_000_000  # 50M EGP upper ceiling
YEAR_MIN = 1960
YEAR_MAX = date.today().year + 1  # allow next model year
KM_MAX = 1_500_000  # 1.5M km is a realistic but extreme upper bound
HP_MAX = 1_500
CC_MAX = 10_000


# ══════════════════════════════════════════════════════════════════════════════
# Test registry
# ══════════════════════════════════════════════════════════════════════════════

_TESTS: list[dict] = []


def test(name: str, category: str, severity: str = "error"):
    """Decorator that registers a test function."""

    def decorator(fn: Callable[[pd.DataFrame], list[str]]):
        _TESTS.append(
            {"name": name, "category": category, "severity": severity, "fn": fn}
        )
        return fn

    return decorator


# ══════════════════════════════════════════════════════════════════════════════
# ── Category 1: Schema ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════


@test("Required columns present", "Schema")
def check_required_columns(df: pd.DataFrame) -> list[str]:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    return [f"Missing required column: '{c}'" for c in missing]


@test("No fully-empty columns", "Schema", severity="warning")
def check_no_empty_columns(df: pd.DataFrame) -> list[str]:
    issues = []
    for col in df.columns:
        if df[col].isna().all():
            issues.append(f"Column '{col}' is 100% null across all rows")
    return issues


# ══════════════════════════════════════════════════════════════════════════════
# ── Category 2: SCD Integrity ─────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════


@test("status column only contains valid values", "SCD Integrity")
def check_status_values(df: pd.DataFrame) -> list[str]:
    bad = df[~df["status"].isin(VALID_STATUSES)]
    if bad.empty:
        return []
    vals = bad["status"].value_counts().to_dict()
    return [f"Invalid status values found: {vals}"]


@test("No overlapping periods per ID", "SCD Integrity")
def check_no_overlapping_periods(df: pd.DataFrame) -> list[str]:
    """Critical for SCD correctness."""
    issues = []
    for listing_id, group in df.groupby("id"):
        closed = group[group["scd_valid_to"].notna()].copy()
        if len(closed) == 0:
            continue
        closed["_from"] = pd.to_datetime(closed["scd_valid_from"])
        closed["_to"] = pd.to_datetime(closed["scd_valid_to"])
        # Simple overlap check (can be optimized)
        for i in range(len(closed)):
            for j in range(i + 1, len(closed)):
                a = closed.iloc[i]
                b = closed.iloc[j]
                if max(a["_from"], b["_from"]) < min(a["_to"], b["_to"]):
                    issues.append(f"ID {listing_id} has overlapping periods")
                    break
    return issues[:10]  # limit output


@test("Active rows must have null scd_valid_to", "SCD Integrity")
def check_active_scd_valid_to(df: pd.DataFrame) -> list[str]:
    active = df[df["status"] == STATUS_ACTIVE]
    bad = active[active["scd_valid_to"].notna()]
    if bad.empty:
        return []
    return [
        f"{len(bad)} active row(s) have a non-null scd_valid_to (they look closed but status=active). "
        f"Sample IDs: {bad['id'].head(5).tolist()}"
    ]


@test("Closed rows (updated/deleted) must have non-null scd_valid_to", "SCD Integrity")
def check_closed_scd_valid_to(df: pd.DataFrame) -> list[str]:
    closed = df[df["status"].isin({STATUS_UPDATED, STATUS_DELETED})]
    bad = closed[closed["scd_valid_to"].isna()]
    if bad.empty:
        return []
    return [
        f"{len(bad)} closed row(s) have null scd_valid_to. "
        f"Sample IDs: {bad['id'].head(5).tolist()}"
    ]


@test("scd_valid_from <= scd_valid_to for closed rows", "SCD Integrity")
def check_date_order(df: pd.DataFrame) -> list[str]:
    closed = df[df["scd_valid_to"].notna()].copy()
    if closed.empty:
        return []
    closed["_from"] = pd.to_datetime(closed["scd_valid_from"], errors="coerce")
    closed["_to"] = pd.to_datetime(closed["scd_valid_to"], errors="coerce")
    bad = closed[closed["_from"] > closed["_to"]]
    if bad.empty:
        return []
    return [
        f"{len(bad)} rows where scd_valid_from > scd_valid_to (time went backwards). "
        f"Sample IDs: {bad['id'].head(5).tolist()}"
    ]


@test("first_seen_date <= scd_valid_from", "SCD Integrity")
def check_first_seen_vs_valid_from(df: pd.DataFrame) -> list[str]:
    d = df.copy()
    d["_fsd"] = pd.to_datetime(d["first_seen_date"], errors="coerce")
    d["_svf"] = pd.to_datetime(d["scd_valid_from"], errors="coerce")
    bad = d[d["_fsd"] > d["_svf"]]
    if bad.empty:
        return []
    return [
        f"{len(bad)} rows where first_seen_date > scd_valid_from. "
        f"Sample IDs: {bad['id'].head(5).tolist()}"
    ]


@test("No duplicate active rows per listing ID", "SCD Integrity")
def check_no_duplicate_active(df: pd.DataFrame) -> list[str]:
    active = df[df["status"] == STATUS_ACTIVE]
    dupes = active[active.duplicated(subset="id", keep=False)]
    if dupes.empty:
        return []
    dup_ids = dupes["id"].unique()
    return [
        f"{len(dup_ids)} listing ID(s) have more than one active row — SCD integrity broken. "
        f"Duplicate IDs: {list(dup_ids[:10])}"
    ]


@test("scd_valid_from not in the future", "SCD Integrity")
def check_valid_from_not_future(df: pd.DataFrame) -> list[str]:
    today = date.today()
    d = df.copy()
    d["_svf"] = pd.to_datetime(d["scd_valid_from"], errors="coerce").dt.date
    bad = d[d["_svf"] > today]
    if bad.empty:
        return []
    return [
        f"{len(bad)} rows have scd_valid_from in the future (> {today}). "
        f"Sample values: {d.loc[bad.index, 'scd_valid_from'].head(5).tolist()}"
    ]


# @test("SCD lifecycle is consistent per ID", "SCD Integrity")
# def check_scd_lifecycle(df: pd.DataFrame) -> list[str]:
#     """
#     For any listing that has both updated/deleted rows AND active rows,
#     the active row's scd_valid_from must be >= the closed row's scd_valid_to.
#     Also: a deleted listing should not also have an active row.
#     """
#     issues = []
#     for listing_id, group in df.groupby('id'):
#         statuses = set(group['status'].unique())

#         # A deleted listing should not simultaneously be active
#         if STATUS_DELETED in statuses and STATUS_ACTIVE in statuses:
#             issues.append(f"ID {listing_id}: has both 'deleted' and 'active' rows simultaneously")

#     # Truncate to avoid giant output
#     if len(issues) > 20:
#         issues = issues[:20] + [f"… and {len(issues) - 20} more"]
#     return issues


# ══════════════════════════════════════════════════════════════════════════════
# ── Category 3: Null / Completeness ───────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════


@test("id must never be null", "Completeness")
def check_id_not_null(df: pd.DataFrame) -> list[str]:
    bad = df[df["id"].isna()]
    return [f"{len(bad)} rows with null 'id'"] if not bad.empty else []


@test("title must never be null", "Completeness")
def check_title_not_null(df: pd.DataFrame) -> list[str]:
    bad = df[df["title"].isna() | (df["title"].astype(str).str.strip() == "")]
    return [f"{len(bad)} rows with null or empty title"] if not bad.empty else []


@test("SCD date fields must never be null", "Completeness")
def check_scd_dates_not_null(df: pd.DataFrame) -> list[str]:
    issues = []
    for col in ["first_seen_date", "scd_valid_from", "status"]:
        bad = df[df[col].isna()]
        if not bad.empty:
            issues.append(f"{len(bad)} rows with null '{col}'")
    return issues


@test("Price null rate for active listings", "Completeness", severity="warning")
def check_price_null_rate(df: pd.DataFrame) -> list[str]:
    active = df[df["status"] == STATUS_ACTIVE]
    if active.empty:
        return []
    null_rate = active["Price"].isna().mean()
    if null_rate > 0.10:
        return [f"{null_rate:.1%} of active listings have null Price (threshold: 10%)"]
    return []


# ══════════════════════════════════════════════════════════════════════════════
# ── Category 4: Value Sanity ──────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════


@test("Price within plausible range (EGP)", "Value Sanity")
def check_price_range(df: pd.DataFrame) -> list[str]:
    priced = df[df["Price"].notna()]
    if priced.empty:
        return []
    issues = []
    too_low = priced[priced["Price"] < PRICE_MIN_EGP]
    too_high = priced[priced["Price"] > PRICE_MAX_EGP]
    if not too_low.empty:
        issues.append(
            f"{len(too_low)} rows with Price < {PRICE_MIN_EGP:,} EGP (possibly junk). "
            f"Min seen: {priced['Price'].min():,.0f}"
        )
    if not too_high.empty:
        issues.append(
            f"{len(too_high)} rows with Price > {PRICE_MAX_EGP:,} EGP. "
            f"Max seen: {priced['Price'].max():,.0f}"
        )
    return issues


@test("Year within plausible range", "Value Sanity")
def check_year_range(df: pd.DataFrame) -> list[str]:
    yeard = df[df["Year"].notna()]
    if yeard.empty:
        return []
    issues = []
    too_old = yeard[yeard["Year"] < YEAR_MIN]
    too_new = yeard[yeard["Year"] > YEAR_MAX]
    if not too_old.empty:
        issues.append(
            f"{len(too_old)} rows with Year < {YEAR_MIN}. Min: {yeard['Year'].min():.0f}"
        )
    if not too_new.empty:
        issues.append(
            f"{len(too_new)} rows with Year > {YEAR_MAX}. Max: {yeard['Year'].max():.0f}"
        )
    return issues


@test("Kilometers within plausible range", "Value Sanity")
def check_km_range(df: pd.DataFrame) -> list[str]:
    kmd = df[df["Kilometers"].notna()]
    if kmd.empty:
        return []
    issues = []
    negative = kmd[kmd["Kilometers"] < 0]
    extreme = kmd[kmd["Kilometers"] > KM_MAX]
    if not negative.empty:
        issues.append(f"{len(negative)} rows with negative Kilometers")
    if not extreme.empty:
        issues.append(
            f"{len(extreme)} rows with Kilometers > {KM_MAX:,}. "
            f"Max: {kmd['Kilometers'].max():,.0f}"
        )
    return issues


@test("Power (hp) within plausible range", "Value Sanity", severity="warning")
def check_hp_range(df: pd.DataFrame) -> list[str]:
    col = "Power (hp)"
    if col not in df.columns:
        return []
    hpd = df[df[col].notna()]
    bad = hpd[(hpd[col] <= 0) | (hpd[col] > HP_MAX)]
    if bad.empty:
        return []
    return [
        f"{len(bad)} rows with implausible Power (hp) values (0 < hp <= {HP_MAX} expected). "
        f"Range seen: {hpd[col].min():.0f} – {hpd[col].max():.0f}"
    ]


@test("Engine Capacity (CC) within plausible range", "Value Sanity", severity="warning")
def check_cc_range(df: pd.DataFrame) -> list[str]:
    col = "Engine Capacity (CC)"
    if col not in df.columns:
        return []
    ccd = df[df[col].notna()]
    bad = ccd[(ccd[col] <= 0) | (ccd[col] > CC_MAX)]
    if bad.empty:
        return []
    return [
        f"{len(bad)} rows with implausible Engine Capacity values (0 < CC <= {CC_MAX} expected). "
        f"Range seen: {ccd[col].min():.0f} – {ccd[col].max():.0f}"
    ]


@test("Date strings are valid ISO-8601 (YYYY-MM-DD)", "Value Sanity")
def check_date_formats(df: pd.DataFrame) -> list[str]:
    issues = []
    for col in ["first_seen_date", "scd_valid_from", "scd_valid_to", "_ingestionDate"]:
        if col not in df.columns:
            continue
        non_null = df[df[col].notna()][col].astype(str)
        parsed = pd.to_datetime(non_null, format=DATE_FMT, errors="coerce")
        bad_count = parsed.isna().sum()
        if bad_count:
            issues.append(
                f"Column '{col}': {bad_count} values that don't parse as YYYY-MM-DD. "
                f"Sample: {non_null[parsed.isna()].head(3).tolist()}"
            )
    return issues


@test("No dates before pipeline launch era", "Value Sanity")
def check_no_ancient_dates(df: pd.DataFrame) -> list[str]:
    issues = []
    threshold = pd.to_datetime(MIN_LAUNCH_DATE)
    for col in ["first_seen_date", "scd_valid_from"]:
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce")
        bad = df[parsed < threshold]
        if not bad.empty:
            issues.append(
                f"Column '{col}': {len(bad)} dates before {MIN_LAUNCH_DATE}. "
                f"Min: {parsed.min().date()}"
            )
    return issues


@test("feature_* columns are boolean dtype", "Value Sanity", severity="warning")
def check_feature_dtypes(df: pd.DataFrame) -> list[str]:
    """
    Known issue: pd.concat / align_schemas can silently widen boolean → object.
    This test surfaces that drift so it can be fixed in the Lambda.
    """
    feature_cols = [c for c in df.columns if c.startswith("feature_")]
    issues = []
    for col in feature_cols:
        dtype = str(df[col].dtype)
        if dtype not in ("boolean", "bool"):
            # Check for the string "True"/"False" symptom
            sample_vals = df[col].dropna().astype(str).unique()[:5].tolist()
            issues.append(
                f"Column '{col}' has dtype '{dtype}' instead of boolean. "
                f"Sample values: {sample_vals}"
            )
    return issues


@test(
    "feature_* values are only True/False/NaN (no string 'True')",
    "Value Sanity",
    severity="warning",
)
def check_feature_no_string_true(df: pd.DataFrame) -> list[str]:
    feature_cols = [c for c in df.columns if c.startswith("feature_")]
    issues = []
    for col in feature_cols:
        if df[col].dtype == object:
            string_trues = (
                df[col].isin(["True", "False", "true", "false", "TRUE", "FALSE"]).sum()
            )
            if string_trues > 0:
                issues.append(
                    f"'{col}': {string_trues} values stored as string 'True'/'False' "
                    f"instead of native boolean. Athena queries need WHERE {col} = 'True'."
                )
    return issues


# ══════════════════════════════════════════════════════════════════════════════
# ── Category 5: Business Logic ────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════


@test("Active listing count is non-zero", "Business Logic")
def check_active_count(df: pd.DataFrame) -> list[str]:
    n = (df["status"] == STATUS_ACTIVE).sum()
    if n == 0:
        return [
            "Zero active listings — something is very wrong (mass-delisting or bad data?)"
        ]
    return []


@test("Active listings must not have scd_valid_to set", "Business Logic")
def check_active_not_closed(df: pd.DataFrame) -> list[str]:
    # Duplicate of SCD integrity check, but framed as business rule
    active_with_to = df[(df["status"] == STATUS_ACTIVE) & df["scd_valid_to"].notna()]
    if active_with_to.empty:
        return []
    return [
        f"{len(active_with_to)} active listings already have a closing date — "
        f"they'll never show up in 'current inventory' Athena queries"
    ]


@test("Deleted listings should have scd_valid_to set", "Business Logic")
def check_deleted_have_to(df: pd.DataFrame) -> list[str]:
    deleted_without_to = df[
        (df["status"] == STATUS_DELETED) & df["scd_valid_to"].isna()
    ]
    if deleted_without_to.empty:
        return []
    return [
        f"{len(deleted_without_to)} deleted listings have no closing date — "
        f"days_to_sell calculation will break for these rows"
    ]


@test("No listing delisted before it was first seen", "Business Logic")
def check_delisted_after_seen(df: pd.DataFrame) -> list[str]:
    deleted = df[(df["status"] == STATUS_DELETED) & df["scd_valid_to"].notna()].copy()
    if deleted.empty:
        return []
    deleted["_fsd"] = pd.to_datetime(deleted["first_seen_date"], errors="coerce")
    deleted["_to"] = pd.to_datetime(deleted["scd_valid_to"], errors="coerce")
    bad = deleted[deleted["_to"] < deleted["_fsd"]]
    if bad.empty:
        return []
    return [
        f"{len(bad)} deleted listings have scd_valid_to < first_seen_date. "
        f"Sample IDs: {bad['id'].head(5).tolist()}"
    ]


@test(
    "Listings active for unreasonably long time", "Business Logic", severity="warning"
)
def check_suspiciously_old_actives(df: pd.DataFrame) -> list[str]:
    """Flag listings active for more than 365 days — possible data issue or real outlier."""
    active = df[df["status"] == STATUS_ACTIVE].copy()
    if active.empty:
        return []
    today = pd.Timestamp(date.today())
    active["_fsd"] = pd.to_datetime(active["first_seen_date"], errors="coerce")
    active["_days"] = (today - active["_fsd"]).dt.days
    old = active[active["_days"] > 365]
    if old.empty:
        return []
    return [
        f"{len(old)} active listings have been live for more than 365 days. "
        f"Oldest: {active['_days'].max()} days. Could be genuine or a data gap issue."
    ]


@test(
    "No impossible Kilometers for the listed Year", "Business Logic", severity="warning"
)
def check_km_vs_year(df: pd.DataFrame) -> list[str]:
    """A car can't have 500k km if it's from this year."""
    d = df[df["Year"].notna() & df["Kilometers"].notna()].copy()
    if d.empty:
        return []
    current_year = date.today().year
    d["_age"] = current_year - d["Year"]
    # Assume max 60,000 km/year as upper ceiling
    d["_km_cap"] = d["_age"].clip(lower=1) * 60_000
    bad = d[(d["_age"] >= 0) & (d["Kilometers"] > d["_km_cap"])]
    if bad.empty:
        return []
    return [
        f"{len(bad)} listings have Kilometers implausibly high for their model year "
        f"(>60k km/year assumed max). "
        f"Sample: Year={d.loc[bad.index, 'Year'].iloc[0]:.0f}, "
        f"Km={d.loc[bad.index, 'Kilometers'].iloc[0]:,.0f}"
    ]


# ══════════════════════════════════════════════════════════════════════════════
# ── Category 6: Duplicate / Uniqueness ────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════


@test("No fully-duplicate rows", "Uniqueness")
def check_no_duplicate_rows(df: pd.DataFrame) -> list[str]:
    dupes = df.duplicated(keep=False).sum()
    if dupes == 0:
        return []
    return [f"{dupes} fully-duplicate rows detected (same values in every column)"]


@test("(id, scd_valid_from) combination is unique", "Uniqueness")
def check_id_valid_from_unique(df: pd.DataFrame) -> list[str]:
    """Each version of a listing has a unique (id, scd_valid_from) key."""
    dupes = df.duplicated(subset=["id", "scd_valid_from"], keep=False)
    if not dupes.any():
        return []
    n = dupes.sum()
    sample = df.loc[dupes, ["id", "scd_valid_from"]].head(5).values.tolist()
    return [f"{n} rows share the same (id, scd_valid_from). " f"Sample: {sample}"]


# ══════════════════════════════════════════════════════════════════════════════
# ── Runner ─────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════


def load_parquet(path: str) -> pd.DataFrame:
    if path.startswith("s3://"):
        if not _HAS_BOTO3:
            raise RuntimeError("boto3 is required to load from S3. pip install boto3")
        s3 = boto3.client("s3")
        path_stripped = path[len("s3://") :]
        bucket, key = path_stripped.split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        return pd.read_parquet(buf)
    else:
        return pd.read_parquet(path)


def run_tests(df: pd.DataFrame, verbose: bool = False) -> dict:
    results = []
    category_order = [
        "Schema",
        "SCD Integrity",
        "Completeness",
        "Value Sanity",
        "Business Logic",
        "Uniqueness",
    ]

    for cat in category_order:
        cat_tests = [t for t in _TESTS if t["category"] == cat]
        for t in cat_tests:
            try:
                issues = t["fn"](df)
                passed = len(issues) == 0
                results.append(
                    {
                        "category": cat,
                        "name": t["name"],
                        "severity": t["severity"],
                        "passed": passed,
                        "issues": issues,
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "category": cat,
                        "name": t["name"],
                        "severity": t["severity"],
                        "passed": False,
                        "issues": [f"Test threw an exception: {exc}"],
                    }
                )

    return results


def print_report(results: list[dict], df: pd.DataFrame):
    PASS = "\033[92m✓\033[0m"
    FAIL = "\033[91m✗\033[0m"
    WARN = "\033[93m⚠\033[0m"

    total = len(results)
    passed = sum(1 for r in results if r["passed"])
    errors = sum(1 for r in results if not r["passed"] and r["severity"] == "error")
    warnings = sum(1 for r in results if not r["passed"] and r["severity"] == "warning")

    print("\n" + "═" * 70)
    print("  GOLD LAYER DATA QUALITY REPORT")
    print(f"  {date.today()}  |  {len(df):,} rows  |  {len(df.columns)} columns")
    print("═" * 70)

    active_n = (df["status"] == "active").sum()
    updated_n = (df["status"] == "updated").sum()
    deleted_n = (df["status"] == "deleted").sum()
    print(
        f"\n  Snapshot stats: active={active_n:,}  updated={updated_n:,}  deleted={deleted_n:,}\n"
    )

    current_cat = None
    for r in results:
        if r["category"] != current_cat:
            current_cat = r["category"]
            print(f"\n  ── {current_cat} " + "─" * (50 - len(current_cat)))

        if r["passed"]:
            icon = PASS
        elif r["severity"] == "warning":
            icon = WARN
        else:
            icon = FAIL

        print(f"  {icon}  {r['name']}")
        if not r["passed"]:
            for issue in r["issues"]:
                wrapped = textwrap.wrap(issue, width=62)
                for i, line in enumerate(wrapped):
                    prefix = "       → " if i == 0 else "         "
                    print(f"{prefix}{line}")

    print("\n" + "═" * 70)
    print(
        f"  Results: {passed}/{total} passed  |  {errors} error(s)  |  {warnings} warning(s)"
    )
    print("═" * 70 + "\n")

    return errors  # non-zero exit code if hard failures exist


def main():
    parser = argparse.ArgumentParser(description="Gold layer data quality tests")
    parser.add_argument(
        "--path",
        default=os.environ.get("GOLD_PARQUET_PATH", ""),
        help="Path to gold Parquet file (local or s3://)",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if not args.path:
        print("ERROR: Provide --path or set GOLD_PARQUET_PATH env var")
        sys.exit(1)

    print(f"\nLoading: {args.path}")
    df = load_parquet(args.path)
    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")

    results = run_tests(df, verbose=args.verbose)
    error_count = print_report(results, df)
    sys.exit(1 if error_count > 0 else 0)


if __name__ == "__main__":
    main()
