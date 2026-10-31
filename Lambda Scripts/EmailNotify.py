"""
DailyDigestEmail Lambda
────────────────────────────────────────────────────────────────────────────────
Triggered by EventBridge ~30 min after the scrape CRON.
Reads today's CloudWatch logs from all 4 pipeline functions,
parses key metrics from their print() output, and sends a
formatted summary email via SNS.

Required environment variables:
    SCRAPE_LOG_GROUP       e.g. /aws/lambda/DubizzleScrapeDay
    B2S_LOG_GROUP          e.g. /aws/lambda/Bronze-to-Silver
    S2G_LOG_GROUP          e.g. /aws/lambda/Silver-to-Gold
    DBT_LOG_GROUP          e.g. /aws/lambda/DbtRunner (optional — section
                           shows "not configured" if unset)
    SNS_TOPIC_ARN          ARN of the SNS topic subscribed to your email
    PIPELINE_TZ_OFFSET     Hours offset from UTC for display (e.g. "3" for Cairo = UTC+3)

Required IAM permissions:
    logs:FilterLogEvents   on each of the log groups
    logs:DescribeLogStreams on each of the log groups
    sns:Publish            on the SNS topic
"""

import boto3
import os
import re
from datetime import datetime, timezone, timedelta

logs = boto3.client("logs")
sns = boto3.client("sns")


# ── Helpers ───────────────────────────────────────────────────────────────────


def get_today_log_events(
    log_group: str, date_str: str, tz_offset: int = 0
) -> list[str]:
    """
    Pull all log events for today from the given log group.
    `date_str` is a local-calendar date (per PIPELINE_TZ_OFFSET), so the window
    is built from local midnight → local midnight, not UTC midnight → UTC
    midnight — otherwise a run that lands near the UTC day boundary (e.g. the
    last Lambda in the chain, which runs latest) can fall just outside the
    window while everything upstream of it still matches.
    """
    # Build time window in milliseconds
    local_midnight = datetime.strptime(date_str, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    today = local_midnight - timedelta(hours=tz_offset)
    start_ms = int(today.timestamp() * 1000)
    end_ms = int((today + timedelta(days=1)).timestamp() * 1000) - 1

    messages = []
    kwargs = {
        "logGroupName": log_group,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 10000,
    }

    try:
        while True:
            resp = logs.filter_log_events(**kwargs)
            for event in resp.get("events", []):
                messages.append(event["message"].strip())
            next_token = resp.get("nextToken")
            if not next_token:
                break
            kwargs["nextToken"] = next_token
    except logs.exceptions.ResourceNotFoundException:
        # Log group doesn't exist yet (e.g. Lambda never invoked)
        pass
    except Exception as e:
        messages.append(f"[ERROR reading logs: {e}]")

    return messages


def first_match(lines: list[str], pattern: str, group: int = 1) -> str | None:
    """Return the first regex match across log lines, or None."""
    rx = re.compile(pattern)
    for line in lines:
        m = rx.search(line)
        if m:
            return m.group(group)
    return None


def has_error(lines: list[str]) -> list[str]:
    """Return any lines that look like errors or exceptions."""
    error_patterns = re.compile(
        r"(error|exception|traceback|✗|failed|raise |❌)", re.IGNORECASE
    )
    return [l for l in lines if error_patterns.search(l)]


def invoked_today(lines: list[str]) -> bool:
    """Was the Lambda invoked at all today?"""
    return len(lines) > 0


# ── Per-function parsers ──────────────────────────────────────────────────────


def parse_scrape(lines: list[str]) -> dict:
    """
    Parse DubizzleScrapeDay.py logs.
    Looks for:
        "→ {N} ads from newest call"
        "→ {N} ads from oldest call"
        "→ Invoked {function_name} asynchronously"
        "Saved to s3://..."
    """
    newest = first_match(lines, r"(\d+) ads from newest call")
    oldest = first_match(lines, r"(\d+) ads from oldest call")
    chained = any("Invoked" in l and "asynchronously" in l for l in lines)
    saved = any("Saved to s3://" in l for l in lines)
    errors = has_error(lines)

    return {
        "invoked": invoked_today(lines),
        "newest": newest,
        "oldest": oldest,
        "chained": chained,
        "saved": saved,
        "errors": errors,
    }


def parse_bronze_to_silver(lines: list[str]) -> dict:
    """
    Parse Bronze-to-Silver.py logs.
    Looks for:
        "Processing {N} day(s): ..."
        "✓ {date} — {N} cars"
        "✗ {date} — {error}"
        "⚠ Skipping Gold invocation"
        "→ Invoked {function_name} asynchronously"
        "MIN_SILVER_ROWS threshold"
    """
    processing_match = first_match(lines, r"Processing (\d+) day\(s\)")
    n_days = int(processing_match) if processing_match else None

    success_days = re.findall(r"✓ (\S+) — (\d+) cars", "\n".join(lines))
    failed_days = re.findall(r"✗ (\S+) — (.+)", "\n".join(lines))

    gate_blocked = any("Skipping Gold invocation" in l for l in lines)
    gate_reason = None
    if gate_blocked:
        gate_match = first_match(lines, r"Skipping Gold invocation — (.+)")
        gate_reason = gate_match

    chained = any("Invoked" in l and "asynchronously" in l for l in lines)
    errors = has_error(lines)

    # Total cars across all successful days
    total_cars = sum(int(n) for _, n in success_days) if success_days else None

    return {
        "invoked": invoked_today(lines),
        "n_days": n_days,
        "success_days": success_days,  # list of (date, n_cars)
        "failed_days": failed_days,  # list of (date, error_msg)
        "total_cars": total_cars,
        "gate_blocked": gate_blocked,
        "gate_reason": gate_reason,
        "chained": chained,
        "errors": errors,
    }


def parse_silver_to_gold(lines: list[str]) -> dict:
    """
    Parse Silver-to-Gold.py logs.
    Looks for:
        "Silver rows : {N}"
        "Gold rows   : {N}"
        "New listings   : {N}"
        "Updated rows   : {N}"
        "Deleted rows   : {N}"
        "✓ Gold saved → s3://... ({N} total rows)"
        "✗ {date} — {error}"
    """
    silver_rows = first_match(lines, r"Silver rows\s*:\s*(\d+)")
    gold_rows = first_match(lines, r"Gold rows\s*:\s*(\d+)")
    new_listings = first_match(lines, r"New listings\s*:\s*(\d+)")
    updated = first_match(lines, r"Updated rows\s*:\s*(\d+)")
    deleted = first_match(lines, r"Deleted rows\s*:\s*(\d+)")
    total_rows = first_match(lines, r"Gold saved.*\((\d+(?:,\d+)?) total rows\)")
    saved = any("Gold saved" in l and "✓" in l for l in lines)
    failed_days = re.findall(r"✗ (\S+) — (.+)", "\n".join(lines))
    errors = has_error(lines)

    return {
        "invoked": invoked_today(lines),
        "silver_rows": silver_rows,
        "gold_rows": gold_rows,
        "new_listings": new_listings,
        "updated": updated,
        "deleted": deleted,
        "total_rows": total_rows,
        "saved": saved,
        "failed_days": failed_days,
        "errors": errors,
    }


def parse_dbt(lines: list[str]) -> dict:
    """
    Parse DbtRunner logs.
    Looks for:
        "DBT SUMMARY: passed=N warned=N failed=N errored=N skipped=N"
        "DBT FAIL  <test_name> (N rows)"
        "DBT WARN  <test_name> (N rows)"
        "DBT ERROR <test_name> — <message>"
    """
    summary_rx = r"DBT SUMMARY: passed=(\d+) warned=(\d+) failed=(\d+) errored=(\d+)"
    passed = first_match(lines, summary_rx, group=1)
    warned = first_match(lines, summary_rx, group=2)
    failed = first_match(lines, summary_rx, group=3)
    errored = first_match(lines, summary_rx, group=4)

    joined = "\n".join(lines)
    failing_tests = re.findall(r"DBT FAIL\s+(\S+)(?: \((\d+) rows\))?", joined)
    warning_tests = re.findall(r"DBT WARN\s+(\S+)(?: \((\d+) rows\))?", joined)
    error_tests = re.findall(r"DBT ERROR\s+(\S+)", joined)

    # A failed FilterLogEvents call (e.g. missing IAM on this log group) gets
    # swallowed into the message list by get_today_log_events. Without this the
    # section looks "invoked" with every counter None — i.e. silently all dashes.
    fetch_error = next((l for l in lines if l.startswith("[ERROR reading logs:")), None)

    return {
        "invoked": invoked_today(lines),
        "fetch_error": fetch_error,
        "passed": passed,
        "warned": warned,
        "failed": failed,
        "errored": errored,
        "failing_tests": failing_tests,  # list of (name, n_rows)
        "warning_tests": warning_tests,  # list of (name, n_rows)
        "error_tests": error_tests,  # list of names
    }


# ── Email formatter ───────────────────────────────────────────────────────────


def fmt(val, suffix="", fallback="—") -> str:
    """Format a value with optional suffix, or return fallback."""
    if val is None:
        return fallback
    return f"{int(val):,}{suffix}" if str(val).isdigit() else f"{val}{suffix}"


def build_email(
    date_str: str, scrape: dict, b2s: dict, s2g: dict, dbt: dict | None = None
) -> tuple[str, str]:
    """
    Returns (subject, body) for the SNS email.
    """
    dbt_ok = dbt is None or (  # section not configured — don't fail the digest over it
        dbt["invoked"]
        and not dbt.get("fetch_error")
        and dbt["passed"] is not None
        and not dbt["failing_tests"]
        and not dbt["error_tests"]
    )

    # Determine overall pipeline status
    all_ok = (
        scrape["invoked"]
        and scrape["saved"]
        and not scrape["errors"]
        and b2s["invoked"]
        and not b2s["gate_blocked"]
        and not b2s["errors"]
        and s2g["invoked"]
        and s2g["saved"]
        and not s2g["errors"]
        and dbt_ok
    )
    status_icon = "✅" if all_ok else "⚠️"

    subject = f"{status_icon} Dubizzle Pipeline — {date_str}"

    sep = "─" * 52

    # ── Section 1: Scrape ─────────────────────────────────────────────────────
    scrape_ok = scrape["invoked"] and scrape["saved"] and not scrape["errors"]
    scrape_icon = "✅" if scrape_ok else ("⚠️" if scrape["invoked"] else "❌")

    scrape_block = f"""
{scrape_icon}  DubizzleScrapeDay
   Newest ads fetched  : {fmt(scrape['newest'])}
   Oldest ads fetched  : {fmt(scrape['oldest'])}
   Saved to S3         : {'✓' if scrape['saved'] else '✗ NOT saved'}
   Chained to B2S      : {'✓' if scrape['chained'] else '✗ NOT invoked'}"""

    if scrape["errors"]:
        scrape_block += "\n\n   ⚠️  ERRORS:\n"
        for e in scrape["errors"][:5]:
            scrape_block += f"   {e}\n"

    # ── Section 2: Bronze-to-Silver ───────────────────────────────────────────
    b2s_ok = (
        b2s["invoked"]
        and not b2s["gate_blocked"]
        and not b2s["errors"]
        and not b2s["failed_days"]
    )
    b2s_icon = "✅" if b2s_ok else ("⚠️" if b2s["invoked"] else "❌")

    b2s_block = f"""
{b2s_icon}  Bronze-to-Silver"""

    if not b2s["invoked"]:
        b2s_block += "\n   ❌ Lambda was NOT invoked today"
    else:
        b2s_block += f"""
   Days processed      : {fmt(b2s['n_days'])}
   Successful days     : {len(b2s['success_days'])}
   Total cars written  : {fmt(b2s['total_cars'])}
   Gate passed         : {'✓' if not b2s['gate_blocked'] else f"✗ BLOCKED — {b2s['gate_reason'] or 'see logs'}"}
   Chained to S2G      : {'✓' if b2s['chained'] else ('N/A (gate blocked)' if b2s['gate_blocked'] else '✗ NOT invoked')}"""

        if b2s["failed_days"]:
            b2s_block += "\n\n   Failed days:\n"
            for date, err in b2s["failed_days"][:5]:
                b2s_block += f"   • {date}: {err}\n"

        if b2s["errors"]:
            b2s_block += "\n\n   ⚠️  ERRORS:\n"
            for e in b2s["errors"][:5]:
                b2s_block += f"   {e}\n"

    # ── Section 3: Silver-to-Gold ─────────────────────────────────────────────
    s2g_ok = (
        s2g["invoked"] and s2g["saved"] and not s2g["errors"] and not s2g["failed_days"]
    )
    s2g_icon = "✅" if s2g_ok else ("⚠️" if s2g["invoked"] else "❌")

    s2g_block = f"""
{s2g_icon}  Silver-to-Gold"""

    if not s2g["invoked"]:
        s2g_block += "\n   ❌ Lambda was NOT invoked today"
    else:
        s2g_block += f"""
   Silver rows read    : {fmt(s2g['silver_rows'])}
   New listings        : {fmt(s2g['new_listings'])}
   Updated rows        : {fmt(s2g['updated'])}
   Deleted rows        : {fmt(s2g['deleted'])}
   Total Gold rows     : {fmt(s2g['total_rows'])}
   Saved to S3         : {'✓' if s2g['saved'] else '✗ NOT saved'}"""

        if s2g["failed_days"]:
            s2g_block += "\n\n   Failed days:\n"
            for date, err in s2g["failed_days"][:5]:
                s2g_block += f"   • {date}: {err}\n"

        if s2g["errors"]:
            s2g_block += "\n\n   ⚠️  ERRORS:\n"
            for e in s2g["errors"][:5]:
                s2g_block += f"   {e}\n"

    # ── Section 4: Data Quality (dbt) ─────────────────────────────────────────
    if dbt is None:
        dbt_block = "\n💤  Data Quality (dbt) — not configured"
    elif not dbt["invoked"]:
        dbt_block = "\n❌  Data Quality (dbt)\n   Runner was NOT invoked today"
    elif dbt.get("fetch_error"):
        dbt_block = (
            "\n⚠️  Data Quality (dbt)\n"
            f"   Could not read runner logs: {dbt['fetch_error']}"
        )
    elif dbt["passed"] is None:
        dbt_block = (
            "\n⚠️  Data Quality (dbt)\n"
            "   Runner logged today but never printed a DBT SUMMARY line —\n"
            "   it was likely still running (or timed out) when this digest\n"
            "   was sent. Check the DbtRunner log group directly."
        )
    else:
        dbt_icon = (
            "✅" if (not dbt["failing_tests"] and not dbt["error_tests"]) else "⚠️"
        )
        dbt_block = f"""
{dbt_icon}  Data Quality (dbt)
   Tests passed        : {fmt(dbt['passed'])}
   Warnings            : {fmt(dbt['warned'])}
   Failures            : {fmt(dbt['failed'])}
   Errors              : {fmt(dbt['errored'])}"""

        if dbt["failing_tests"]:
            dbt_block += "\n\n   Failing tests:\n"
            for name, n_rows in dbt["failing_tests"][:5]:
                rows = f" ({n_rows} rows)" if n_rows else ""
                dbt_block += f"   ✗ {name}{rows}\n"

        if dbt["warning_tests"]:
            dbt_block += "\n   Warning tests:\n"
            for name, n_rows in dbt["warning_tests"][:5]:
                rows = f" ({n_rows} rows)" if n_rows else ""
                dbt_block += f"   ⚠ {name}{rows}\n"

        if dbt["error_tests"]:
            dbt_block += "\n   Errored:\n"
            for name in dbt["error_tests"][:5]:
                dbt_block += f"   ✗ {name}\n"

    # ── Footer ────────────────────────────────────────────────────────────────
    tz_offset = int(os.environ.get("PIPELINE_TZ_OFFSET", "0"))
    local_time = datetime.now(timezone.utc) + timedelta(hours=tz_offset)
    footer = f"\nGenerated at {local_time.strftime('%Y-%m-%d %H:%M')} (UTC+{tz_offset})"

    body = f"""Daily Pipeline Summary — {date_str}
{sep}
{scrape_block}

{sep}
{b2s_block}

{sep}
{s2g_block}

{sep}
{dbt_block}

{sep}
{footer}
"""

    return subject, body


# ── Lambda handler ────────────────────────────────────────────────────────────


def lambda_handler(event, context):
    # Allow manual overrides via event payload for debugging specific dates
    tz_offset = int(os.environ.get("PIPELINE_TZ_OFFSET", "0"))
    local_today = datetime.now(timezone.utc) + timedelta(hours=tz_offset)
    date_str = event.get("date", local_today.strftime("%Y-%m-%d"))

    print(f"Digest for: {date_str}")

    scrape_log = os.environ["SCRAPE_LOG_GROUP"]
    b2s_log = os.environ["B2S_LOG_GROUP"]
    s2g_log = os.environ["S2G_LOG_GROUP"]
    dbt_log = os.environ.get("DBT_LOG_GROUP")  # optional

    # Fetch logs
    print("Fetching scrape logs...")
    scrape_lines = get_today_log_events(scrape_log, date_str, tz_offset)
    print(f"  {len(scrape_lines)} lines")

    print("Fetching Bronze-to-Silver logs...")
    b2s_lines = get_today_log_events(b2s_log, date_str, tz_offset)
    print(f"  {len(b2s_lines)} lines")

    print("Fetching Silver-to-Gold logs...")
    s2g_lines = get_today_log_events(s2g_log, date_str, tz_offset)
    print(f"  {len(s2g_lines)} lines")

    dbt = None
    if dbt_log:
        print("Fetching DbtRunner logs...")
        dbt_lines = get_today_log_events(dbt_log, date_str, tz_offset)
        print(f"  {len(dbt_lines)} lines")
        dbt = parse_dbt(dbt_lines)

    # Parse
    scrape = parse_scrape(scrape_lines)
    b2s = parse_bronze_to_silver(b2s_lines)
    s2g = parse_silver_to_gold(s2g_lines)

    # Build and send
    subject, body = build_email(date_str, scrape, b2s, s2g, dbt)

    print("Publishing to SNS...")
    print(f"Subject: {subject}")
    print(body)

    sns.publish(
        TopicArn=os.environ["SNS_TOPIC_ARN"],
        Subject=subject,
        Message=body,
    )

    return {
        "statusCode": 200,
        "body": {
            "date": date_str,
            "subject": subject,
        },
    }
