"""The digest parses log lines by regex, so these lines mirror the print() calls."""


def test_parse_scrape(email_notify):
    result = email_notify.parse_scrape(
        [
            "Fetching newest cars …",
            "→ 1200 ads from newest call",
            "→ 800 ads from oldest call",
            "Saved to s3://bronze/x.json",
            "→ Invoked Bronze-to-Silver asynchronously",
        ]
    )

    assert result["newest"] == "1200"
    assert result["oldest"] == "800"
    assert result["saved"] is True
    assert result["chained"] is True
    assert result["errors"] == []


def test_parse_bronze_to_silver_success(email_notify):
    result = email_notify.parse_bronze_to_silver(
        [
            "Processing 1 day(s): 2026-03-10 → 2026-03-10",
            "✓ 2026-03-10 — 13000 cars",
            "→ Invoked Silver-to-Gold asynchronously",
        ]
    )

    assert result["n_days"] == 1
    assert result["success_days"] == [("2026-03-10", "13000")]
    assert result["total_cars"] == 13000
    assert result["gate_blocked"] is False
    assert result["chained"] is True


def test_parse_bronze_to_silver_blocked_gate(email_notify):
    reason = "0 failed day(s), 1 day(s) below 500-row threshold"
    result = email_notify.parse_bronze_to_silver(
        [
            "✓ 2026-03-10 — 12 cars",
            f"⚠ Skipping Gold invocation — {reason}",
        ]
    )

    assert result["gate_blocked"] is True
    assert result["gate_reason"] == reason
    assert result["chained"] is False


def test_parse_silver_to_gold(email_notify):
    result = email_notify.parse_silver_to_gold(
        [
            "Silver rows : 13000",
            "Gold rows   : 132000",
            "New listings   : 400",
            "Updated rows   : 300",
            "Deleted rows   : 200",
            "✓ Gold saved → s3://gold/cars_scd.parquet  (132951 total rows)",
        ]
    )

    assert result["silver_rows"] == "13000"
    assert result["gold_rows"] == "132000"
    assert result["new_listings"] == "400"
    assert result["updated"] == "300"
    assert result["deleted"] == "200"
    assert result["total_rows"] == "132951"
    assert result["saved"] is True
    assert result["failed_days"] == []


def test_parse_silver_to_gold_failure(email_notify):
    result = email_notify.parse_silver_to_gold(["✗ 2026-03-10 — NoSuchKey"])

    assert result["saved"] is False
    assert result["failed_days"] == [("2026-03-10", "NoSuchKey")]
    assert result["errors"] == ["✗ 2026-03-10 — NoSuchKey"]


def test_parse_dbt_reads_the_runner_output_format(email_notify):
    """Same formats as summarize() and lambda_handler in dbt-runner/handler.py."""
    result = email_notify.parse_dbt(
        [
            "DBT PASS  not_null_id",
            "DBT WARN  assert_km_plausible_for_year (471 rows)",
            "DBT FAIL  assert_one_active_row_per_id (3 rows)",
            "DBT ERROR cars_scd_analytics — boom",
            "DBT SUMMARY: passed=25 warned=5 failed=1 errored=1 skipped=0",
        ]
    )

    assert result["passed"] == "25"
    assert result["warned"] == "5"
    assert result["failed"] == "1"
    assert result["errored"] == "1"
    assert result["warning_tests"] == [("assert_km_plausible_for_year", "471")]
    assert result["failing_tests"] == [("assert_one_active_row_per_id", "3")]
    assert result["error_tests"] == ["cars_scd_analytics"]
    assert result["fetch_error"] is None


def test_parse_dbt_surfaces_a_log_fetch_error(email_notify):
    """Regression (4fc63e0): this used to show as a section of silent dashes."""
    line = "[ERROR reading logs: AccessDenied]"

    result = email_notify.parse_dbt([line])

    assert result["fetch_error"] == line
    assert result["passed"] is None


def test_no_logs_means_not_invoked(email_notify):
    assert email_notify.parse_scrape([])["invoked"] is False
    assert email_notify.parse_dbt([])["invoked"] is False
