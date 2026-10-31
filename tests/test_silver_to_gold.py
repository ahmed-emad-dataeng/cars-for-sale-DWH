import io
from unittest.mock import MagicMock

import pandas as pd
import pytest

TODAY = "2026-03-10"
YESTERDAY = "2026-03-09"


def silver(*rows):
    """Rows of (id, updatedAt) or (id, updatedAt, price)."""
    return pd.DataFrame(
        [
            {"id": r[0], "updatedAt": r[1], "Price": r[2] if len(r) > 2 else 100.0}
            for r in rows
        ]
    )


def gold(*rows):
    """Rows of (id, updatedAt, status)."""
    return pd.DataFrame(
        [
            {
                "id": r[0],
                "updatedAt": r[1],
                "Price": 100.0,
                "first_seen_date": "2026-03-01",
                "scd_valid_from": "2026-03-01",
                "scd_valid_to": None if r[2] == "active" else YESTERDAY,
                "status": r[2],
            }
            for r in rows
        ]
    )


def rows_for(df, listing_id):
    return df[df["id"] == listing_id]


# ── apply_scd ─────────────────────────────────────────────────────────────────
def test_bootstrap_inserts_every_row_as_active(silver_to_gold):
    result = silver_to_gold.apply_scd(silver((1, "a"), (2, "b")), None, TODAY)

    assert len(result) == 2
    assert set(result["status"]) == {"active"}
    assert set(result["first_seen_date"]) == {TODAY}
    assert set(result["scd_valid_from"]) == {TODAY}
    assert result["scd_valid_to"].isna().all()


def test_new_listing_is_inserted_as_active(silver_to_gold):
    result = silver_to_gold.apply_scd(
        silver((1, "a"), (2, "b")), gold((1, "a", "active")), TODAY
    )

    new = rows_for(result, 2).iloc[0]
    assert new["status"] == "active"
    assert new["first_seen_date"] == TODAY
    assert new["scd_valid_from"] == TODAY
    assert pd.isna(new["scd_valid_to"])


def test_unchanged_listing_is_left_alone(silver_to_gold):
    before = gold((1, "a", "active"))
    result = silver_to_gold.apply_scd(silver((1, "a")), before.copy(), TODAY)

    assert len(result) == 1
    row = result.iloc[0]
    assert row["status"] == "active"
    assert row["scd_valid_from"] == "2026-03-01"
    assert pd.isna(row["scd_valid_to"])


def test_updated_listing_closes_old_row_and_keeps_first_seen(silver_to_gold):
    result = silver_to_gold.apply_scd(
        silver((1, "b", 90.0)), gold((1, "a", "active")), TODAY
    )

    versions = rows_for(result, 1)
    assert len(versions) == 2

    old = versions[versions["status"] == "updated"].iloc[0]
    assert old["scd_valid_to"] == TODAY
    assert old["Price"] == 100.0

    new = versions[versions["status"] == "active"].iloc[0]
    assert new["first_seen_date"] == "2026-03-01"
    assert new["scd_valid_from"] == TODAY
    assert new["Price"] == 90.0
    assert pd.isna(new["scd_valid_to"])


def test_missing_listing_is_closed_as_deleted(silver_to_gold):
    result = silver_to_gold.apply_scd(
        silver((2, "b")), gold((1, "a", "active"), (2, "b", "active")), TODAY
    )

    row = rows_for(result, 1).iloc[0]
    assert row["status"] == "deleted"
    assert row["scd_valid_to"] == TODAY
    assert rows_for(result, 2).iloc[0]["status"] == "active"


def test_closed_history_rows_are_not_touched(silver_to_gold):
    result = silver_to_gold.apply_scd(
        silver((2, "b")), gold((1, "a", "updated"), (2, "b", "active")), TODAY
    )

    row = rows_for(result, 1).iloc[0]
    assert row["status"] == "updated"
    assert row["scd_valid_to"] == YESTERDAY


def test_relisted_listing_starts_over_with_a_new_first_seen(silver_to_gold):
    """Pins current behaviour: a delisted ID that returns counts as brand new."""
    result = silver_to_gold.apply_scd(
        silver((1, "a")), gold((1, "a", "deleted")), TODAY
    )

    versions = rows_for(result, 1)
    assert len(versions) == 2
    new = versions[versions["status"] == "active"].iloc[0]
    assert new["first_seen_date"] == TODAY


def test_exactly_one_active_row_per_listing_after_merge(silver_to_gold):
    result = silver_to_gold.apply_scd(
        silver((1, "b"), (3, "c")),
        gold((1, "a", "active"), (2, "b", "active")),
        TODAY,
    )

    active = result[result["status"] == "active"]
    assert active["id"].is_unique
    assert set(active["id"]) == {1, 3}


# ── align_schemas / enforce_datatypes ─────────────────────────────────────────
def test_align_backfills_a_new_feature_as_false_in_gold(silver_to_gold):
    new = silver((1, "a"))
    new["feature_ABS"] = pd.array([True], dtype="boolean")

    _, aligned_gold = silver_to_gold.align_schemas(new, gold((2, "b", "active")))

    assert not aligned_gold["feature_ABS"].iloc[0]


def test_align_fills_columns_missing_from_silver(silver_to_gold):
    old = gold((2, "b", "active"))
    old["feature_Sunroof"] = pd.array([True], dtype="boolean")
    old["Color"] = "Red"

    aligned_silver, _ = silver_to_gold.align_schemas(silver((1, "a")), old)

    assert not aligned_silver["feature_Sunroof"].iloc[0]
    assert pd.isna(aligned_silver["Color"].iloc[0])


def test_align_does_not_copy_scd_columns_into_silver(silver_to_gold):
    aligned_silver, _ = silver_to_gold.align_schemas(
        silver((1, "a")), gold((2, "b", "active"))
    )

    assert not set(silver_to_gold.SCD_COLS) & set(aligned_silver.columns)


def test_enforce_datatypes_makes_features_nullable_boolean(silver_to_gold):
    df = pd.DataFrame({"feature_ABS": [True, None], "Price": [1.0, 2.0]})

    result = silver_to_gold.enforce_datatypes(df)

    assert result["feature_ABS"].dtype == pd.BooleanDtype()
    assert pd.isna(result["feature_ABS"].iloc[1])
    assert result["Price"].dtype == "float64"


# ── save_gold ─────────────────────────────────────────────────────────────────
def test_merged_table_survives_a_parquet_round_trip(silver_to_gold, monkeypatch):
    """Schema-evolved frames must still write: this is what Athena reads."""
    monkeypatch.setenv("GOLD_BUCKET_NAME", "gold-bucket")
    monkeypatch.setenv("GOLD_DATA_PATH", "OneBigTable")
    fake_s3 = MagicMock()
    monkeypatch.setattr(silver_to_gold, "s3", fake_s3)

    new = silver((1, "b"), (3, "c"))
    new["feature_ABS"] = pd.array([True, False], dtype="boolean")
    new["Color"] = ["Red", None]
    old = gold((1, "a", "active"), (2, "b", "active"))

    merged = silver_to_gold.apply_scd(new, old, TODAY)
    merged = silver_to_gold.enforce_datatypes(merged)
    path = silver_to_gold.save_gold(merged)

    assert path == "s3://gold-bucket/OneBigTable/cars_scd.parquet"
    body = fake_s3.put_object.call_args.kwargs["Body"]
    written = pd.read_parquet(io.BytesIO(body))
    assert len(written) == len(merged)
    assert set(written.columns) == set(merged.columns)
    assert written[written["status"] == "active"]["scd_valid_to"].isna().all()


# ── lambda_handler ────────────────────────────────────────────────────────────
@pytest.fixture
def stubbed_s2g(silver_to_gold, monkeypatch):
    monkeypatch.setenv("DBT_RUNNER_FUNCTION", "DbtRunner")
    invoke = MagicMock()
    monkeypatch.setattr(silver_to_gold, "invoke_next", invoke)
    monkeypatch.setattr(silver_to_gold, "read_silver", lambda day: silver((1, "a")))
    monkeypatch.setattr(silver_to_gold, "read_gold", lambda: None)
    monkeypatch.setattr(silver_to_gold, "save_gold", lambda df: "s3://gold/x")
    return silver_to_gold, invoke


def test_handler_chains_to_dbt_after_a_successful_save(stubbed_s2g):
    module, invoke = stubbed_s2g
    result = module.lambda_handler({"start_date": TODAY, "end_date": TODAY}, None)

    assert result["body"]["processed"] == 1
    invoke.assert_called_once_with("DbtRunner", {"trigger": "silver-to-gold"})


def test_handler_does_not_chain_when_every_day_fails(stubbed_s2g, monkeypatch):
    module, invoke = stubbed_s2g

    def boom(day):
        raise RuntimeError("no silver")

    monkeypatch.setattr(module, "read_silver", boom)
    result = module.lambda_handler({"start_date": TODAY, "end_date": TODAY}, None)

    assert result["body"]["failed"] == 1
    invoke.assert_not_called()


def test_handler_rejects_a_reversed_date_range(stubbed_s2g):
    module, _ = stubbed_s2g
    with pytest.raises(ValueError):
        module.lambda_handler({"start_date": TODAY, "end_date": YESTERDAY}, None)


def test_backfill_builds_gold_day_by_day_in_order(stubbed_s2g, monkeypatch):
    module, _ = stubbed_s2g
    seen = []

    def read_silver(day):
        seen.append(day)
        return silver((1, "a"))

    monkeypatch.setattr(module, "read_silver", read_silver)
    module.lambda_handler({"start_date": "2026-03-01", "end_date": "2026-03-03"}, None)

    assert seen == ["2026-03-01", "2026-03-02", "2026-03-03"]
