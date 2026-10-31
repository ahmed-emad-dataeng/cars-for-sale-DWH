from unittest.mock import MagicMock

import pandas as pd
import pytest

DAY = "2026-03-10"


def car(car_id, extra_fields=None, **overrides):
    return {
        "id": car_id,
        "externalID": f"ext{car_id}",
        "title": f"Car {car_id}",
        "slug_l1": f"car-{car_id}",
        "description_l1": "A car",
        "formattedExtraFields": extra_fields or [],
        "createdAt": 1773100800,
        "updatedAt": 1773187200,
        **overrides,
    }


def field(name, value):
    return {"name_l1": name, "formattedValue_l1": value}


# ── merge_cars ────────────────────────────────────────────────────────────────
def test_merge_keeps_the_newest_pass_on_duplicate_ids(bronze_to_silver):
    newest = {"cars": [{"id": 1, "from": "newest"}]}
    oldest = {"cars": [{"id": 1, "from": "oldest"}, {"id": 2, "from": "oldest"}]}

    merged = bronze_to_silver.merge_cars(newest, oldest)

    assert merged == [{"id": 1, "from": "newest"}, {"id": 2, "from": "oldest"}]


# ── transform ─────────────────────────────────────────────────────────────────
def test_transform_pivots_extra_fields_into_columns(bronze_to_silver):
    cars = [car(1, [field("Brand", "Kia")]), car(2, [field("Model", "Rio")])]

    result, all_keys = bronze_to_silver.transform(cars, DAY)

    assert all_keys == {"Brand", "Model"}
    assert result[0]["Brand"] == "Kia"
    assert result[0]["Model"] is None
    assert result[1]["Model"] == "Rio"


def test_transform_turns_extra_features_into_feature_columns(bronze_to_silver):
    cars = [car(1, [field("Extra Features", ["ABS", "Sunroof"])])]

    result, _ = bronze_to_silver.transform(cars, DAY)

    assert result[0]["feature_ABS"] is True
    assert result[0]["feature_Sunroof"] is True
    assert "Extra Features" not in result[0]


def test_transform_drops_car_category_and_adds_metadata(bronze_to_silver):
    result, _ = bronze_to_silver.transform(
        [car(1, [field("Car Category", "Used")])], DAY
    )

    assert "Car Category" not in result[0]
    assert result[0]["_ingestionDate"] == DAY
    assert result[0]["slug"] == "car-1"
    assert result[0]["_sourceURL"] == (
        "https://www.dubizzle.com.eg/ad/car-1-IDext1.html"
    )


# ── change_dtypes ─────────────────────────────────────────────────────────────
def test_change_dtypes_parses_formatted_price(bronze_to_silver):
    df = bronze_to_silver.change_dtypes([{"id": 1, "Price": "1,250,000.00"}])

    assert df["Price"].iloc[0] == 1250000
    assert df["Price"].dtype == pd.Float64Dtype()


def test_change_dtypes_keeps_a_missing_price_null(bronze_to_silver):
    df = bronze_to_silver.change_dtypes(
        [{"id": 1, "Price": None}, {"id": 2, "Price": "5"}]
    )

    assert pd.isna(df["Price"].iloc[0])


def test_change_dtypes_coerces_bad_numbers_to_null(bronze_to_silver):
    df = bronze_to_silver.change_dtypes(
        [{"id": 1, "Kilometers": "not a number"}, {"id": 2, "Kilometers": "1000"}]
    )

    assert pd.isna(df["Kilometers"].iloc[0])
    assert df["Kilometers"].iloc[1] == 1000
    assert df["Kilometers"].dtype == pd.Float64Dtype()


def test_change_dtypes_converts_unix_timestamps_to_iso_utc(bronze_to_silver):
    df = bronze_to_silver.change_dtypes([{"id": 1, "createdAt": 0, "updatedAt": 86400}])

    assert df["createdAt"].iloc[0] == "1970-01-01T00:00:00+00:00"
    assert df["updatedAt"].iloc[0] == "1970-01-02T00:00:00+00:00"


def test_change_dtypes_leaves_unknown_features_null_not_false(bronze_to_silver):
    df = bronze_to_silver.change_dtypes([{"id": 1, "feature_ABS": True}, {"id": 2}])

    assert df["feature_ABS"].dtype == pd.BooleanDtype()
    assert df["feature_ABS"].iloc[0]
    assert pd.isna(df["feature_ABS"].iloc[1])


def test_change_dtypes_drops_duplicate_ids(bronze_to_silver):
    df = bronze_to_silver.change_dtypes([{"id": 1}, {"id": 1}])

    assert len(df) == 1


# ── lambda_handler: the sanity gate before Gold ───────────────────────────────
@pytest.fixture
def stubbed_b2s(bronze_to_silver, monkeypatch):
    def with_cars(n_cars):
        bronze = {"cars": [car(i, [field("Price", "100")]) for i in range(n_cars)]}
        monkeypatch.setattr(
            bronze_to_silver, "extract_bronze", lambda day: (bronze, {"cars": []})
        )
        return bronze_to_silver

    invoke = MagicMock()
    monkeypatch.setattr(bronze_to_silver, "invoke_next", invoke)
    monkeypatch.setattr(bronze_to_silver, "save_silver", lambda df, cars, day: "key")
    monkeypatch.setenv("SILVER_TO_GOLD_FUNCTION_NAME", "Silver-to-Gold")
    monkeypatch.setenv("MIN_SILVER_ROWS", "3")
    return with_cars, invoke


def test_gate_invokes_gold_when_there_are_enough_rows(stubbed_b2s):
    with_cars, invoke = stubbed_b2s

    with_cars(3).lambda_handler({"start_date": DAY, "end_date": DAY}, None)

    invoke.assert_called_once_with(
        "Silver-to-Gold", {"start_date": DAY, "end_date": DAY}
    )


def test_gate_blocks_gold_on_a_thin_scrape(stubbed_b2s, capsys):
    """A partial scrape must never reach Gold: it would look like mass delisting."""
    with_cars, invoke = stubbed_b2s

    with_cars(2).lambda_handler({"start_date": DAY, "end_date": DAY}, None)

    invoke.assert_not_called()
    assert "Skipping Gold invocation" in capsys.readouterr().out


def test_gate_blocks_gold_when_a_day_fails(stubbed_b2s, bronze_to_silver, monkeypatch):
    _, invoke = stubbed_b2s

    def boom(day):
        raise RuntimeError("no bronze")

    monkeypatch.setattr(bronze_to_silver, "extract_bronze", boom)
    result = bronze_to_silver.lambda_handler({"start_date": DAY, "end_date": DAY}, None)

    assert result["body"]["failed"] == 1
    invoke.assert_not_called()


def test_gate_defaults_to_500_rows(stubbed_b2s, monkeypatch):
    with_cars, invoke = stubbed_b2s
    monkeypatch.delenv("MIN_SILVER_ROWS")

    with_cars(499).lambda_handler({"start_date": DAY, "end_date": DAY}, None)

    invoke.assert_not_called()
