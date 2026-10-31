import gzip
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest


def hits(*names):
    return {"hits": {"hits": [{"_source": {"id": name}} for name in names]}}


def test_fetch_ads_merges_regular_elite_and_featured(scraper, monkeypatch):
    responses = [{}, hits("elite"), hits("featured"), hits("regular1", "regular2")]
    post = MagicMock()
    post.return_value.json.return_value = {"responses": responses}
    monkeypatch.setattr(scraper.requests, "post", post)

    ads = scraper.fetch_ads("desc")

    assert [ad["id"] for ad in ads] == ["regular1", "regular2", "elite", "featured"]
    assert post.call_args.kwargs["data"] == "desc-payload"


def test_fetch_ads_raises_on_http_error(scraper, monkeypatch):
    post = MagicMock()
    post.return_value.raise_for_status.side_effect = RuntimeError("HTTP 500")
    monkeypatch.setattr(scraper.requests, "post", post)

    with pytest.raises(RuntimeError):
        scraper.fetch_ads("asc")


def test_build_payload_rejects_an_unknown_order(scraper):
    with pytest.raises(ValueError):
        scraper.build_payload("sideways")


def test_build_payload_picks_the_payload_for_the_order(scraper):
    assert scraper.build_payload("desc") == "desc-payload"
    assert scraper.build_payload("asc") == "asc-payload"


@pytest.fixture
def stubbed_handler(scraper, monkeypatch):
    invoke = MagicMock()
    monkeypatch.setattr(scraper, "invoke_next", invoke)
    monkeypatch.setattr(scraper, "fetch_ads", lambda order: [{"id": order}])
    monkeypatch.setattr(scraper, "save_to_s3", lambda cars, label: f"bronze/{label}")
    monkeypatch.setenv("BRONZE_TO_SILVER_FUNCTION_NAME", "b2s")
    monkeypatch.setenv("JSONL_PREFIX", "raw/jsonl")
    return scraper, invoke


def test_jsonl_is_gzipped_lines_stamped_with_pass_and_time(scraper, monkeypatch):
    fake_s3 = MagicMock()
    monkeypatch.setattr(scraper, "s3", fake_s3)
    scraped_at = datetime(2026, 3, 10, 3, 33, tzinfo=timezone.utc)

    key = scraper.save_jsonl_to_s3(
        [{"id": 1}, {"id": 2}], "newest", scraped_at, "raw/jsonl"
    )

    assert key == "raw/jsonl/ingestion_date=2026-03-10/newest.jsonl.gz"
    body = gzip.decompress(fake_s3.put_object.call_args.kwargs["Body"]).decode()
    records = [json.loads(line) for line in body.splitlines()]
    assert [r["id"] for r in records] == [1, 2]
    assert {r["_pass"] for r in records} == {"newest"}
    assert {r["_scraped_at"] for r in records} == {scraped_at.isoformat()}


def test_handler_writes_jsonl_for_both_passes(stubbed_handler, monkeypatch):
    scraper, invoke = stubbed_handler
    save = MagicMock(return_value="key")
    monkeypatch.setattr(scraper, "save_jsonl_to_s3", save)

    scraper.lambda_handler({}, None)

    assert [call.args[1] for call in save.call_args_list] == ["newest", "oldest"]
    invoke.assert_called_once_with("b2s", {})


def test_jsonl_failure_never_blocks_the_chain(stubbed_handler, monkeypatch):
    scraper, invoke = stubbed_handler
    monkeypatch.setattr(
        scraper, "save_jsonl_to_s3", MagicMock(side_effect=RuntimeError("S3 down"))
    )

    scraper.lambda_handler({}, None)

    invoke.assert_called_once_with("b2s", {})


def test_jsonl_is_skipped_when_the_prefix_is_unset(stubbed_handler, monkeypatch):
    scraper, invoke = stubbed_handler
    monkeypatch.delenv("JSONL_PREFIX")
    save = MagicMock()
    monkeypatch.setattr(scraper, "save_jsonl_to_s3", save)

    scraper.lambda_handler({}, None)

    save.assert_not_called()
    invoke.assert_called_once_with("b2s", {})
