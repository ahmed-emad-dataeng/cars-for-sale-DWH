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
