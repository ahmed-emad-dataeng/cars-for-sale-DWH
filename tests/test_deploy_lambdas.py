import io
import json
import zipfile
from datetime import time
from unittest.mock import MagicMock

import pytest


def test_identical_code(deploy_tool):
    assert deploy_tool.classify_drift("x = 1\n", "x = 1\n") == deploy_tool.IDENTICAL


def test_line_endings_are_not_drift(deploy_tool):
    assert deploy_tool.classify_drift("x = 1\r\n", "x = 1\n") == deploy_tool.IDENTICAL


def test_black_reformat_is_not_drift(deploy_tool):
    deployed = "foo = {'a':1}\n"
    repo = 'foo = {"a": 1}\n'
    assert deploy_tool.classify_drift(deployed, repo) == deploy_tool.FORMATTING_ONLY


def test_changed_logic_is_drift(deploy_tool):
    assert deploy_tool.classify_drift("x = 1\n", "x = 2\n") == deploy_tool.CODE_DRIFT


def test_unparseable_deployed_code_is_drift(deploy_tool):
    assert deploy_tool.classify_drift("def (:\n", "x = 1\n") == deploy_tool.CODE_DRIFT


def test_extras_ignore_the_handler_folders_and_bytecode(deploy_tool):
    names = [
        "lambda_function.py",
        "__pycache__/lambda_function.cpython-314.pyc",
        "requests/",
        "requests/api.py",
    ]
    assert deploy_tool.find_extras(names, "lambda_function.py") == ["requests/api.py"]


def test_layer_packages_reads_dist_info(deploy_tool):
    names = [
        "python/pandas/__init__.py",
        "python/pandas-2.3.3.dist-info/METADATA",
        "python/pandas-2.3.3.dist-info/RECORD",
        "python/pyarrow-22.0.0.dist-info/METADATA",
    ]
    assert deploy_tool.layer_packages(names) == ["pandas==2.3.3", "pyarrow==22.0.0"]


# ── Deploy ────────────────────────────────────────────────────────────────────
MANIFEST = {
    "zip_functions": [
        {"name": "scrape", "source": "Lambda Scripts/DubizzleScrapeDay.py"},
        {"name": "gold", "source": "Lambda Scripts/Silver-to-Gold.py"},
    ],
    "image_functions": [{"name": "DbtRunner"}],
    "image_source_prefixes": ["dubizzle_dbt/", "Lambda Scripts/dbt-runner/"],
}


@pytest.mark.parametrize(
    "now, blocked",
    [
        (time(3, 24), False),
        (time(3, 25), True),
        (time(4, 0), True),
        (time(4, 20), True),
        (time(4, 21), False),
    ],
)
def test_block_window_edges(deploy_tool, now, blocked):
    assert deploy_tool.in_block_window(now, ["03:25", "04:20"]) is blocked


def test_only_a_changed_lambda_is_targeted(deploy_tool):
    paths = ["Lambda Scripts/Silver-to-Gold.py", "README.md"]
    assert deploy_tool.targets_from_paths(MANIFEST, paths) == (["gold"], False)


def test_dbt_changes_target_the_image(deploy_tool):
    paths = ["dubizzle_dbt/tests/assert_one_active_row_per_id.sql"]
    assert deploy_tool.targets_from_paths(MANIFEST, paths) == ([], True)


def test_undeployed_files_target_nothing(deploy_tool):
    paths = ["Lambda Scripts/Test_gold_layer.py", "Raw Data/README.md", "tests/x.py"]
    assert deploy_tool.targets_from_paths(MANIFEST, paths) == ([], False)


def test_select_all_and_single_functions(deploy_tool):
    assert deploy_tool.select_targets(MANIFEST, "all") == (["scrape", "gold"], True)
    assert deploy_tool.select_targets(MANIFEST, "gold") == (["gold"], False)
    assert deploy_tool.select_targets(MANIFEST, "DbtRunner") == ([], True)


def test_select_rejects_an_unknown_function(deploy_tool):
    with pytest.raises(ValueError):
        deploy_tool.select_targets(MANIFEST, "nope")


def test_real_manifest_points_at_real_files(deploy_tool):
    manifest = json.loads(deploy_tool.MANIFEST.read_text(encoding="utf-8"))
    for function in manifest["zip_functions"]:
        assert (deploy_tool.REPO_ROOT / function["source"]).is_file()


def test_workflow_choices_match_the_manifest(deploy_tool):
    manifest = json.loads(deploy_tool.MANIFEST.read_text(encoding="utf-8"))
    workflow = (deploy_tool.REPO_ROOT / ".github/workflows/deploy.yml").read_text(
        encoding="utf-8"
    )
    for function in manifest["zip_functions"] + manifest["image_functions"]:
        assert f"- {function['name']}" in workflow


def test_zip_uses_the_handler_name_and_is_reproducible(deploy_tool):
    first = deploy_tool.build_zip("x = 1\r\n", "lambda_function.py")
    second = deploy_tool.build_zip("x = 1\n", "lambda_function.py")

    assert first == second
    with zipfile.ZipFile(io.BytesIO(first)) as zf:
        assert zf.namelist() == ["lambda_function.py"]
        assert zf.read("lambda_function.py") == b"x = 1\n"


@pytest.fixture
def lambda_client(deploy_tool, monkeypatch):
    def fake_download(url):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            for name in url.split(","):
                zf.writestr(name, "x")
        return zipfile.ZipFile(io.BytesIO(buffer.getvalue()))

    monkeypatch.setattr(deploy_tool, "download_zip", fake_download)
    client = MagicMock()
    client.get_function.return_value = {
        "Configuration": {"Handler": "lambda_function.lambda_handler"},
        "Code": {"Location": "lambda_function.py"},
    }
    return client


FUNCTION = {"name": "gold", "source": "Lambda Scripts/Silver-to-Gold.py"}


def test_deploy_updates_waits_and_verifies(deploy_tool, lambda_client):
    def report_uploaded_hash(**kwargs):
        sha = deploy_tool.code_sha256(lambda_client.uploaded)
        return {"CodeSha256": sha}

    lambda_client.update_function_code.side_effect = lambda **kwargs: setattr(
        lambda_client, "uploaded", kwargs["ZipFile"]
    )
    lambda_client.get_function_configuration.side_effect = report_uploaded_hash

    deploy_tool.deploy_zip_function(lambda_client, FUNCTION)

    with zipfile.ZipFile(io.BytesIO(lambda_client.uploaded)) as zf:
        assert zf.namelist() == ["lambda_function.py"]
        assert b"def apply_scd" in zf.read("lambda_function.py")
    lambda_client.get_waiter.assert_called_once_with("function_updated_v2")


def test_deploy_fails_when_the_live_hash_differs(deploy_tool, lambda_client):
    lambda_client.get_function_configuration.return_value = {"CodeSha256": "other"}

    with pytest.raises(RuntimeError, match="doesn't match"):
        deploy_tool.deploy_zip_function(lambda_client, FUNCTION)


def test_deploy_refuses_to_drop_files_from_the_live_zip(deploy_tool, lambda_client):
    lambda_client.get_function.return_value["Code"][
        "Location"
    ] = "lambda_function.py,helper.py"

    with pytest.raises(RuntimeError, match="helper.py"):
        deploy_tool.deploy_zip_function(lambda_client, FUNCTION)

    lambda_client.update_function_code.assert_not_called()


def test_image_deploy_verifies_the_digest(deploy_tool):
    client = MagicMock()
    client.get_function.return_value = {"Code": {"ResolvedImageUri": "repo@sha256:old"}}

    with pytest.raises(RuntimeError, match="expected"):
        deploy_tool.deploy_image_function(client, "DbtRunner", "repo@sha256:new")
