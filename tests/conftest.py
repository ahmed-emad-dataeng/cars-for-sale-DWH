import importlib.util
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
LAMBDA_DIR = REPO_ROOT / "Lambda Scripts"

# The Lambdas create boto3 clients at import. These never reach AWS.
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-north-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")


def load_module(path: Path, name: str):
    """Load by path: the hyphenated Lambda filenames can't be imported normally."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def silver_to_gold():
    return load_module(LAMBDA_DIR / "Silver-to-Gold.py", "silver_to_gold")


@pytest.fixture(scope="session")
def bronze_to_silver():
    return load_module(LAMBDA_DIR / "Bronze-to-Silver.py", "bronze_to_silver")


@pytest.fixture(scope="session")
def email_notify():
    return load_module(LAMBDA_DIR / "EmailNotify.py", "email_notify")


@pytest.fixture(scope="session")
def scraper():
    """DubizzleScrapeDay reads config.json from S3 at import, so fake that read."""
    config = {
        "s3": {"bucket": "test-bucket", "prefix": "bronze"},
        "request": {"url": "https://example.test/search", "headers": {}},
        "payloads": {"desc": "desc-payload", "asc": "asc-payload"},
    }
    fake_s3 = MagicMock()
    fake_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps(config).encode())
    }
    os.environ.setdefault("BUCKET_NAME", "test-bucket")
    with patch("boto3.client", return_value=fake_s3):
        return load_module(LAMBDA_DIR / "DubizzleScrapeDay.py", "scraper")


@pytest.fixture(scope="session")
def deploy_tool():
    return load_module(REPO_ROOT / "scripts" / "deploy_lambdas.py", "deploy_lambdas")
