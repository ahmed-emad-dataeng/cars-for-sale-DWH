"""
Compare what's deployed on AWS with what's in this repo.
────────────────────────────────────────────────────────────────────────────────
Read-only. Downloaded zips are only inspected in memory: never extracted or run.

Usage:
    python scripts/deploy_lambdas.py check [--profile NAME]

Exits 1 if a function's code really differs from the repo, or if its zip holds
files a deploy would drop. Formatting-only differences don't count as drift.
"""

import argparse
import ast
import difflib
import io
import json
import re
import sys
import urllib.request
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST = REPO_ROOT / "deploy" / "lambdas.json"

IDENTICAL = "identical"
FORMATTING_ONLY = "formatting-only"
CODE_DRIFT = "CODE DRIFT"


def normalize(source: str) -> str:
    return source.replace("\r\n", "\n")


def classify_drift(deployed: str, repo: str) -> str:
    """Compare by text first, then by AST, so a black reformat isn't drift."""
    if normalize(deployed) == normalize(repo):
        return IDENTICAL
    try:
        if ast.dump(ast.parse(deployed)) == ast.dump(ast.parse(repo)):
            return FORMATTING_ONLY
    except SyntaxError:
        pass
    return CODE_DRIFT


def find_extras(names: list[str], handler_file: str) -> list[str]:
    """Files in the deployed zip other than the handler — a deploy would drop them."""
    return [
        name
        for name in names
        if name != handler_file and not name.endswith("/") and "__pycache__" not in name
    ]


def layer_packages(names: list[str]) -> list[str]:
    """Read `package==version` from the *.dist-info folders in a layer zip."""
    packages = set()
    for name in names:
        match = re.search(r"([^/]+)-([^-/]+)\.dist-info/", name)
        if match:
            packages.add(f"{match.group(1)}=={match.group(2)}")
    return sorted(packages)


def download_zip(url: str) -> zipfile.ZipFile:
    with urllib.request.urlopen(url) as resp:
        return zipfile.ZipFile(io.BytesIO(resp.read()))


def check_zip_function(lambda_client, function: dict) -> bool:
    """Print one function's report. Returns True if it's safe to deploy over."""
    name = function["name"]
    resp = lambda_client.get_function(FunctionName=name)
    config = resp["Configuration"]
    handler_file = config["Handler"].split(".")[0] + ".py"

    print(f"\n── {name} ──")
    print(f"  Runtime      : {config.get('Runtime')}")
    print(f"  Architecture : {config.get('Architectures')}")
    print(f"  Handler      : {config['Handler']}")
    print(f"  Memory/Time  : {config.get('MemorySize')} MB / {config.get('Timeout')} s")

    for layer in config.get("Layers", []):
        arn = layer["Arn"]
        print(f"  Layer        : {arn.split(':layer:')[-1]}")
        content = lambda_client.get_layer_version_by_arn(Arn=arn)["Content"]
        with download_zip(content["Location"]) as layer_zip:
            for package in layer_packages(layer_zip.namelist()):
                print(f"    {package}")

    with download_zip(resp["Code"]["Location"]) as code_zip:
        names = code_zip.namelist()
        extras = find_extras(names, handler_file)
        if handler_file not in names:
            print(f"  ✗ {handler_file} not found in the zip")
            return False
        deployed = code_zip.read(handler_file).decode("utf-8")

    repo = (REPO_ROOT / function["source"]).read_text(encoding="utf-8")
    drift = classify_drift(deployed, repo)
    print(f"  Code vs repo : {drift}")
    if drift == CODE_DRIFT:
        diff = difflib.unified_diff(
            normalize(deployed).splitlines(),
            normalize(repo).splitlines(),
            fromfile=f"deployed/{handler_file}",
            tofile=function["source"],
            lineterm="",
        )
        for line in diff:
            print(f"    {line}")

    if extras:
        print(f"  ✗ {len(extras)} other file(s) in the zip, a deploy would drop them:")
        for extra in extras[:20]:
            print(f"    {extra}")

    return drift != CODE_DRIFT and not extras


def check(session, manifest: dict) -> int:
    lambda_client = session.client("lambda")
    ok = True

    for function in manifest["zip_functions"]:
        try:
            ok = check_zip_function(lambda_client, function) and ok
        except lambda_client.exceptions.ResourceNotFoundException:
            print(f"\n── {function['name']} ──\n  ✗ no such function")
            ok = False

    for function in manifest["image_functions"]:
        print(f"\n── {function['name']} (image) ──")
        try:
            config = lambda_client.get_function(FunctionName=function["name"])
        except lambda_client.exceptions.ResourceNotFoundException:
            print("  ✗ no such function")
            ok = False
            continue
        print(f"  Architecture : {config['Configuration'].get('Architectures')}")
        print(f"  Image        : {config['Code'].get('ImageUri')}")
        print("  Compare /var/task in this image with the repo by hand")

    print("\n── EventBridge rules ──")
    for rule in session.client("events").list_rules()["Rules"]:
        schedule = rule.get("ScheduleExpression", "-")
        print(f"  {rule['Name']}: {schedule} ({rule.get('State')})")

    bucket = manifest["gold_bucket"]
    versioning = session.client("s3").get_bucket_versioning(Bucket=bucket)
    print(f"\n── {bucket} versioning: {versioning.get('Status', 'Disabled')} ──")

    print("\nResult:", "safe to deploy" if ok else "NOT safe to deploy, see ✗ above")
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("command", choices=["check"])
    parser.add_argument("--profile", help="AWS CLI profile to use")
    args = parser.parse_args()

    # Imported here so the helpers above can be tested without boto3 configured
    import boto3

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    session = boto3.Session(profile_name=args.profile, region_name=manifest["region"])
    return check(session, manifest)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(main())
