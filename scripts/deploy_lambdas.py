"""
Compare the deployed Lambdas with this repo, and deploy them.
────────────────────────────────────────────────────────────────────────────────
Downloaded zips are only inspected in memory: never extracted or run.

Usage:
    python scripts/deploy_lambdas.py check [--profile NAME]
    python scripts/deploy_lambdas.py guard
    python scripts/deploy_lambdas.py targets --before SHA --after SHA
    python scripts/deploy_lambdas.py targets --select NAME|all
    python scripts/deploy_lambdas.py deploy NAME
    python scripts/deploy_lambdas.py deploy-image NAME IMAGE_URI

`check` is read-only. It exits 1 if a function's code really differs from the
repo, or if its zip holds files a deploy would drop. Formatting-only differences
don't count as drift.
"""

import argparse
import ast
import base64
import difflib
import hashlib
import io
import json
import re
import subprocess
import sys
import urllib.request
import zipfile
from datetime import datetime, time, timezone
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


# ── Deploy ────────────────────────────────────────────────────────────────────
def in_block_window(now: time, window: list[str]) -> bool:
    """True during the daily run, when a deploy could leave mixed versions."""
    start, end = (time.fromisoformat(value) for value in window)
    return start <= now <= end


def select_targets(manifest: dict, select: str) -> tuple[list[str], bool]:
    """Resolve a manual run's choice to (zip function names, deploy the image?)."""
    zip_names = [f["name"] for f in manifest["zip_functions"]]
    image_names = [f["name"] for f in manifest["image_functions"]]
    if select == "all":
        return zip_names, True
    if select in zip_names:
        return [select], False
    if select in image_names:
        return [], True
    raise ValueError(f"Unknown function: {select}")


def targets_from_paths(manifest: dict, paths: list[str]) -> tuple[list[str], bool]:
    """Map changed files to (zip function names, deploy the image?)."""
    names = [f["name"] for f in manifest["zip_functions"] if f["source"] in paths]
    image_changed = any(
        path.startswith(prefix)
        for path in paths
        for prefix in manifest["image_source_prefixes"]
    )
    return names, image_changed


def changed_paths(before: str, after: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--name-only", "-z", before, after],
        capture_output=True,
        check=True,
        text=True,
        encoding="utf-8",
        cwd=REPO_ROOT,
    )
    return [path for path in result.stdout.split("\0") if path]


def build_zip(source: str, handler_file: str) -> bytes:
    """Zip one file under the live handler's name, with a fixed timestamp."""
    info = zipfile.ZipInfo(handler_file, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr(info, normalize(source))
    return buffer.getvalue()


def code_sha256(zip_bytes: bytes) -> str:
    """The same encoding Lambda reports as CodeSha256."""
    return base64.b64encode(hashlib.sha256(zip_bytes).digest()).decode()


def deploy_zip_function(lambda_client, function: dict) -> None:
    name = function["name"]
    current = lambda_client.get_function(FunctionName=name)
    handler_file = current["Configuration"]["Handler"].split(".")[0] + ".py"

    with download_zip(current["Code"]["Location"]) as code_zip:
        extras = find_extras(code_zip.namelist(), handler_file)
    if extras:
        raise RuntimeError(f"{name}: a deploy would drop {extras}")

    source = (REPO_ROOT / function["source"]).read_text(encoding="utf-8")
    zip_bytes = build_zip(source, handler_file)
    lambda_client.update_function_code(FunctionName=name, ZipFile=zip_bytes)
    lambda_client.get_waiter("function_updated_v2").wait(FunctionName=name)

    deployed = lambda_client.get_function_configuration(FunctionName=name)
    if deployed["CodeSha256"] != code_sha256(zip_bytes):
        raise RuntimeError(f"{name}: deployed code doesn't match the built zip")
    print(f"✓ {name} deployed ({deployed['CodeSha256']})")


def deploy_image_function(lambda_client, name: str, image_uri: str) -> None:
    lambda_client.update_function_code(FunctionName=name, ImageUri=image_uri)
    lambda_client.get_waiter("function_updated_v2").wait(FunctionName=name)

    resolved = lambda_client.get_function(FunctionName=name)["Code"]["ResolvedImageUri"]
    if resolved.split("@")[-1] != image_uri.split("@")[-1]:
        raise RuntimeError(f"{name}: running {resolved}, expected {image_uri}")
    print(f"✓ {name} deployed ({resolved})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    commands = parser.add_subparsers(dest="command", required=True)

    check_parser = commands.add_parser("check")
    check_parser.add_argument("--profile", help="AWS CLI profile to use")

    commands.add_parser("guard")

    targets_parser = commands.add_parser("targets")
    targets_parser.add_argument("--before")
    targets_parser.add_argument("--after")
    targets_parser.add_argument("--select")

    deploy_parser = commands.add_parser("deploy")
    deploy_parser.add_argument("name")

    image_parser = commands.add_parser("deploy-image")
    image_parser.add_argument("name")
    image_parser.add_argument("image_uri")

    args = parser.parse_args()
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    if args.command == "guard":
        now = datetime.now(timezone.utc).time()
        window = manifest["deploy_block_utc"]
        if in_block_window(now, window):
            print(f"✗ Daily run window ({window[0]}-{window[1]} UTC). Re-run after it.")
            return 1
        return 0

    if args.command == "targets":
        if args.select:
            names, image = select_targets(manifest, args.select)
        else:
            paths = changed_paths(args.before, args.after)
            names, image = targets_from_paths(manifest, paths)
        print(f"functions={json.dumps(names)}")
        print(f"image={json.dumps(image)}")
        return 0

    # Imported here so the helpers above can be tested without boto3 configured
    import boto3

    profile = getattr(args, "profile", None)
    session = boto3.Session(profile_name=profile, region_name=manifest["region"])

    if args.command == "check":
        return check(session, manifest)

    lambda_client = session.client("lambda")
    if args.command == "deploy":
        function = next(f for f in manifest["zip_functions"] if f["name"] == args.name)
        deploy_zip_function(lambda_client, function)
    else:
        deploy_image_function(lambda_client, args.name, args.image_uri)
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(main())
