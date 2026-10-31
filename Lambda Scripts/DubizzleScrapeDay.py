import json
import gzip
import boto3
import requests
from datetime import datetime, timezone
import os

# ── Config ────────────────────────────────────────────────────────────────────
s3 = boto3.client("s3")


def load_config() -> dict:
    bucket = os.environ["BUCKET_NAME"]
    key = f"config.json"
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


CONFIG = load_config()

S3_BUCKET = CONFIG.get("s3").get("bucket")
S3_PREFIX = CONFIG.get("s3").get("prefix")
URL = CONFIG.get("request").get("url")

HEADERS = CONFIG.get("request").get("headers")
# ── Payload builder ───────────────────────────────────────────────────────────


def build_payload(order: str) -> str:
    if order == "desc":
        payload = CONFIG.get("payloads").get("desc")
    elif order == "asc":
        payload = CONFIG.get("payloads").get("asc")
    else:
        raise ValueError(f"Invalid order: {order}")
    return payload


# ── Core helpers ──────────────────────────────────────────────────────────────
lambda_client = boto3.client("lambda")


def invoke_next(function_name: str, payload: dict):
    """Fire-and-forget async invoke of the next Lambda in the chain."""
    lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="Event",  # async — ScrapeDay doesn't wait
        Payload=json.dumps(payload).encode(),
    )
    print(f"  → Invoked {function_name} asynchronously")


def fetch_ads(order: str) -> list[dict]:
    """Call the API and return the list of ad _source dicts."""
    payload = build_payload(order)
    resp = requests.post(URL, headers=HEADERS, data=payload, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    responses = data.get("responses", [])

    # get the ads from the right response based on the order
    ads_response = responses[-1]
    hits = ads_response.get("hits", {}).get("hits", [])
    hits = [hit["_source"] for hit in hits]
    elite_hits = responses[1].get("hits", {}).get("hits", [])
    elite_hits = [hit["_source"] for hit in elite_hits]
    featured_hits = responses[2].get("hits", {}).get("hits", [])
    featured_hits = [hit["_source"] for hit in featured_hits]
    hits.extend(elite_hits)
    hits.extend(featured_hits)
    return hits


def save_to_s3(cars: list[dict], label: str) -> str:
    """Save deduplicated cars to S3 and return the S3 key."""
    s3 = boto3.client("s3")
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"{S3_PREFIX}/{date_str}/{label}.json"

    body = json.dumps({"count": len(cars), "cars": cars}, ensure_ascii=False)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    return key


def save_jsonl_to_s3(
    cars: list[dict], pass_label: str, scraped_at: datetime, prefix: str
) -> str:
    """
    Save one record per line (gzipped JSON Lines) for Athena/dbt to read.
    Each record is stamped with the pass it came from and the scrape time.
    Hive-style folder (ingestion_date=...) so Athena can partition on it.
    """
    date_str = scraped_at.strftime("%Y-%m-%d")
    key = f"{prefix}/ingestion_date={date_str}/{pass_label}.jsonl.gz"

    lines = (
        json.dumps(
            {**car, "_pass": pass_label, "_scraped_at": scraped_at.isoformat()},
            ensure_ascii=False,
        )
        for car in cars
    )
    body = gzip.compress(("\n".join(lines) + "\n").encode("utf-8"))
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/gzip",
    )
    return key


# ── Lambda handler ────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    print("Fetching newest cars …")
    newest = fetch_ads("desc")
    print(f"  → {len(newest)} ads from newest call")

    print("Fetching oldest cars …")
    oldest = fetch_ads("asc")
    print(f"  → {len(oldest)} ads from oldest call")

    key = save_to_s3(oldest, "oldest_cars")
    print(f"Saved to s3://{S3_BUCKET}/{key}")
    key = save_to_s3(newest, "newest_cars")
    print(f"Saved to s3://{S3_BUCKET}/{key}")

    # Dual-write JSON Lines for the dbt pipeline. Skipped if JSONL_PREFIX is
    # unset, and a failure here never blocks the legacy chain below.
    jsonl_prefix = os.environ.get("JSONL_PREFIX")
    if jsonl_prefix:
        scraped_at = datetime.now(timezone.utc)
        try:
            for pass_label, cars in (("newest", newest), ("oldest", oldest)):
                jsonl_key = save_jsonl_to_s3(cars, pass_label, scraped_at, jsonl_prefix)
                print(f"  JSONL written → s3://{S3_BUCKET}/{jsonl_key}")
        except Exception as e:
            print(f" ⚠ JSONL write failed — {e}")
    else:
        print(" ⚠ JSONL_PREFIX not set — skipping JSONL write")

    # Chain to next Lambda (Bronze to Silver) if configured
    b2s_function = os.environ.get("BRONZE_TO_SILVER_FUNCTION_NAME")
    if b2s_function:
        invoke_next(b2s_function, {})  # {} = process today by default
    else:
        print(" ⚠ BRONZE_TO_SILVER_FUNCTION_NAME not set — skipping chain")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "newest": len(newest),
                "oldest": len(oldest),
                "s3_key": key,
            }
        ),
    }
