# =============================================
# Notebook/Script: 02_wistia_api_ingestion.py
# Purpose: Fetch Wistia API data - Shared cluster compatible (GitHub-safe)
# =============================================

import requests
import json
import time
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.functions import *

# ------------------------------------------------------
# 1) Parameters (Databricks Widgets) - no secrets in code
# ------------------------------------------------------
try:
    dbutils.widgets.text("STORAGE_ACCOUNT", "stawistiaanalytics", "Storage Account")
    dbutils.widgets.text("RAW_CONTAINER", "raw-data", "Raw Container")
    dbutils.widgets.text("PROCESSED_CONTAINER", "processed-data", "Processed Container")

    dbutils.widgets.text("SECRET_SCOPE", "kv-wistia", "Secret Scope (Storage + API)")
    dbutils.widgets.text("STORAGE_KEY_NAME", "storage-account-key", "Storage Account Key Name")
    dbutils.widgets.text("WISTIA_TOKEN_NAME", "wistia-api-token", "Wistia API Token Name")

    # Comma-separated list for convenience; override at run-time if needed
    dbutils.widgets.text("MEDIA_IDS", "gskhw4w4lm,v08dlrgr7v", "Wistia Media IDs")
    dbutils.widgets.text("LOOKBACK_DAYS", "7", "Lookback Days (default: 7)")
except NameError:
    # Running outside of Databricks; set env/args here if you wish
    pass

def _get_param(name, default):
    try:
        return dbutils.widgets.get(name).strip()
    except Exception:
        return default

storage_account      = _get_param("STORAGE_ACCOUNT", "stawistiaanalytics")
raw_container        = _get_param("RAW_CONTAINER", "raw-data")
processed_container  = _get_param("PROCESSED_CONTAINER", "processed-data")
secret_scope         = _get_param("SECRET_SCOPE", "kv-wistia")
storage_key_name     = _get_param("STORAGE_KEY_NAME", "storage-account-key")
wistia_token_name    = _get_param("WISTIA_TOKEN_NAME", "wistia-api-token")
media_ids_str        = _get_param("MEDIA_IDS", "gskhw4w4lm,v08dlrgr7v")
lookback_days        = int(_get_param("LOOKBACK_DAYS", "7"))

MEDIA_IDS = [m.strip() for m in media_ids_str.split(",") if m.strip()]
BASE_URL  = "https://api.wistia.com/v1/stats/medias"

# ------------------------------------------------------
# 2) Secrets and Spark configuration
# ------------------------------------------------------
# Retrieve storage key (preferred: Databricks Secrets; fallback: Spark conf)
storage_key = None
secret_source = None
try:
    storage_key = dbutils.secrets.get(scope=secret_scope, key=storage_key_name)
    secret_source = f"databricks-secrets:{secret_scope}/{storage_key_name}"
except Exception:
    candidate = spark.conf.get(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", None)
    if candidate:
        storage_key = candidate
        secret_source = "spark-conf"
    else:
        raise RuntimeError(
            "No storage key found. Add it to Databricks Secrets or set Spark conf "
            f"'fs.azure.account.key.{storage_account}.dfs.core.windows.net'."
        )

# Configure ADLS Gen2 access
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# Retrieve Wistia API token (must be stored as secret)
try:
    API_TOKEN = dbutils.secrets.get(scope=secret_scope, key=wistia_token_name)
except Exception:
    raise RuntimeError(
        f"Wistia API token not available. Please add secret '{wistia_token_name}' in scope '{secret_scope}'."
    )

# ------------------------------------------------------
# 3) Build ABFSS paths (Gen2 best practice)
# ------------------------------------------------------
RAW_PATH        = f"abfss://{raw_container}@{storage_account}.dfs.core.windows.net"
PROCESSED_PATH  = f"abfss://{processed_container}@{storage_account}.dfs.core.windows.net"

print("=" * 78)
print("🚀 WISTIA API DATA INGESTION (Shared Cluster) — GitHub-safe")
print("=" * 78)
print(f"✓ Storage account  : {storage_account}")
print(f"✓ Raw container    : {raw_container}")
print(f"✓ Processed cont.  : {processed_container}")
print(f"✓ Secret source    : {secret_source}")
print(f"✓ RAW_PATH         : {RAW_PATH}")
print(f"✓ PROCESSED_PATH   : {PROCESSED_PATH}")
print(f"✓ MEDIA_IDS        : {MEDIA_IDS}")
print(f"✓ LOOKBACK_DAYS    : {lookback_days}")

# ------------------------------------------------------
# 4) Helpers
# ------------------------------------------------------
def fetch_with_retry(url, headers, params=None, max_retries=3):
    """HTTP GET with simple retry/backoff for 429 and transient errors."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 429:
                wait_s = (2 ** attempt) * 5
                print(f"  ⚠️  Rate limited (429). Waiting {wait_s}s...")
                time.sleep(wait_s)
                continue
            if resp.status_code == 404:
                print("  ⚠️  404 Not Found")
                return None
            print(f"  ✗ HTTP {resp.status_code}: {resp.text[:200]}")
            return None
        except Exception as e:
            print(f"  ✗ Attempt {attempt+1} error: {str(e)[:160]}")
            if attempt < max_retries - 1:
                time.sleep(5)
    return None

def fetch_media_stats(media_id):
    """Fetch media statistics from Wistia API."""
    url = f"{BASE_URL}/{media_id}.json"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    print("  → Fetching media stats...")
    response = fetch_with_retry(url, headers)
    if response and response.status_code == 200:
        data = response.json()
        print(f"  ✓ Retrieved: {data.get('name', 'Unknown')}")
        return data
    print("  ✗ Failed to fetch media stats")
    return None

def fetch_visitor_data(media_id, since_date=None):
    """Fetch visitor data with pagination."""
    all_visitors, page = [], 1
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    print("  → Fetching visitor data...")
    while True:
        url = f"{BASE_URL}/{media_id}/visitors.json"
        params = {"page": page, "per_page": 100}
        if since_date:
            params["since"] = since_date.strftime("%Y-%m-%d")
        resp = fetch_with_retry(url, headers, params)
        if not resp or resp.status_code != 200:
            break
        visitors = resp.json()
        if not visitors:
            if page == 1:
                print("  ⚠️  No visitors found")
            else:
                print(f"  ✓ Total visitors: {len(all_visitors)}")
            break
        all_visitors.extend(visitors)
        print(f"  ✓ Page {page}: +{len(visitors)} (total: {len(all_visitors)})")
        page += 1
        time.sleep(1)
        if page > 100:
            print("  ⚠️  Pagination limit reached")
            break
    return all_visitors

def save_json_as_dataframe(data, path):
    """Convert JSON (dict/list) to DataFrame and write as JSON (overwrite)."""
    if not data:
        print("  ⚠️  No data to save")
        return False
    try:
        if isinstance(data, list):
            # Ensure we handle possibly sparse dicts
            rows = [Row(**{k: v for k, v in item.items()}) for item in data]
            df = spark.createDataFrame(rows) if rows else spark.createDataFrame([], schema=None)
        else:
            df = spark.createDataFrame([Row(**data)])
        df.write.mode("overwrite").json(path)
        cnt = df.count()
        print(f"  ✓ Saved: {cnt} record(s) to {path}")
        return True
    except Exception as e:
        print(f"  ✗ Save failed: {str(e)[:200]}")
        return False

def get_last_run_date():
    """Read last successful run from RAW metadata; default to lookback window."""
    try:
        meta_path = f"{RAW_PATH}/metadata/last_run.json"
        df = spark.read.json(meta_path)
        if df.count() > 0:
            last_date_str = df.first().timestamp
            last_date = datetime.fromisoformat(last_date_str)
            print(f"ℹ️  Last run: {last_date}")
            return last_date
    except Exception:
        print(f"ℹ️  No previous run found (using {lookback_days}-day lookback)")
    return datetime.utcnow() - timedelta(days=lookback_days)

def update_last_run_date():
    """Write current UTC timestamp to RAW metadata."""
    try:
        now_iso = datetime.utcnow().isoformat()
        df = spark.createDataFrame([Row(timestamp=now_iso, status="success")])
        meta_path = f"{RAW_PATH}/metadata/last_run.json"
        df.write.mode("overwrite").json(meta_path)
        print("✓ Updated last run timestamp")
    except Exception as e:
        print(f"⚠️  Could not update timestamp: {str(e)[:160]}")

# ------------------------------------------------------
# 5) Main
# ------------------------------------------------------
def main():
    # Verify storage access quickly
    try:
        dbutils.fs.ls(RAW_PATH + "/")
        print("✓ Storage access verified")
    except Exception as e:
        print(f"❌ Storage access failed: {str(e)[:200]}")
        print("   Please run 01_setup_storage_access first.")
        return

    last_run_date = get_last_run_date()
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    total_media_success = 0
    total_visitors_fetched = 0

    for idx, media_id in enumerate(MEDIA_IDS, 1):
        print("\n" + "=" * 78)
        print(f"🎥 MEDIA {idx}/{len(MEDIA_IDS)}: {media_id}")
        print("=" * 78)

        # Media stats
        media = fetch_media_stats(media_id)
        if media:
            media_path = f"{RAW_PATH}/media/{media_id}_{run_ts}"
            if save_json_as_dataframe(media, media_path):
                total_media_success += 1

        # Visitors (incremental by last_run_date)
        visitors = fetch_visitor_data(media_id, last_run_date)
        if visitors:
            visitor_path = f"{RAW_PATH}/visitors/{media_id}_{run_ts}"
            if save_json_as_dataframe(visitors, visitor_path):
                total_visitors_fetched += len(visitors)

        if idx < len(MEDIA_IDS):
            print("  ⏸️  Pausing 3 seconds...")
            time.sleep(3)

    update_last_run_date()

    print("\n" + "=" * 78)
    print("✅ INGESTION COMPLETE")
    print("=" * 78)
    print(f"  Media processed: {total_media_success}/{len(MEDIA_IDS)}")
    print(f"  Total visitors : {total_visitors_fetched}")
    print(f"  Timestamp      : {run_ts}")
    print("=" * 78 + "\n")

# Execute
main()
