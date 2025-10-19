# =============================================
# Notebook/Script: 01_setup_storage_access.py
# Purpose: Configure secure storage access for shared clusters (GitHub-safe)
# Notes:
#   - No secrets are hard-coded.
#   - Uses Databricks Secrets (scope/key) or Spark conf fallback.
#   - Uses ADLS Gen2 (abfss://) paths.
#   - Safe to commit to GitHub.
# =============================================

from datetime import datetime
from pyspark.sql import Row

# ----------------------------
# 1) Widgets / Parameters
# ----------------------------
# These let you pass values at run-time without hard-coding them into source control.
try:
    dbutils.widgets.text("STORAGE_ACCOUNT", "stawistiaanalytics", "Storage Account")
    dbutils.widgets.text("RAW_CONTAINER", "raw-data", "Raw Container")
    dbutils.widgets.text("PROCESSED_CONTAINER", "processed-data", "Processed Container")
    dbutils.widgets.text("SECRET_SCOPE", "kv-wistia", "Databricks Secret Scope")
    dbutils.widgets.text("SECRET_KEY", "storage-account-key", "Databricks Secret Key")
except NameError:
    # If running as a pure .py (outside Databricks), you can set environment variables or Spark conf.
    pass

# Resolve params
def _get_widget_or_default(name, default):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default

storage_account       = _get_widget_or_default("STORAGE_ACCOUNT", "stawistiaanalytics").strip()
raw_container         = _get_widget_or_default("RAW_CONTAINER", "raw-data").strip()
processed_container   = _get_widget_or_default("PROCESSED_CONTAINER", "processed-data").strip()
secret_scope          = _get_widget_or_default("SECRET_SCOPE", "kv-wistia").strip()
secret_key_name       = _get_widget_or_default("SECRET_KEY", "storage-account-key").strip()

# ----------------------------
# 2) Retrieve secret or fallback
# ----------------------------
account_key = None
secret_source = None

# Prefer Databricks Secrets
try:
    account_key = dbutils.secrets.get(scope=secret_scope, key=secret_key_name)
    secret_source = f"databricks-secrets:{secret_scope}/{secret_key_name}"
except Exception:
    # Optional fallback: Spark conf (set via cluster / job config). Useful in dev.
    # DO NOT hard-code in code; set with:
    # spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", "<key>")
    candidate = spark.conf.get(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        None
    )
    if candidate:
        account_key = candidate
        secret_source = "spark-conf"
    else:
        raise RuntimeError(
            "No storage key available. Configure a secret in Databricks (preferred) "
            f"or set spark conf 'fs.azure.account.key.{storage_account}.dfs.core.windows.net'."
        )

# ----------------------------
# 3) Spark config for ADLS Gen2
# ----------------------------
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    account_key
)

# Build ABFSS paths (recommended for ADLS Gen2)
RAW_PATH       = f"abfss://{raw_container}@{storage_account}.dfs.core.windows.net"
PROCESSED_PATH = f"abfss://{processed_container}@{storage_account}.dfs.core.windows.net"

# ----------------------------
# 4) Sanity banner (no secrets printed)
# ----------------------------
print("=" * 78)
print("🔧 STORAGE ACCESS CONFIGURATION")
print("=" * 78)
print(f"✓ Storage account: {storage_account}")
print(f"✓ Raw container: {raw_container}")
print(f"✓ Processed container: {processed_container}")
print(f"✓ Secret source: {secret_source}")
print(f"✓ RAW_PATH: {RAW_PATH}")
print(f"✓ PROCESSED_PATH: {PROCESSED_PATH}")

# ----------------------------
# 5) Health check: write & read a tiny JSON to RAW
# ----------------------------
try:
    test_df = spark.createDataFrame([
        Row(message=f"Health check OK @ {datetime.now().isoformat(timespec='seconds')}", status="OK")
    ])
    health_dir  = f"{RAW_PATH}/_healthcheck"
    test_path   = f"{health_dir}/test_access.json"

    # Write
    test_df.write.mode("overwrite").json(test_path)
    print("✓ Write test successful")

    # Read
    read_df = spark.read.json(test_path)
    first = read_df.first()
    print(f"✓ Read test successful: {first.message if first else 'No rows read'}")

    # Optional: quick list of known containers (we can only list when we know the container name)
    try:
        _ = dbutils.fs.ls(RAW_PATH + "/")
        print(f"✓ Listing RAW container succeeded: {RAW_PATH}/")
    except Exception as e:
        print(f"• Could not list RAW container ({RAW_PATH}/): {e}")

    try:
        _ = dbutils.fs.ls(PROCESSED_PATH + "/")
        print(f"✓ Listing PROCESSED container succeeded: {PROCESSED_PATH}/")
    except Exception as e:
        print(f"• Could not list PROCESSED container ({PROCESSED_PATH}/): {e}")

    print("\n" + "=" * 78)
    print("✅ STORAGE ACCESS CONFIGURED SUCCESSFULLY!")
    print("=" * 78)

except Exception as e:
    print(f"✗ Error: {str(e)}")
    print("\nPlease verify:")
    print("  1) Storage account name is correct")
    print("  2) Storage key is valid (via Databricks Secret or Spark conf)")
    print("  3) Containers exist (raw-data, processed-data)")
    print("  4) Your cluster has permission to access the secret scope (if using Key Vault-backed)")
    raise
