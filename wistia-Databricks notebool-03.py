# =============================================
# Notebook/Script: 03_wistia_data_processing.py
# Purpose: Transform raw data - Shared cluster & UC-safe (GitHub-friendly)
# =============================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# ------------------------------------------------------
# 1) Parameters (no secrets in code)
# ------------------------------------------------------
try:
    dbutils.widgets.text("STORAGE_ACCOUNT", "stawistiaanalytics", "Storage Account")
    dbutils.widgets.text("RAW_CONTAINER", "raw-data", "Raw Container")
    dbutils.widgets.text("PROCESSED_CONTAINER", "processed-data", "Processed Container")
    dbutils.widgets.text("SECRET_SCOPE", "kv-wistia", "Secret Scope")
    dbutils.widgets.text("STORAGE_KEY_NAME", "storage-account-key", "Storage Key Name")
except NameError:
    pass

def _p(name, default):
    try:
        return dbutils.widgets.get(name).strip()
    except Exception:
        return default

storage_account     = _p("STORAGE_ACCOUNT", "stawistiaanalytics")
raw_container       = _p("RAW_CONTAINER", "raw-data")
processed_container = _p("PROCESSED_CONTAINER", "processed-data")
secret_scope        = _p("SECRET_SCOPE", "kv-wistia")
storage_key_name    = _p("STORAGE_KEY_NAME", "storage-account-key")

# ------------------------------------------------------
# 2) Configure ADLS Gen2 access (Databricks Secrets → Spark conf)
# ------------------------------------------------------
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
            "No storage key available. Store it in Databricks Secrets or set "
            f"spark conf 'fs.azure.account.key.{storage_account}.dfs.core.windows.net'."
        )

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# ------------------------------------------------------
# 3) Paths (Gen2 best practice)
# ------------------------------------------------------
RAW_PATH       = f"abfss://{raw_container}@{storage_account}.dfs.core.windows.net"
PROCESSED_PATH = f"abfss://{processed_container}@{storage_account}.dfs.core.windows.net"

print("=" * 70)
print("🔄 WISTIA DATA PROCESSING (Shared Cluster, Unity Catalog-safe)")
print("=" * 70)
print(f"✓ Storage account : {storage_account}")
print(f"✓ RAW_PATH        : {RAW_PATH}")
print(f"✓ PROCESSED_PATH  : {PROCESSED_PATH}")
print(f"✓ Secret source   : {secret_source}")

# =============================================
# READ RAW DATA
# =============================================

print("\n📂 Reading raw data...")

try:
    # Best-effort listing
    try:
        media_folders   = dbutils.fs.ls(f"{RAW_PATH}/media/")
        visitor_folders = dbutils.fs.ls(f"{RAW_PATH}/visitors/")
        print(f"  ✓ Found {len(media_folders)} media folder(s)")
        print(f"  ✓ Found {len(visitor_folders)} visitor folder(s)")
    except Exception:
        print("  ⚠️  Could not list folders (they may not exist yet)")

    # Read media data
    media_df = (
        spark.read
        .option("multiline", "true")
        .json(f"{RAW_PATH}/media/*/*.json")
    )
    media_count = media_df.count()
    print(f"  ✓ Loaded {media_count} media record(s)")

    # Read visitor data
    visitor_df = (
        spark.read
        .option("multiline", "true")
        .json(f"{RAW_PATH}/visitors/*/*.json")
    )
    visitor_count = visitor_df.count()
    print(f"  ✓ Loaded {visitor_count} visitor record(s)")

except Exception as e:
    print(f"  ❌ Error reading data: {str(e)[:300]}")
    print("\n  Troubleshooting:")
    print("  1. Run 02_Wistia_API_Ingestion or 00_Generate_Dummy_Data first")
    print("  2. Verify data exists in storage account")
    raise

print("\n📊 Sample Media Data:")
if media_count > 0:
    display(media_df.limit(3))
else:
    print("  ⚠️  No media data found")

print("\n📊 Sample Visitor Data:")
if visitor_count > 0:
    display(visitor_df.limit(3))
else:
    print("  ⚠️  No visitor data found")

# =============================================
# TRANSFORM: dim_media
# =============================================

print("\n🔧 Processing dim_media...")

if media_count > 0:
    dim_media = media_df.select(
        F.col("hashed_id").alias("media_id"),
        F.coalesce(F.col("name"), F.lit("Unknown")).alias("title"),
        F.concat(F.lit("https://wistia.com/series/health/videos/"), F.col("hashed_id")).alias("url"),
        F.when(F.lower(F.coalesce(F.col("name"), F.lit(""))).contains("facebook"), "Facebook")
         .when(F.lower(F.coalesce(F.col("name"), F.lit(""))).contains("youtube"), "YouTube")
         .when(F.lower(F.coalesce(F.col("name"), F.lit(""))).contains("instagram"), "Instagram")
         .otherwise("Wistia").alias("channel"),
        F.when(F.col("created").cast("bigint").isNotNull(),
               F.from_unixtime(F.col("created")).cast("timestamp"))
         .otherwise(F.current_timestamp()).alias("created_at"),
        F.current_timestamp().alias("processed_at")
    ).distinct()

    # Ensure non-null PK
    print("  🔒 Ensuring media_id integrity...")
    dim_media = dim_media.withColumn(
        "media_id",
        F.when(F.col("media_id").isNull() | (F.col("media_id") == ""),
               F.concat(F.lit("media_"), F.monotonically_increasing_id()))
        .otherwise(F.col("media_id"))
    )

    dim_media_count = dim_media.count()
    print(f"  ✓ Processed {dim_media_count} media record(s)")
else:
    print("  ⚠️  Skipping - no media data")
    dim_media = None
    dim_media_count = 0

# =============================================
# TRANSFORM: dim_visitor
# =============================================

print("\n🔧 Processing dim_visitor...")

if visitor_count > 0:
    dim_visitor = visitor_df.select(
        F.col("visitor_key").alias("visitor_id"),
        F.coalesce(F.col("ip_address"), F.lit("Unknown")).alias("ip_address"),
        F.coalesce(F.col("country"), F.lit("Unknown")).alias("country"),
        F.current_timestamp().alias("processed_at")
    ).distinct()

    print("  🔒 Ensuring visitor_id integrity...")
    dim_visitor = dim_visitor.withColumn(
        "visitor_id",
        F.when(F.col("visitor_id").isNull() | (F.col("visitor_id") == ""),
               F.concat(F.lit("visitor_"), F.monotonically_increasing_id()))
        .otherwise(F.col("visitor_id"))
    )

    dim_visitor_count = dim_visitor.count()
    print(f"  ✓ Processed {dim_visitor_count} visitor(s)")
else:
    print("  ⚠️  Skipping - no visitor data")
    dim_visitor = None
    dim_visitor_count = 0

# =============================================
# TRANSFORM: fact_media_engagement (UC-safe path extraction)
# =============================================

print("\n🔧 Processing fact_media_engagement...")

if visitor_count > 0:
    # Use UC-safe metadata column instead of input_file_name()
    visitor_with_path = visitor_df.withColumn("file_path", F.col("_metadata.file_path"))

    # Extract media_id from folder path
    p1 = F.regexp_extract(F.col("file_path"), r"/visitors/([a-z0-9]+)_\d{8}_\d{6}/", 1)
    p2 = F.regexp_extract(F.col("file_path"), r"/visitors/([a-z0-9]+)_", 1)
    visitor_with_media = visitor_with_path.withColumn("media_id", F.coalesce(p1, p2))

    # Ensure media_id present
    visitor_with_media = visitor_with_media.withColumn(
        "media_id",
        F.when(F.col("media_id").isNull() | (F.col("media_id") == ""),
               F.concat(F.lit("media_"), F.monotonically_increasing_id()))
        .otherwise(F.col("media_id"))
    )

    if "events" in visitor_df.columns:
        fact_engagement = (
            visitor_with_media
            .filter(F.col("events").isNotNull() & (F.size(F.col("events")) > 0))
            .select(
                F.col("media_id"),
                F.col("visitor_key").alias("visitor_id"),
                F.explode(F.col("events")).alias("event")
            )
            .filter(F.col("event.type") == "play")
            .withColumn("event_date", F.to_date(F.from_unixtime(F.col("event.time"))))
            .groupBy("media_id", "visitor_id", F.col("event_date").alias("date"))
            .agg(
                F.count("*").alias("play_count"),
                F.round(F.count("*") / F.lit(10.0), 2).alias("play_rate"),
                F.round(F.sum(F.coalesce(F.col("event.duration_watched").cast("double"), F.lit(0.0))), 2)
                  .alias("total_watch_time_seconds"),
                F.round(F.avg(F.coalesce(F.col("event.percent_watched").cast("double"), F.lit(0.0))), 2)
                  .alias("avg_percent_watched")
            )
            .withColumn("loaded_at", F.current_timestamp())
        )

        # Drop rows with null keys
        fact_engagement = fact_engagement.filter(
            F.col("media_id").isNotNull() &
            F.col("visitor_id").isNotNull() &
            F.col("date").isNotNull()
        )

        fact_engagement_count = fact_engagement.count()
        print(f"  ✓ Processed {fact_engagement_count} engagement fact(s)")
    else:
        print("  ⚠️  No events column found in visitor data")
        fact_engagement = None
        fact_engagement_count = 0
else:
    print("  ⚠️  Skipping - no visitor data")
    fact_engagement = None
    fact_engagement_count = 0

# =============================================
# DATA VALIDATION & CLEANING
# =============================================

print("\n🔍 Performing data validation...")

def validate_and_log(df, df_name, key_columns):
    if df is None:
        print(f"\n  📊 Validating {df_name}: NO DATA")
        return df
    total_count = df.count()
    print(f"\n  📊 Validating {df_name}:")
    print(f"     - Total records: {total_count}")
    for col_name in key_columns:
        null_count  = df.filter(F.col(col_name).isNull()).count()
        empty_count = df.filter((F.col(col_name) == "") | (F.trim(F.col(col_name)) == "")).count()
        if null_count > 0:
            print(f"     ⚠️  {null_count} records with NULL {col_name}")
            display(df.filter(F.col(col_name).isNull()).limit(3))
        if empty_count > 0:
            print(f"     ⚠️  {empty_count} records with empty {col_name}")
    return df

dim_media      = validate_and_log(dim_media, "dim_media", ["media_id"])
dim_visitor    = validate_and_log(dim_visitor, "dim_visitor", ["visitor_id"])
fact_engagement = validate_and_log(fact_engagement, "fact_engagement", ["media_id", "visitor_id"])

# Final safety filter
print("\n🧹 Final data cleaning...")
if dim_media is not None:
    c0 = dim_media.count()
    dim_media = dim_media.filter(F.col("media_id").isNotNull())
    print(f"  ✓ dim_media: {c0} → {dim_media.count()}")

if dim_visitor is not None:
    c0 = dim_visitor.count()
    dim_visitor = dim_visitor.filter(F.col("visitor_id").isNotNull())
    print(f"  ✓ dim_visitor: {c0} → {dim_visitor.count()}")

if fact_engagement is not None:
    c0 = fact_engagement.count()
    fact_engagement = fact_engagement.filter(
        F.col("media_id").isNotNull() & F.col("visitor_id").isNotNull()
    )
    print(f"  ✓ fact_engagement: {c0} → {fact_engagement.count()}")

# =============================================
# DEDUPLICATION (prevent PK violations)
# =============================================

print("\n🔍 Removing duplicates to prevent primary key violations...")

def remove_duplicates(df, df_name, key_columns):
    if df is None:
        return df
    dup_groups = df.groupBy(key_columns).count().filter(F.col("count") > 1)
    if dup_groups.count() > 0:
        print(f"  ⚠️  Found {dup_groups.count()} duplicate key groups in {df_name}")
        display(dup_groups.limit(3))
    w = Window.partitionBy(key_columns).orderBy(F.lit(1))
    df_dedup = df.withColumn("row_num", F.row_number().over(w)).filter("row_num = 1").drop("row_num")
    return df_dedup

dim_media       = remove_duplicates(dim_media, "dim_media", ["media_id"])
dim_visitor     = remove_duplicates(dim_visitor, "dim_visitor", ["visitor_id"])
if fact_engagement is not None:
    fact_engagement = remove_duplicates(fact_engagement, "fact_engagement", ["media_id", "visitor_id", "date"])

# Post-check
print("\n🔎 Final duplicate check...")
if dim_media is not None and dim_media.groupBy("media_id").count().filter("count > 1").count() == 0:
    print("  ✅ No duplicate media_id")
if dim_visitor is not None and dim_visitor.groupBy("visitor_id").count().filter("count > 1").count() == 0:
    print("  ✅ No duplicate visitor_id")

# =============================================
# FINAL DATA QUALITY CHECK
# =============================================

print("\n📊 Final Data Quality Report:")
if dim_media is not None:
    print(f"  📹 dim_media: {dim_media.count():,} records")
    print(f"     - Unique media_id: {dim_media.select('media_id').distinct().count():,}")

if dim_visitor is not None:
    print(f"  👥 dim_visitor: {dim_visitor.count():,} records")
    print(f"     - Unique visitor_id: {dim_visitor.select('visitor_id').distinct().count():,}")

if fact_engagement is not None:
    print(f"  📈 fact_engagement: {fact_engagement.count():,} records")
    print(f"     - Unique combos: {fact_engagement.select('media_id','visitor_id','date').distinct().count():,}")

# =============================================
# WRITE PROCESSED DATA
# =============================================

print("\n💾 Writing processed data...")

try:
    if dim_media is not None and dim_media.count() > 0:
        dim_media.write.mode("overwrite").parquet(f"{PROCESSED_PATH}/dim-media/")
        print(f"  ✅ Saved dim_media: {dim_media.count()} records")
        print("\n📊 Final dim_media sample:")
        display(dim_media.limit(3))
    else:
        print("  ⚠️  Skipped dim_media (no data)")

    if dim_visitor is not None and dim_visitor.count() > 0:
        dim_visitor.write.mode("overwrite").parquet(f"{PROCESSED_PATH}/dim-visitor/")
        print(f"  ✅ Saved dim_visitor: {dim_visitor.count()} records")
    else:
        print("  ⚠️  Skipped dim_visitor (no data)")

    if fact_engagement is not None and fact_engagement.count() > 0:
        fact_engagement.write.mode("overwrite").parquet(f"{PROCESSED_PATH}/fact-engagement/")
        print(f"  ✅ Saved fact_engagement: {fact_engagement.count()} records")
    else:
        print("  ⚠️  Skipped fact_engagement (no data)")
except Exception as e:
    print(f"  ❌ Error writing: {str(e)[:300]}")
    raise

# =============================================
# SUMMARY
# =============================================

print("\n" + "=" * 70)
print("✅ DATA PROCESSING COMPLETE")
print("=" * 70)
print(f"  dim_media:       {dim_media.count() if dim_media else 0:,} records")
print(f"  dim_visitor:     {dim_visitor.count() if dim_visitor else 0:,} records")
print(f"  fact_engagement: {fact_engagement.count() if fact_engagement else 0:,} records")
print(f"  Processed at:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)
print("\n🎯 Next Steps:")
print("  1) Verify no duplicate keys in final data")
print("  2) Run ADF pipeline to load to SQL Database")
print("  3) Check pipeline_execution_log for any issues")
print("=" * 70 + "\n")
