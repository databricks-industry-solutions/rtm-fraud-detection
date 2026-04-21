# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2: ML-Powered Fraud Detection with Lakebase & MLflow
# MAGIC
# MAGIC ## From Rules to Machine Learning
# MAGIC
# MAGIC In **Part 1**, we built a rule-based fraud detection system that scores transactions
# MAGIC using hardcoded thresholds (velocity > 5 = score 85, amount ratio > 10 = score 80, etc.).
# MAGIC
# MAGIC That works, but it has limitations:
# MAGIC - **Rules are static** -- they can't adapt to evolving fraud patterns
# MAGIC - **Thresholds are arbitrary** -- why is 5 transactions "suspicious"? Why not 4 or 6?
# MAGIC - **No learning** -- we never improve from past decisions
# MAGIC
# MAGIC In **Part 2**, we upgrade to a **machine learning** approach:
# MAGIC 1. **Stream rich features to Lakebase** (online feature store for sub-ms reads)
# MAGIC 2. **Train an ML model with MLflow** that learns fraud patterns from labeled data
# MAGIC 3. **Score transactions in real-time** using the model + live features
# MAGIC
# MAGIC
# MAGIC ### Key Concepts for Newcomers
# MAGIC
# MAGIC | Technology | What it is | Role in this demo |
# MAGIC |-----------|-----------|-------------------|
# MAGIC | **Lakebase** | Managed PostgreSQL on Databricks | Online feature store -- stores per-card features for sub-millisecond reads |
# MAGIC | **MLflow** | ML lifecycle platform | Train, version, and deploy fraud detection models |
# MAGIC | **jdbcStreaming** | Streaming Lakebase sink | Write features from Spark Streaming directly to Lakebase with upserts |
# MAGIC | **Feature Store** | A pattern for serving ML features | Lakebase holds the latest features per card, always fresh |
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - **Run Notebook 1 first** (or at least its Section 1 for Kafka config)
# MAGIC - Same cluster requirements as Notebook 1 (Real-Time Mode enabled)
# MAGIC - A **Lakebase instance** (we'll configure the name below)
# MAGIC
# MAGIC ### Notebook Sections
# MAGIC | # | Section | What it does |
# MAGIC |---|---------|-------------|
# MAGIC | 1 | Setup & Configuration | Kafka config, Lakebase connection, create tables |
# MAGIC | 2 | Reference Data | Merchant + card profiles (reused from Notebook 1) |
# MAGIC | 3 | Feature Engineering | Design and compute real-time features |
# MAGIC | 4 | Stream Features to Lakebase | `jdbcStreaming` pipeline to online feature store |
# MAGIC | 5 | Generate Labeled Training Data | Synthetic historical data with fraud labels |
# MAGIC | 6 | Train & Register MLflow Model | RandomForest trained on fraud features |
# MAGIC | 7 | ML Scoring Pipeline | Real-time scoring with MLflow model + Lakebase |
# MAGIC | 8 | Demo Execution | End-to-end: traffic → features → scores |
# MAGIC | 9 | Cleanup | Stop queries, drop tables |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 1: Setup & Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Verify Cluster & Load Kafka Config
# MAGIC
# MAGIC If you ran Notebook 1 on this same cluster, the Kafka config is already in `spark.conf`.
# MAGIC If not, we load it fresh from Databricks Secrets.

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk psycopg kafka-python

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,TODO: Configure your environment
# --- Kafka (only needed if running standalone, i.e., without running Notebook 1 first) ---
dbutils.widgets.text("secret_scope", "", "Kafka Secret Scope Name")

# --- Lakebase ---
# Create a Lakebase instance if you don't have one:
#   1. In the Databricks UI, go to SQL > Lakebase
#   2. Click "Create instance", give it a name (e.g., "rtm-lakebase-demo"), select a size
#   3. Enter that instance name in the widget below
dbutils.widgets.text("lakebase_instance", "", "Lakebase Instance Name")

# COMMAND ----------

# Verify real-time mode
assert spark.conf.get("spark.databricks.streaming.realTimeMode.enabled", "false") == "true", \
    "Real-time mode is not enabled! Add 'spark.databricks.streaming.realTimeMode.enabled true' to cluster Spark config."

# Enable JDBC streaming for Lakebase (required for jdbcStreaming sink)
spark.conf.set("spark.sql.streaming.jdbc.enabled", "true")

# Load Kafka config -- try spark.conf first (set by Notebook 1), fall back to secrets
try:
    kafka_brokers = spark.conf.get("demo.kafka.brokers")
    input_topic = spark.conf.get("demo.topic.input")
    print("Loaded Kafka config from Notebook 1 (spark.conf)")
except Exception:
    # Running standalone -- load from secrets via widget
    secret_scope = dbutils.widgets.get("secret_scope")
    if not secret_scope:
        raise ValueError(
            "Widget 'secret_scope' is empty. Set your Databricks Secret Scope name "
            "in the widget at the top of this notebook. See README for setup instructions."
        )
    kafka_brokers_tls = dbutils.secrets.get(secret_scope, "kafka-bootstrap-servers-tls")
    kafka_brokers = kafka_brokers_tls
    username = spark.sql("SELECT current_user()").collect()[0][0]
    user = username.split("@")[0].replace(".", "_")
    input_topic = f"{user}_raw_transactions"
    # Store for later cells
    spark.conf.set("demo.kafka.brokers", kafka_brokers)
    spark.conf.set("demo.kafka.brokers.tls", kafka_brokers_tls)
    spark.conf.set("demo.topic.input", input_topic)
    print("Loaded Kafka config from Databricks Secrets (standalone mode)")

# Get username for paths
username = spark.sql("SELECT current_user()").collect()[0][0]
user = username.split("@")[0].replace(".", "_")

print(f"""
Configuration:
  Kafka Brokers: {kafka_brokers}
  Input Topic:   {input_topic}
  User:          {username}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Configure Lakebase Connection
# MAGIC
# MAGIC **Lakebase** is Databricks' managed PostgreSQL service. We use it as an **online feature
# MAGIC store** -- a low-latency database where streaming pipelines write features and
# MAGIC scoring services read them in sub-milliseconds.
# MAGIC
# MAGIC Set your Lakebase instance name below. If you don't have one yet, create it from
# MAGIC the Databricks UI under **SQL > Lakebase**.

# COMMAND ----------

# ============================================================
# Lakebase config (reads from widget)
# ============================================================
LAKEBASE_INSTANCE_NAME = dbutils.widgets.get("lakebase_instance")
if not LAKEBASE_INSTANCE_NAME:
    raise ValueError(
        "Widget 'lakebase_instance' is empty. Enter your Lakebase instance name in the widget above. "
        "To create one: Databricks UI > SQL > Lakebase > Create instance."
    )
LAKEBASE_DATABASE = "databricks_postgres"  # Default database

# Feature store table names (must match the app's table names in apps/app.py)
FEATURE_TABLE = "card_features"
SCORES_TABLE  = "fraud_scores"

# Checkpoint paths for streaming
project_dir = f"/home/{username}/fraud_detection_ml_demo"
checkpoint_feature_store = f"{project_dir}/checkpoints/feature_store"
checkpoint_scoring       = f"{project_dir}/checkpoints/scoring"

print(f"""
Lakebase Configuration:
  Instance:       {LAKEBASE_INSTANCE_NAME}
  Database:       {LAKEBASE_DATABASE}
  Feature Table:  {FEATURE_TABLE}
  Scores Table:   {SCORES_TABLE}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Connect to Lakebase & Create Tables
# MAGIC
# MAGIC We connect via `psycopg` (PostgreSQL driver) to create our two tables:
# MAGIC - **card_features** -- per-card feature vector, upserted on every transaction
# MAGIC - **fraud_scores** -- per-transaction ML fraud score

# COMMAND ----------

def connect_to_lakebase(instance_name, database):
  """Generate credentials for a Lakebase instance."""
  from databricks.sdk import WorkspaceClient
  import uuid
  import psycopg

  w = WorkspaceClient()
  host = w.database.get_database_instance(name=instance_name).read_write_dns
  cred = w.database.generate_database_credential(
      request_id=str(uuid.uuid4()), instance_names=[instance_name])
  
  return psycopg.connect(
    host=host,
    port=5432,
    dbname=database,
    user=w.current_user.me().user_name,
    password=cred.token,
    sslmode='require'
  )

# Test connection and create tables
with connect_to_lakebase(LAKEBASE_INSTANCE_NAME, LAKEBASE_DATABASE) as conn, conn.cursor() as cur:
    # Feature store table: one row per card, upserted on each transaction
    cur.execute(f"DROP TABLE IF EXISTS {FEATURE_TABLE}")
    cur.execute(f"""
        CREATE TABLE {FEATURE_TABLE} (
            card_id              VARCHAR(20) PRIMARY KEY,
            last_transaction_id  VARCHAR(20),
            last_amount          DOUBLE PRECISION,
            amount_to_avg_ratio  DOUBLE PRECISION,
            exceeds_30d_max      INTEGER,
            is_online            INTEGER,
            hour_of_day          INTEGER,
            is_weekend           INTEGER,
            is_night_owl         INTEGER,
            is_high_risk_country INTEGER,
            is_cross_border      INTEGER,
            is_round_amount      INTEGER,
            is_small_probe       INTEGER,
            merchant_risk_score  INTEGER,
            amount_usd           DOUBLE PRECISION,
            last_latitude        DOUBLE PRECISION,
            last_longitude       DOUBLE PRECISION,
            updated_at           TIMESTAMP(6)
        )
    """)

    # Scores table: one row per transaction
    cur.execute(f"DROP TABLE IF EXISTS {SCORES_TABLE}")
    cur.execute(f"""
        CREATE TABLE {SCORES_TABLE} (
            transaction_id      VARCHAR(20) PRIMARY KEY,
            card_id             VARCHAR(20),
            amount_usd          DOUBLE PRECISION,
            fraud_probability   DOUBLE PRECISION,
            ml_decision         VARCHAR(10),
            model_version       VARCHAR(50),
            scored_at           TIMESTAMP(6)
        )
    """)

    conn.commit()
    print(f"Created tables: {FEATURE_TABLE}, {SCORES_TABLE}")

# Verify tables exist
with connect_to_lakebase(LAKEBASE_INSTANCE_NAME, LAKEBASE_DATABASE) as conn, conn.cursor() as cur:
    cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{FEATURE_TABLE}' ORDER BY ordinal_position")
    print(f"\n{FEATURE_TABLE} schema:")
    for row in cur.fetchall():
        print(f"  {row[0]:30s} {row[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2: Reference Data
# MAGIC
# MAGIC Loaded from the shared `resources/00_reference_data` notebook (same data used in Notebook 1).
# MAGIC Creates cached temp views `merchants` and `card_profiles`, plus the `RISK_TIER_MAP` dict.

# COMMAND ----------

# MAGIC %run ./resources/00_reference_data

# COMMAND ----------

from pyspark.sql.types import *
import pyspark.sql.functions as F

display(merchant_data)

# COMMAND ----------

display(card_profile_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3: Feature Engineering Design
# MAGIC
# MAGIC Good features are the difference between a mediocre and a great fraud model.
# MAGIC Real-world fraud detection systems use features from several categories:
# MAGIC
# MAGIC ### Feature Categories
# MAGIC
# MAGIC | Category | Feature | Why it matters |
# MAGIC |----------|---------|---------------|
# MAGIC | **Amount Anomaly** | `amount_to_avg_ratio` | A $3000 purchase on a card that averages $50 is 60x -- very suspicious |
# MAGIC | **Amount Anomaly** | `exceeds_30d_max` | Exceeding the 30-day max is a strong fraud signal |
# MAGIC | **Amount Anomaly** | `is_round_amount` | Fraudsters often use round numbers ($100, $500) for testing |
# MAGIC | **Amount Anomaly** | `is_small_probe` | Amounts < $5 are "card testing" -- checking if the card works |
# MAGIC | **Channel** | `is_online` | Online transactions have ~3x higher fraud rate than POS |
# MAGIC | **Temporal** | `hour_of_day` | Legitimate spending peaks 10am-8pm; fraud peaks 1am-5am |
# MAGIC | **Temporal** | `is_weekend` | Weekend shopping patterns differ from weekday |
# MAGIC | **Temporal** | `is_night_owl` | Transactions between midnight and 5am are higher risk |
# MAGIC | **Geographic** | `is_high_risk_country` | IP from Romania/Nigeria/Russia on a US card = red flag |
# MAGIC | **Geographic** | `is_cross_border` | IP country differs from cardholder home country |
# MAGIC | **Merchant** | `merchant_risk_score` | Gift cards and jewelry are high-risk categories |
# MAGIC
# MAGIC ### What the ML Model Learns vs. Rules
# MAGIC
# MAGIC In Notebook 1, we used hardcoded rules like "if amount > 10x average, score = 80".
# MAGIC The ML model instead learns **non-linear combinations** -- for example:
# MAGIC - `is_small_probe=1` alone is low risk (buying a coffee)
# MAGIC - `is_small_probe=1 AND is_online=1 AND is_high_risk_country=1` together = card testing fraud
# MAGIC
# MAGIC The model discovers these patterns automatically from labeled data.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4: Stream Features to Lakebase
# MAGIC
# MAGIC This is the core **online feature store** pipeline:
# MAGIC 1. Read raw transactions from Kafka
# MAGIC 2. Enrich with merchant + card profile data (inline lookups -- avoids slot pressure)
# MAGIC 3. Compute feature columns
# MAGIC 4. Write to Lakebase via `jdbcStreaming` (upserts by `card_id`)
# MAGIC
# MAGIC After this pipeline starts, **Lakebase always has the latest features for every card**,
# MAGIC updated within milliseconds of each transaction.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Read from Kafka & Parse

# COMMAND ----------

import json
transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("card_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount_usd", DoubleType()),
    StructField("currency", StringType()),
    StructField("channel", StringType()),
    StructField("ip_country", StringType()),
    StructField("device_fingerprint", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("event_time", StringType()),
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_brokers)
    .option("assign", json.dumps({input_topic: [0, 1]}))  # Read only 2 of 8 partitions
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("kafka.security.protocol", "SSL")
    .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(F.from_json(F.col("json_value"), transaction_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", F.to_timestamp(F.col("event_time")))
)

print(f"Kafka source configured: topic={input_topic}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Enrich & Compute Features

# COMMAND ----------

# Compute features WITHOUT joins -- use inline lookups instead
# This avoids broadcast exchanges that consume concurrent scheduler slots

feature_stream = (
    parsed_stream

    # --- Card profile lookups (inlined) ---
    .withColumn("home_country",
        F.when(F.col("card_id") == "card_0004", "GB").otherwise("US"))
    .withColumn("avg_txn_amount",
        F.when(F.col("card_id") == "card_0001", 65.0)
         .when(F.col("card_id") == "card_0002", 120.0)
         .when(F.col("card_id") == "card_0003", 45.0)
         .when(F.col("card_id") == "card_0004", 85.0)
         .when(F.col("card_id") == "card_0005", 50.0)
         .otherwise(65.0))
    .withColumn("max_txn_30d",
        F.when(F.col("card_id") == "card_0001", 320.0)
         .when(F.col("card_id") == "card_0002", 450.0)
         .when(F.col("card_id") == "card_0003", 200.0)
         .when(F.col("card_id") == "card_0004", 380.0)
         .when(F.col("card_id") == "card_0005", 250.0)
         .otherwise(300.0))

    # --- Merchant risk lookup (inlined) ---
    .withColumn("merchant_risk_score",
        F.when(F.col("merchant_id") == "merch_0006", 80)                          # VERY_HIGH
         .when(F.col("merchant_id").isin("merch_0001", "merch_0005", "merch_0007"), 50)  # HIGH
         .when(F.col("merchant_id").isin("merch_0004", "merch_0009", "merch_0010"), 20)  # MEDIUM
         .otherwise(0))                                                            # LOW

    # --- Amount anomaly features ---
    .withColumn("amount_to_avg_ratio", F.round(F.col("amount_usd") / F.col("avg_txn_amount"), 2))
    .withColumn("exceeds_30d_max", (F.col("amount_usd") > F.col("max_txn_30d")).cast("integer"))
    .withColumn("is_round_amount", ((F.col("amount_usd") % 50 == 0) & (F.col("amount_usd") > 0)).cast("integer"))
    .withColumn("is_small_probe", (F.col("amount_usd") < 5.0).cast("integer"))

    # --- Channel feature ---
    .withColumn("is_online", (F.col("channel") == "ONLINE").cast("integer"))

    # --- Temporal features ---
    .withColumn("hour_of_day", F.hour(F.col("event_time")))
    .withColumn("is_weekend", F.dayofweek(F.col("event_time")).isin(1, 7).cast("integer"))
    .withColumn("is_night_owl", (F.hour(F.col("event_time")).between(0, 5)).cast("integer"))

    # --- Geographic features ---
    .withColumn("is_high_risk_country", F.col("ip_country").isin("RO", "NG", "CN", "RU", "UA").cast("integer"))
    .withColumn("is_cross_border", (F.col("ip_country") != F.col("home_country")).cast("integer"))

    # --- Metadata ---
    .withColumn("updated_at", F.current_timestamp())
)

feature_output = feature_stream.select(
    F.col("card_id"),
    F.col("transaction_id").alias("last_transaction_id"),
    F.col("amount_usd").alias("last_amount"),
    F.col("amount_to_avg_ratio"),
    F.col("exceeds_30d_max"),
    F.col("is_online"),
    F.col("hour_of_day"),
    F.col("is_weekend"),
    F.col("is_night_owl"),
    F.col("is_high_risk_country"),
    F.col("is_cross_border"),
    F.col("is_round_amount"),
    F.col("is_small_probe"),
    F.col("merchant_risk_score"),
    F.col("amount_usd"),
    F.col("latitude").alias("last_latitude"),
    F.col("longitude").alias("last_longitude"),
    F.col("updated_at"),
)

print("Feature computation pipeline configured (join-free).")

# (Broadcast-join version removed -- using inline lookups above to avoid concurrent scheduler slot issues on single-node clusters)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Write Features to Lakebase via jdbcStreaming
# MAGIC
# MAGIC The `jdbcStreaming` sink writes directly to Lakebase with **upsert** semantics:
# MAGIC each new transaction for a card **updates** that card's feature row.
# MAGIC This means Lakebase always reflects the card's latest state.
# MAGIC
# MAGIC The `realTime` trigger tells Spark to process with minimal latency (40-300ms)
# MAGIC while checkpointing every 30 seconds for durability.

# COMMAND ----------

print(f"Active streaming queries: {len(spark.streams.active)}")
for q in spark.streams.active:
    print(f"  Stopping: {q.name or q.id}")
    q.stop()
print("All queries stopped.")

# COMMAND ----------

dbutils.fs.rm(checkpoint_feature_store, recurse=True)

feature_query = (
    feature_output
    .withWatermark("updated_at", "10 seconds")
    .writeStream
    .format("jdbcStreaming")
    .option("instancename", LAKEBASE_INSTANCE_NAME)
    .option("dbtable", FEATURE_TABLE)
    .option("upsertkey", "card_id")
    .option("batchinterval", "10 milliseconds")
    .option("checkpointLocation", checkpoint_feature_store)
    .trigger(realTime="30 seconds")
    .outputMode("update")
    .start()
)

print(f"Feature store pipeline started -> Lakebase table: {FEATURE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5: Generate Labeled Training Data
# MAGIC
# MAGIC To train an ML model, we need **labeled** historical data -- transactions where we
# MAGIC know the outcome (fraud vs. legitimate). In production, this comes from chargebacks
# MAGIC and manual reviews. For this demo, we generate synthetic data with realistic patterns.
# MAGIC
# MAGIC ### How We Simulate Realistic Fraud Patterns
# MAGIC
# MAGIC | Pattern | Normal Transaction | Fraudulent Transaction |
# MAGIC |---------|-------------------|----------------------|
# MAGIC | Amount | 0.2x - 2x average | 5x - 50x average |
# MAGIC | Channel | 60% POS, 40% online | 90% online |
# MAGIC | Time | 8am - 10pm | 1am - 5am |
# MAGIC | Country | 95% domestic | 70% cross-border, high-risk |
# MAGIC | Merchant | Mixed categories | Gift cards, electronics, jewelry |
# MAGIC | Amount pattern | Varied amounts | Round numbers, small probes |

# COMMAND ----------

import pandas as pd
import numpy as np

np.random.seed(42)

def generate_training_data(n_normal=8000, n_fraud=800):
    """
    Generate synthetic labeled fraud data with realistic feature distributions.
    Fraud is ~10% of total -- similar to real-world class imbalance (but compressed
    for demo purposes; real fraud rates are ~0.1-0.5%).
    """
    # --- Normal transactions ---
    normal = pd.DataFrame({
        "amount_to_avg_ratio":  np.clip(np.random.lognormal(0.0, 0.5, n_normal), 0.1, 5.0),
        "exceeds_30d_max":      np.random.choice([0, 1], n_normal, p=[0.95, 0.05]),
        "is_online":            np.random.choice([0, 1], n_normal, p=[0.60, 0.40]),
        "hour_of_day":          np.random.choice(range(7, 23), n_normal),
        "is_weekend":           np.random.choice([0, 1], n_normal, p=[0.72, 0.28]),
        "is_night_owl":         np.random.choice([0, 1], n_normal, p=[0.97, 0.03]),
        "is_high_risk_country": np.zeros(n_normal, dtype=int),
        "is_cross_border":      np.random.choice([0, 1], n_normal, p=[0.95, 0.05]),
        "is_round_amount":      np.random.choice([0, 1], n_normal, p=[0.88, 0.12]),
        "is_small_probe":       np.random.choice([0, 1], n_normal, p=[0.92, 0.08]),
        "merchant_risk_score":  np.random.choice([0, 20, 50], n_normal, p=[0.55, 0.35, 0.10]),
        "label":                np.zeros(n_normal, dtype=int),
    })

    # --- Fraudulent transactions ---
    fraud = pd.DataFrame({
        "amount_to_avg_ratio":  np.clip(np.random.lognormal(2.0, 0.8, n_fraud), 3.0, 60.0),
        "exceeds_30d_max":      np.random.choice([0, 1], n_fraud, p=[0.25, 0.75]),
        "is_online":            np.random.choice([0, 1], n_fraud, p=[0.10, 0.90]),
        "hour_of_day":          np.random.choice(list(range(0, 6)) + [23], n_fraud),
        "is_weekend":           np.random.choice([0, 1], n_fraud, p=[0.50, 0.50]),
        "is_night_owl":         np.random.choice([0, 1], n_fraud, p=[0.30, 0.70]),
        "is_high_risk_country": np.random.choice([0, 1], n_fraud, p=[0.25, 0.75]),
        "is_cross_border":      np.random.choice([0, 1], n_fraud, p=[0.15, 0.85]),
        "is_round_amount":      np.random.choice([0, 1], n_fraud, p=[0.35, 0.65]),
        "is_small_probe":       np.random.choice([0, 1], n_fraud, p=[0.50, 0.50]),
        "merchant_risk_score":  np.random.choice([50, 80], n_fraud, p=[0.40, 0.60]),
        "label":                np.ones(n_fraud, dtype=int),
    })

    df = pd.concat([normal, fraud], ignore_index=True).sample(frac=1, random_state=42).reset_index(drop=True)
    return df

training_pdf = generate_training_data()

print(f"Training data: {len(training_pdf)} rows")
print(f"  Legitimate: {(training_pdf['label'] == 0).sum()} ({(training_pdf['label'] == 0).mean():.1%})")
print(f"  Fraudulent: {(training_pdf['label'] == 1).sum()} ({(training_pdf['label'] == 1).mean():.1%})")

# Convert to Spark DataFrame for display
training_sdf = spark.createDataFrame(training_pdf)
display(training_sdf.limit(20))

# COMMAND ----------

# Show feature distributions by label
display(
    training_sdf.groupBy("label").agg(
        F.round(F.avg("amount_to_avg_ratio"), 2).alias("avg_amount_ratio"),
        F.round(F.avg("is_online"), 2).alias("pct_online"),
        F.round(F.avg("hour_of_day"), 1).alias("avg_hour"),
        F.round(F.avg("is_high_risk_country"), 2).alias("pct_high_risk_country"),
        F.round(F.avg("is_cross_border"), 2).alias("pct_cross_border"),
        F.round(F.avg("merchant_risk_score"), 1).alias("avg_merchant_risk"),
        F.round(F.avg("is_night_owl"), 2).alias("pct_night_owl"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 6: Train & Register MLflow Model
# MAGIC
# MAGIC We train a **Random Forest** classifier on the synthetic labeled data.
# MAGIC Random Forest is a good choice for fraud detection because:
# MAGIC - It handles mixed feature types (binary flags + continuous ratios)
# MAGIC - It captures non-linear feature interactions (the key patterns!)
# MAGIC - It outputs calibrated probabilities (not just yes/no)
# MAGIC - It's interpretable via feature importance
# MAGIC
# MAGIC We use **MLflow** to:
# MAGIC 1. Track the experiment (parameters, metrics, artifacts)
# MAGIC 2. Log the trained model with its signature
# MAGIC 3. Register it in the Model Registry for versioning

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report, roc_auc_score, precision_score,
    recall_score, f1_score, confusion_matrix
)

# Feature columns (must match what the streaming pipeline produces)
FEATURE_COLUMNS = [
    "amount_to_avg_ratio",
    "exceeds_30d_max",
    "is_online",
    "hour_of_day",
    "is_weekend",
    "is_night_owl",
    "is_high_risk_country",
    "is_cross_border",
    "is_round_amount",
    "is_small_probe",
    "merchant_risk_score",
]

MODEL_NAME = f"{user}_fraud_detection_rf"

# Split data
X = training_pdf[FEATURE_COLUMNS]
y = training_pdf["label"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

print(f"Train: {len(X_train)} rows | Test: {len(X_test)} rows")
print(f"Features: {FEATURE_COLUMNS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Train the Model

# COMMAND ----------

mlflow.set_experiment(f"/Users/{username}/fraud_detection_ml_demo")

with mlflow.start_run(run_name="fraud_rf_v1") as run:
    # Hyperparameters
    params = {
        "n_estimators": 200,
        "max_depth": 8,
        "min_samples_split": 10,
        "min_samples_leaf": 5,
        "class_weight": "balanced",  # Handle class imbalance
        "random_state": 42,
    }

    model = RandomForestClassifier(**params)
    model.fit(X_train, y_train)

    # Predictions
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    # Metrics
    metrics = {
        "auc_roc": roc_auc_score(y_test, y_prob),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1": f1_score(y_test, y_pred),
        "test_size": len(X_test),
        "fraud_rate": y.mean(),
    }

    # Log everything to MLflow
    mlflow.log_params(params)
    mlflow.log_metrics(metrics)

    # Log model with signature
    from mlflow.models import infer_signature
    signature = infer_signature(X_train, model.predict_proba(X_train)[:, 1])
    mlflow.sklearn.log_model(model, "fraud_model", signature=signature)

    # Feature importance
    importance = pd.DataFrame({
        "feature": FEATURE_COLUMNS,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)

    mlflow.log_table(importance, "feature_importance.json")

    run_id = run.info.run_id
    print(f"""
Model Training Complete!
========================
Run ID:    {run_id}
AUC-ROC:   {metrics['auc_roc']:.4f}
Precision: {metrics['precision']:.4f}
Recall:    {metrics['recall']:.4f}
F1 Score:  {metrics['f1']:.4f}
""")

    print("Feature Importance:")
    for _, row in importance.iterrows():
        bar = "█" * int(row["importance"] * 50)
        print(f"  {row['feature']:30s} {row['importance']:.3f} {bar}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Register the Model
# MAGIC
# MAGIC Registering the model in MLflow's Model Registry gives us versioning,
# MAGIC stage transitions (Staging → Production), and a single URI to load it.

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()
model_uri = f"runs:/{run_id}/fraud_model"

# Register (creates the model if it doesn't exist, or adds a new version)
registered = mlflow.register_model(model_uri, MODEL_NAME)

print(f"""
Model Registered:
  Name:    {MODEL_NAME}
  Version: {registered.version}
  URI:     models:/{MODEL_NAME}/{registered.version}

You can view this model in the MLflow UI under 'Models'.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 7: ML Scoring Pipeline
# MAGIC
# MAGIC Now we bring it all together: a streaming pipeline that reads transactions from Kafka,
# MAGIC computes the same features, scores each transaction with the trained ML model,
# MAGIC and writes the fraud probability + decision to Lakebase.
# MAGIC
# MAGIC The ML model is loaded as a **Spark UDF** (User-Defined Function), which means
# MAGIC it runs natively inside the streaming pipeline -- no external API calls needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Load Model as Spark UDF

# COMMAND ----------

model_version = registered.version
scoring_model_uri = f"models:/{MODEL_NAME}/{model_version}"

# Load the MLflow model as a Spark UDF that returns fraud probability
predict_udf = mlflow.pyfunc.spark_udf(spark, scoring_model_uri, result_type="double")

print(f"Loaded model as Spark UDF: {scoring_model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Build Scoring Pipeline
# MAGIC
# MAGIC This pipeline mirrors Section 4 (same Kafka source, same feature computation),
# MAGIC but adds the ML scoring step before writing to Lakebase.

# COMMAND ----------

# Read from the same Kafka topic (2 partitions to stay within slot limits)
raw_stream_scoring = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_brokers)
    .option("assign", json.dumps({input_topic: [0, 1]}))
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("kafka.security.protocol", "SSL")
    .load()
)

parsed_scoring = (
    raw_stream_scoring
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(F.from_json(F.col("json_value"), transaction_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", F.to_timestamp(F.col("event_time")))
)

# FIX 2: Inline lookups instead of broadcast joins (avoids concurrent scheduler slot issue)
scored_features = (
    parsed_scoring
    # Card profile lookups
    .withColumn("home_country",
        F.when(F.col("card_id") == "card_0004", "GB").otherwise("US"))
    .withColumn("avg_txn_amount",
        F.when(F.col("card_id") == "card_0001", 65.0)
         .when(F.col("card_id") == "card_0002", 120.0)
         .when(F.col("card_id") == "card_0003", 45.0)
         .when(F.col("card_id") == "card_0004", 85.0)
         .when(F.col("card_id") == "card_0005", 50.0)
         .otherwise(65.0))
    .withColumn("max_txn_30d",
        F.when(F.col("card_id") == "card_0001", 320.0)
         .when(F.col("card_id") == "card_0002", 450.0)
         .when(F.col("card_id") == "card_0003", 200.0)
         .when(F.col("card_id") == "card_0004", 380.0)
         .when(F.col("card_id") == "card_0005", 250.0)
         .otherwise(300.0))
    # Merchant risk lookup
    .withColumn("merchant_risk_score",
        F.when(F.col("merchant_id") == "merch_0006", 80)
         .when(F.col("merchant_id").isin("merch_0001", "merch_0005", "merch_0007"), 50)
         .when(F.col("merchant_id").isin("merch_0004", "merch_0009", "merch_0010"), 20)
         .otherwise(0))
    # Amount anomaly features
    .withColumn("amount_to_avg_ratio", F.round(F.col("amount_usd") / F.col("avg_txn_amount"), 2))
    .withColumn("exceeds_30d_max", (F.col("amount_usd") > F.col("max_txn_30d")).cast("integer"))
    .withColumn("is_round_amount", ((F.col("amount_usd") % 50 == 0) & (F.col("amount_usd") > 0)).cast("integer"))
    .withColumn("is_small_probe", (F.col("amount_usd") < 5.0).cast("integer"))
    # Channel
    .withColumn("is_online", (F.col("channel") == "ONLINE").cast("integer"))
    # Temporal
    .withColumn("hour_of_day", F.hour(F.col("event_time")))
    .withColumn("is_weekend", F.dayofweek(F.col("event_time")).isin(1, 7).cast("integer"))
    .withColumn("is_night_owl", (F.hour(F.col("event_time")).between(0, 5)).cast("integer"))
    # Geographic
    .withColumn("is_high_risk_country", F.col("ip_country").isin("RO", "NG", "CN", "RU", "UA").cast("integer"))
    .withColumn("is_cross_border", (F.col("ip_country") != F.col("home_country")).cast("integer"))
)

# Apply ML model
ml_scored = (
    scored_features
    .withColumn("fraud_probability", F.round(
        predict_udf(F.struct(*[F.col(c) for c in FEATURE_COLUMNS])), 4
    ))
    .withColumn("ml_decision",
        F.when(F.col("fraud_probability") < 0.3, "APPROVED")
         .when(F.col("fraud_probability") < 0.7, "FLAGGED")
         .otherwise("BLOCKED")
    )
    .withColumn("model_version", F.lit(f"{MODEL_NAME}/v{model_version}"))
    .withColumn("scored_at", F.current_timestamp())
)

# Lakebase scores output
scores_output = ml_scored.select(
    F.col("transaction_id"),
    F.col("card_id"),
    F.col("amount_usd"),
    F.col("fraud_probability"),
    F.col("ml_decision"),
    F.col("model_version"),
    F.col("scored_at"),
)

# In-memory table for monitoring
ml_scored.select(
    "transaction_id", "card_id", "amount_usd",
    "fraud_probability", "ml_decision",
    "amount_to_avg_ratio", "is_online", "is_high_risk_country",
    "is_cross_border", "is_night_owl", "merchant_risk_score",
    "scored_at",
).writeStream.format("memory").queryName("ml_fraud_scores").outputMode("append").start()

print("ML scoring pipeline configured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Write Scores to Lakebase

# COMMAND ----------

dbutils.fs.rm(checkpoint_scoring, recurse=True)

scoring_query = (
    scores_output
    #.withWatermark("scored_at", "10 seconds") #stateful streaming
    .writeStream
    .format("jdbcStreaming")
    .option("instancename", LAKEBASE_INSTANCE_NAME)
    .option("dbtable", SCORES_TABLE)
    .option("upsertkey", "transaction_id")
    .option("batchinterval", "10 milliseconds")
    .option("checkpointLocation", checkpoint_scoring)
    .trigger(realTime="5 minutes") #longer the better on performance
    .outputMode("update")
    .start()
)

print(f"Scoring pipeline started -> Lakebase table: {SCORES_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 8: Demo Execution
# MAGIC
# MAGIC Both pipelines are running. Now let's generate traffic and watch features
# MAGIC and scores appear in Lakebase in real time.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Data Generator
# MAGIC
# MAGIC Loaded from the shared `resources/00_datagenerator` notebook (same generator used in Notebook 1).
# MAGIC Provides: `send_transaction()`, `generate_normal_transaction()`, `generate_amount_spike()`,
# MAGIC `generate_small_probe()`, `inject_fraud_pattern()`, `run_baseline_generator()`.

# COMMAND ----------

# MAGIC %run ./resources/00_datagenerator

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Send Baseline Traffic
# MAGIC
# MAGIC Generate 30 seconds of normal transactions so we can see features populate Lakebase.

# COMMAND ----------

run_baseline_generator(duration_seconds=30, tps=5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Query Features in Lakebase
# MAGIC
# MAGIC Let's see what the online feature store looks like now. Each card should have
# MAGIC its latest features updated in near-real-time.

# COMMAND ----------

time.sleep(10)

with connect_to_lakebase(LAKEBASE_INSTANCE_NAME, LAKEBASE_DATABASE) as conn, conn.cursor() as cur:
    cur.execute(f"SELECT count(*) FROM {FEATURE_TABLE}")
    total = cur.fetchone()[0]
    print(f"Total rows in {FEATURE_TABLE}: {total}\n")

    cur.execute(f"SELECT * FROM {FEATURE_TABLE} ORDER BY updated_at DESC")
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]

display(spark.createDataFrame([dict(zip(col_names, row)) for row in rows]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.4 Inject Fraud & Compare ML Scores
# MAGIC
# MAGIC Now let's inject some fraudulent transactions and see how the ML model scores them
# MAGIC compared to normal traffic.

# COMMAND ----------

import random, time

print("Injecting fraud patterns...\n")

# Card testing: 3 small probes (each call generates a unique transaction)
card = random.choice(CARD_IDS)
print(f"--- Card Testing Attack on {card} ---")
for _ in range(3):
    for txn in generate_small_probe(card):
        send_transaction(txn)
        time.sleep(1)

# Amount spike
print(f"\n--- Amount Spike on {card} ---")
for txn in generate_amount_spike(card):
    send_transaction(txn)

# A few more normal transactions for contrast
print(f"\n--- Normal traffic for contrast ---")
for _ in range(5):
    send_transaction(generate_normal_transaction())
    time.sleep(0.5)

print("\nFraud injection complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.5 View ML Scores in Lakebase

# COMMAND ----------

time.sleep(300)

with connect_to_lakebase(LAKEBASE_INSTANCE_NAME, LAKEBASE_DATABASE) as conn, conn.cursor() as cur:
    cur.execute(f"SELECT count(*) FROM {SCORES_TABLE}")
    total = cur.fetchone()[0]
    print(f"Total scored transactions: {total}\n")

    cur.execute(f"""
        SELECT transaction_id, card_id, amount_usd,
               round(fraud_probability::numeric, 4) as fraud_prob,
               ml_decision, model_version, scored_at
        FROM {SCORES_TABLE}
        ORDER BY scored_at DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]

display(spark.createDataFrame([dict(zip(col_names, row)) for row in rows]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.6 Compare: ML Scores for Normal vs. Fraud

# COMMAND ----------

display(spark.sql("""
    SELECT ml_decision,
           COUNT(*) as count,
           ROUND(AVG(fraud_probability), 4) as avg_fraud_prob,
           ROUND(MIN(fraud_probability), 4) as min_fraud_prob,
           ROUND(MAX(fraud_probability), 4) as max_fraud_prob
    FROM ml_fraud_scores
    GROUP BY ml_decision
    ORDER BY avg_fraud_prob
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT transaction_id, card_id, amount_usd,
           ROUND(fraud_probability, 4) as fraud_prob,
           ml_decision,
           amount_to_avg_ratio, is_online, is_high_risk_country,
           is_cross_border, is_night_owl, merchant_risk_score
    FROM ml_fraud_scores
    ORDER BY fraud_probability DESC
    LIMIT 20
"""))

# COMMAND ----------

# DBTITLE 1,Grant App access to all tables
# If you want to grant access to all tables in the Lakebase schema, uncomment the following code
# with connect_to_lakebase(LAKEBASE_INSTANCE_NAME, LAKEBASE_DATABASE) as conn, conn.cursor() as cur:
#       conn.autocommit = True
#       cur.execute("GRANT ALL ON ALL TABLES IN SCHEMA public TO PUBLIC")
#       print("Done") 

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 9: Cleanup
# MAGIC
# MAGIC Stop all streaming queries and optionally drop Lakebase tables.

# COMMAND ----------

# Stop all streaming queries
for q in spark.streams.active:
    print(f"Stopping: {q.name or q.id}")
    q.stop()

# Uncache reference tables
spark.catalog.uncacheTable("merchants")
spark.catalog.uncacheTable("card_profiles")

print("All queries stopped, tables uncached.")

# COMMAND ----------

# Optional: drop Lakebase tables (uncomment to run)
# with connect_to_lakebase(LAKEBASE_INSTANCE_NAME, LAKEBASE_DATABASE) as conn, conn.cursor() as cur:
#     cur.execute(f"DROP TABLE IF EXISTS {FEATURE_TABLE}")
#     cur.execute(f"DROP TABLE IF EXISTS {SCORES_TABLE}")
#     conn.commit()
#     print("Lakebase tables dropped.")

# Optional: clean up checkpoints
# dbutils.fs.rm(f"{project_dir}/checkpoints", recurse=True)
# print("Checkpoints cleared.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## End of Notebook
# MAGIC
# MAGIC **References:**
# MAGIC - [Databricks Lakebase Documentation](https://docs.databricks.com/en/database/lakebase.html)
# MAGIC - [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)
# MAGIC - [Real-Time Mode in Structured Streaming](https://docs.databricks.com/en/structured-streaming/real-time.html)
# MAGIC - [Structured Streaming + Lakebase](https://docs.databricks.com/en/database/lakebase-streaming.html)
# MAGIC - [Feature Store Concepts](https://docs.databricks.com/en/machine-learning/feature-store/index.html)