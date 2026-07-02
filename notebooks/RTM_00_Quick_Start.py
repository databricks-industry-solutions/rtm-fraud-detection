# Databricks notebook source
# MAGIC %md
# MAGIC # Quick Start: Real-Time Mode Fraud Detection (No Kafka Required)
# MAGIC
# MAGIC This self-contained notebook demonstrates Databricks **Real-Time Mode** with
# MAGIC **zero external dependencies** — no Kafka, no Lakebase, no secrets.
# MAGIC
# MAGIC It uses a built-in `rate` source for synthetic transactions and `display()` for
# MAGIC output, so you can validate RTM on any properly configured cluster in under 5 minutes.
# MAGIC
# MAGIC ### What This Shows
# MAGIC - **Real-Time trigger** (`trigger(realTime=...)`) for sub-300ms processing
# MAGIC - **`transformWithState`** for per-card velocity tracking with TTL
# MAGIC - **Python scalar UDFs** for fraud scoring inline in the pipeline
# MAGIC - **Live `display()`** of streaming results in the notebook
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Databricks Runtime 18 LTS or above
# MAGIC - Dedicated cluster with RTM enabled (`spark.databricks.streaming.realTimeMode.enabled true`)
# MAGIC - Photon disabled, autoscaling disabled

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 1: Verify Cluster Configuration

# COMMAND ----------

rtm_enabled = spark.conf.get("spark.databricks.streaming.realTimeMode.enabled", "false")
assert rtm_enabled == "true", (
    "Real-time mode is not enabled! Add 'spark.databricks.streaming.realTimeMode.enabled true' "
    "to your cluster's Spark config."
)

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1")
spark.conf.set("spark.sql.shuffle.partitions", "10")

print("Cluster is configured for Real-Time Mode.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2: Generate Synthetic Transactions
# MAGIC
# MAGIC The `rate` source generates rows at a configurable rate. We transform each row
# MAGIC into a realistic-looking transaction using Spark SQL expressions — no external
# MAGIC data source needed.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import *

ROWS_PER_SECOND = 10

rate_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", ROWS_PER_SECOND)
    .option("numPartitions", 2)
    .load()
)

categories = F.array(F.lit("GROCERY"), F.lit("GAS"), F.lit("DINING"), F.lit("ELECTRONICS"), F.lit("JEWELRY"))

synthetic_transactions = (
    rate_stream
    .withColumn("transaction_id", F.concat(F.lit("txn_"), col("value").cast("string")))
    .withColumn("card_id", F.concat(F.lit("card_"), F.lpad(((col("value") % 5) + 1).cast("string"), 4, "0")))
    .withColumn("merchant_category", F.element_at(categories, (col("value") % 5 + 1).cast("int")))
    .withColumn("amount_usd", F.round(
        F.when(F.rand() > 0.95, F.rand() * 3000 + 500)   # 5% high-value (potential fraud)
         .otherwise(F.rand() * 145 + 5), 2))               # 95% normal range
    .withColumn("channel", F.when(F.rand() > 0.6, "ONLINE").otherwise("POS"))
    .withColumn("ip_country",
        F.when(F.rand() > 0.97, "RO")
         .when(F.rand() > 0.95, "NG")
         .otherwise("US"))
    .withColumn("latitude", F.lit(40.7128) + (F.rand() - 0.5) * 0.2)
    .withColumn("longitude", F.lit(-74.0060) + (F.rand() - 0.5) * 0.2)
    .withColumn("event_time", col("timestamp"))
    .drop("timestamp", "value")
)

print(f"Synthetic transaction stream configured: {ROWS_PER_SECOND} TPS")
synthetic_transactions.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3: Stateful Velocity Tracking
# MAGIC
# MAGIC `transformWithState` maintains per-card state across the stream. For each `card_id`,
# MAGIC we track the transaction count in a 60-second window. State auto-expires via TTL.
# MAGIC
# MAGIC In Real-Time Mode, `handleInputRows` is called **once per row** (the iterator yields
# MAGIC a single value), enabling true record-at-a-time processing.

# COMMAND ----------

import math
from typing import Iterator
from pyspark.sql import Row
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle

velocity_state_schema = StructType([
    StructField("count", LongType()),
    StructField("last_lat", DoubleType()),
    StructField("last_lon", DoubleType()),
    StructField("last_ts", TimestampType()),
])

velocity_output_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("card_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount_usd", DoubleType()),
    StructField("channel", StringType()),
    StructField("ip_country", StringType()),
    StructField("event_time", TimestampType()),
    StructField("velocity_60s", LongType()),
    StructField("geo_distance_km", DoubleType()),
    StructField("time_diff_seconds", LongType()),
])


class VelocityProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.state = handle.getValueState("velocity", velocity_state_schema, 60000)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        for row in rows:
            prev = self.state.get()
            if prev is None:
                count, last_lat, last_lon, last_ts = 0, row["latitude"], row["longitude"], row["event_time"]
            else:
                count, last_lat, last_lon, last_ts = prev[0], prev[1], prev[2], prev[3]

            if count > 0:
                d_lat = math.radians(row["latitude"] - last_lat)
                d_lon = math.radians(row["longitude"] - last_lon)
                a = (math.sin(d_lat / 2) ** 2 +
                     math.cos(math.radians(last_lat)) * math.cos(math.radians(row["latitude"])) *
                     math.sin(d_lon / 2) ** 2)
                geo_km = 6371.0 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
                time_diff = int(row["event_time"].timestamp() - last_ts.timestamp())
            else:
                geo_km, time_diff = 0.0, 0

            new_count = count + 1
            self.state.update((new_count, row["latitude"], row["longitude"], row["event_time"]))

            yield Row(
                transaction_id=row["transaction_id"],
                card_id=row["card_id"],
                merchant_category=row["merchant_category"],
                amount_usd=row["amount_usd"],
                channel=row["channel"],
                ip_country=row["ip_country"],
                event_time=row["event_time"],
                velocity_60s=new_count,
                geo_distance_km=geo_km,
                time_diff_seconds=time_diff,
            )

    def close(self) -> None:
        pass


velocity_stream = (
    synthetic_transactions
    .groupBy("card_id")
    .transformWithState(
        statefulProcessor=VelocityProcessor(),
        outputStructType=velocity_output_schema,
        outputMode="Update",
        timeMode="processingTime",
    )
)

print("Velocity tracking configured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4: Fraud Scoring
# MAGIC
# MAGIC Simplified scoring using three signals: velocity, amount anomaly, and country risk.
# MAGIC Each signal produces a 0-100 score; the final score is a weighted average.

# COMMAND ----------

from pyspark.sql.functions import udf

@udf(returnType=IntegerType())
def score_velocity(count):
    if count is None or count <= 1: return 0
    elif count == 2: return 20
    elif count == 3: return 40
    elif count == 4: return 60
    else: return 85

@udf(returnType=IntegerType())
def score_amount(amount):
    if amount is None: return 0
    if amount > 2000: return 90
    elif amount > 1000: return 60
    elif amount > 500: return 30
    else: return 0

@udf(returnType=IntegerType())
def score_country(ip_country):
    return 70 if ip_country in ("RO", "NG", "CN", "RU", "UA") else 0

scored_stream = (
    velocity_stream
    .withColumn("velocity_score", score_velocity(col("velocity_60s")))
    .withColumn("amount_score", score_amount(col("amount_usd")))
    .withColumn("country_score", score_country(col("ip_country")))
    .withColumn("fraud_score", (
        (col("velocity_score") * 0.35 +
         col("amount_score") * 0.35 +
         col("country_score") * 0.30)).cast("int"))
    .withColumn("decision",
        F.when(col("fraud_score") <= 30, "APPROVED")
         .when(col("fraud_score") <= 60, "FLAGGED")
         .otherwise("BLOCKED"))
    .withColumn("scored_time", F.current_timestamp())
)

print("Fraud scoring configured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5: Live Display
# MAGIC
# MAGIC `display()` with a `realTime` trigger shows streaming results directly in the notebook,
# MAGIC updated continuously. This is supported on Databricks Runtime 17.1+.

# COMMAND ----------

display(
    scored_stream.select(
        "transaction_id", "card_id", "amount_usd", "channel", "ip_country",
        "velocity_60s", "velocity_score", "amount_score", "country_score",
        "fraud_score", "decision", "event_time", "scored_time",
    ),
    realTime="5 minutes",
    outputMode="update",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 6: Spark Structured Streaming vs Real-Time Mode — Latency Comparison
# MAGIC
# MAGIC > *"Using the same fraud detection logic and Event Hub source, Real-Time Mode reduced
# MAGIC > end-to-end latency from X ms to Y ms while maintaining comparable throughput."*
# MAGIC
# MAGIC This section runs the **exact same pipeline** twice — once with the traditional
# MAGIC `processingTime` trigger (standard Structured Streaming) and once with the `realTime`
# MAGIC trigger (RTM). By comparing `query.lastProgress` output side-by-side, you can
# MAGIC quantify the latency improvement.
# MAGIC
# MAGIC ### Demo Approach
# MAGIC 1. **Run A** — Traditional trigger: `.trigger(processingTime="1 second")`
# MAGIC 2. **Run B** — Real-Time Mode trigger: `.trigger(realTime="5 seconds")`
# MAGIC 3. Capture `query.lastProgress` from each run
# MAGIC 4. Build a comparison DataFrame
# MAGIC 5. *(Optional)* Screenshot both and place side-by-side in a PPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 — Run A: Traditional Structured Streaming (`processingTime`)
# MAGIC
# MAGIC This uses the standard micro-batch trigger. Even at 1-second intervals, latency
# MAGIC is bounded by the micro-batch scheduling overhead.

# COMMAND ----------

import time
import json

# --- Run A: Traditional processingTime trigger ---
query_traditional = (
    scored_stream.select(
        "transaction_id", "card_id", "amount_usd", "channel", "ip_country",
        "velocity_60s", "velocity_score", "amount_score", "country_score",
        "fraud_score", "decision", "event_time", "scored_time",
    )
    .writeStream
    .queryName("traditional_streaming")
    .format("memory")
    .outputMode("update")
    .trigger(processingTime="1 second")
    .start()
)

print("Traditional Structured Streaming query started (processingTime='1 second').")
print("Letting it run for 60 seconds to collect stable metrics...")
time.sleep(60)

# Capture lastProgress
traditional_progress = query_traditional.lastProgress
print("\n=== Traditional Streaming — query.lastProgress ===")
print(json.dumps(traditional_progress, indent=2, default=str))

# Stop the traditional query
query_traditional.stop()
print("\nTraditional query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 — Run B: Real-Time Mode (`realTime`)
# MAGIC
# MAGIC Same pipeline, same scoring logic — only the trigger changes. RTM processes
# MAGIC records one-at-a-time with sub-300ms latency.

# COMMAND ----------

# --- Run B: Real-Time Mode trigger ---
query_rtm = (
    scored_stream.select(
        "transaction_id", "card_id", "amount_usd", "channel", "ip_country",
        "velocity_60s", "velocity_score", "amount_score", "country_score",
        "fraud_score", "decision", "event_time", "scored_time",
    )
    .writeStream
    .queryName("rtm_streaming")
    .format("memory")
    .outputMode("update")
    .trigger(realTime="5 seconds")
    .start()
)

print("Real-Time Mode query started (realTime='5 seconds').")
print("Letting it run for 60 seconds to collect stable metrics...")
time.sleep(60)

# Capture lastProgress
rtm_progress = query_rtm.lastProgress
print("\n=== Real-Time Mode — query.lastProgress ===")
print(json.dumps(rtm_progress, indent=2, default=str))

# Stop the RTM query
query_rtm.stop()
print("\nRTM query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 — Side-by-Side Comparison
# MAGIC
# MAGIC Build a comparison DataFrame from the captured `lastProgress` metrics.
# MAGIC Key metrics to compare:
# MAGIC - **triggerExecution (ms)** — total time for one trigger cycle
# MAGIC - **inputRowsPerSecond** — throughput
# MAGIC - **processedRowsPerSecond** — processing throughput
# MAGIC - **latency (ms)** — derived from `durationMs` fields

# COMMAND ----------

from pyspark.sql import Row

def extract_metrics(progress, label):
    """Extract key latency and throughput metrics from query.lastProgress."""
    if progress is None:
        return Row(
            mode=label,
            trigger_execution_ms=None,
            input_rows_per_sec=None,
            processed_rows_per_sec=None,
            add_batch_ms=None,
            get_batch_ms=None,
            query_planning_ms=None,
            wal_commit_ms=None,
            num_input_rows=None,
        )

    durations = progress.get("durationMs", {})
    return Row(
        mode=label,
        trigger_execution_ms=durations.get("triggerExecution"),
        input_rows_per_sec=progress.get("inputRowsPerSecond"),
        processed_rows_per_sec=progress.get("processedRowsPerSecond"),
        add_batch_ms=durations.get("addBatch"),
        get_batch_ms=durations.get("getBatch"),
        query_planning_ms=durations.get("queryPlanning"),
        wal_commit_ms=durations.get("walCommit"),
        num_input_rows=progress.get("numInputRows"),
    )


metrics_rows = [
    extract_metrics(traditional_progress, "Structured Streaming (processingTime=1s)"),
    extract_metrics(rtm_progress, "Real-Time Mode (realTime=5s)"),
]

metrics_df = spark.createDataFrame(metrics_rows)

print("=== Latency Comparison: Structured Streaming vs Real-Time Mode ===")
print()
display(metrics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 — How to Read the Results
# MAGIC
# MAGIC | Metric | What It Tells You |
# MAGIC |--------|-------------------|
# MAGIC | `trigger_execution_ms` | Total time for one trigger cycle — **lower = faster** |
# MAGIC | `add_batch_ms` | Time spent processing the batch — the core latency metric |
# MAGIC | `input_rows_per_sec` | Throughput — should be comparable between modes |
# MAGIC | `processed_rows_per_sec` | Effective processing rate |
# MAGIC | `get_batch_ms` | Time to fetch data from source |
# MAGIC | `query_planning_ms` | Spark planning overhead |
# MAGIC | `wal_commit_ms` | Write-ahead log commit time |
# MAGIC
# MAGIC **Expected Result:** RTM should show significantly lower `trigger_execution_ms`
# MAGIC and `add_batch_ms` compared to traditional streaming, while `input_rows_per_sec`
# MAGIC remains comparable.
# MAGIC
# MAGIC ### Presenting to Stakeholders
# MAGIC 1. Run this notebook and screenshot the `metrics_df` table above
# MAGIC 2. Also screenshot `query.lastProgress` raw JSON from Sections 6.1 and 6.2
# MAGIC 3. Place them side-by-side in a slide with the headline:
# MAGIC    > *"Real-Time Mode reduced trigger execution latency from X ms to Y ms
# MAGIC    > (Z% improvement) while maintaining comparable throughput."*
# MAGIC 4. Fill in X, Y, Z from your actual run results

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 — Raw `lastProgress` Inspection
# MAGIC
# MAGIC For deeper analysis, inspect the full `lastProgress` dictionaries captured above.
# MAGIC Uncomment the cells below to display them.

# COMMAND ----------

# Uncomment to inspect raw progress data:
# print("=== Traditional Streaming — Full lastProgress ===")
# print(json.dumps(traditional_progress, indent=2, default=str))

# print("\n=== Real-Time Mode — Full lastProgress ===")
# print(json.dumps(rtm_progress, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 7: Cleanup

# COMMAND ----------

for q in spark.streams.active:
    print(f"Stopping: {q.name or q.id}")
    q.stop()

print("All queries stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that you've seen Real-Time Mode in action:
# MAGIC
# MAGIC 1. **Part 1** (`RTM_01_Introduction_fraud_detection`) — Full Kafka pipeline with broadcast joins, 5-signal fraud scoring, and routing to output topics
# MAGIC 2. **Part 2** (`RTM_02_Advanced_fraud_detection_ml`) — ML-powered scoring with Lakebase as an online feature store and MLflow for model management
# MAGIC 3. **Dashboard** (`app/`) — Deploy a Streamlit app for live fraud monitoring