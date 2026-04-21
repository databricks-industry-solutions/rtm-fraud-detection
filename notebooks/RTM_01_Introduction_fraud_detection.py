# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1: Real-Time Transaction Fraud Detection
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook demonstrates Databricks **Real-Time Mode** in Structured Streaming for ultra-low latency fraud detection (40-300ms end-to-end).
# MAGIC
# MAGIC ### Business Context
# MAGIC Fraud detection demands sub-second latency — a fraudulent transaction must be blocked
# MAGIC *before* it settles. Until now, meeting that bar meant licensing separate specialized
# MAGIC streaming engines outside your data platform, adding cost, operational complexity, and
# MAGIC yet another tool to maintain. Traditional micro-batch approaches on the data platform
# MAGIC simply can't deliver the latency these workloads require.
# MAGIC
# MAGIC With [Real-Time Mode](https://www.databricks.com/blog/announcing-general-availability-real-time-mode-apache-spark-structured-streaming-databricks)
# MAGIC (GA March 2026), Databricks brings **sub-300ms end-to-end latency** directly into Spark
# MAGIC Structured Streaming — same APIs, same platform, no separate engine needed.
# MAGIC
# MAGIC ### What This Demo Shows
# MAGIC - **Sub-300ms fraud detection** using RealTimeTrigger
# MAGIC - **Stateful processing** with `transformWithState` for velocity tracking
# MAGIC - **Dictionary-based enrichment** for customer/merchant lookups (no BroadcastExchange overhead)
# MAGIC - **Rule-based fraud scoring** with explainable features
# MAGIC - **End-to-end latency metrics** via StreamingQueryProgress
# MAGIC
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Databricks Runtime 18 LTS or above
# MAGIC - Dedicated (Single User) cluster with:
# MAGIC   - Photon disabled
# MAGIC   - Autoscaling disabled
# MAGIC   - Spot instances disabled
# MAGIC   - Spark config: `spark.databricks.streaming.realTimeMode.enabled true`
# MAGIC - Kafka cluster (AWS MSK, Confluent Cloud, or local Kafka)
# MAGIC
# MAGIC ### Notebook Sections
# MAGIC | # | Section | What it does |
# MAGIC |---|---------|-------------|
# MAGIC | 1 | Setup & Configuration | Cluster checks, secrets, topic creation, imports |
# MAGIC | 2 | Reference Data | Merchant + card profile lookup tables |
# MAGIC | 3 | Fraud Scoring Rules | UDFs that produce 0-100 scores per signal |
# MAGIC | 4 | Stateful Processor | `transformWithState` for velocity & geo tracking |
# MAGIC | 5 | Streaming Pipeline | Read -> Enrich -> Score -> Route -> Write |
# MAGIC | 6 | Monitoring | Latency metrics, in-memory dashboard |
# MAGIC | 7 | Data Generator | Python Kafka producer for test traffic |
# MAGIC | 8 | Demo Execution | Run baseline + inject fraud patterns |
# MAGIC | 9 | Cleanup | Stop queries, reset state |
# MAGIC | A | Appendix | Troubleshooting guide |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 1: Setup & Configuration
# MAGIC
# MAGIC This section verifies the cluster is properly configured for Real-Time Mode,
# MAGIC loads Kafka credentials from Databricks Secrets, and defines per-user topic names
# MAGIC so multiple people can demo simultaneously without conflicts.
# MAGIC
# MAGIC Before starting the streaming pipeline, we need clean Kafka topics. This cell checks
# MAGIC if the topics already exist -- if so, it deletes and recreates them to purge any stale
# MAGIC data from previous demo runs. This guarantees a clean slate.

# COMMAND ----------

# DBTITLE 1,TODO: Set your Kafka secret scope name
# --- Configure your Databricks Secret Scope here ---
# Create a scope with: databricks secrets create-scope <your-scope-name>
# Store your Kafka TLS bootstrap servers:
#   databricks secrets put-secret <your-scope-name> kafka-bootstrap-servers-tls --string-value "<brokers>"
#   databricks secrets put-secret <your-scope-name> kafka-bootstrap-servers-plaintext --string-value "<brokers>"
dbutils.widgets.text("secret_scope", "", "Kafka Secret Scope Name")

# COMMAND ----------

# DBTITLE 1,Run config (also installs kafka-python)
# MAGIC %run ./resources/00_config

# COMMAND ----------

# DBTITLE 1,Check topic status
print(f"""
Kafka Topics Ready (clean):
  Input:    {input_topic}
  Approved: {output_topic_approved}
  Flagged:  {output_topic_flagged}
  Blocked:  {output_topic_blocked}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2: Reference Data
# MAGIC
# MAGIC Small lookup tables for merchant and cardholder context, loaded from
# MAGIC the shared `resources/00_reference_data` notebook. These are collected into
# MAGIC Python dictionaries and looked up inside UDFs (no BroadcastExchange stages).
# MAGIC
# MAGIC In production, these would be Delta tables refreshed by separate ETL pipelines.

# COMMAND ----------

# DBTITLE 1,Load shared reference data
# MAGIC %run ./resources/00_reference_data

# COMMAND ----------

# DBTITLE 1,Display reference data
display(merchant_data)

# COMMAND ----------

display(card_profile_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3: Fraud Scoring Rules (UDFs)
# MAGIC
# MAGIC Each UDF below produces a score from **0** (safe) to **100** (very suspicious).
# MAGIC The final fraud score is a weighted average of all individual scores.
# MAGIC
# MAGIC | Signal | Weight | What it catches |
# MAGIC |--------|--------|----------------|
# MAGIC | Velocity | 25% | Rapid-fire transactions (card testing) |
# MAGIC | Geo | 30% | Impossible travel (NYC then London in 60s) |
# MAGIC | Amount | 25% | Spending far above normal pattern |
# MAGIC | Category | 10% | High-risk merchant types (gift cards, jewelry) |
# MAGIC | Country | 10% | Transaction from high-risk country |
# MAGIC
# MAGIC **Decision thresholds:**
# MAGIC - **0-30:** APPROVED (green) -- transaction goes through
# MAGIC - **31-60:** FLAGGED (yellow) -- routed to manual review
# MAGIC - **61-100:** BLOCKED (red) -- automatically declined

# COMMAND ----------

import time
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, current_timestamp, unix_timestamp, to_json, struct
from pyspark.sql.types import *

@udf(returnType=IntegerType())
def velocity_score(count):
    """Velocity score: more transactions in 60s window = higher score."""
    if count is None or count <= 1:
        return 0
    elif count == 2:
        return 20
    elif count == 3:
        return 40
    elif count == 4:
        return 60
    else:
        return 85

@udf(returnType=IntegerType())
def geo_score(distance, time_diff_seconds):
    """Geo score: calculates implied travel speed; flags impossible travel."""
    if time_diff_seconds is None or time_diff_seconds == 0 or distance is None or distance == 0.0:
        return 0
    speed_kmh = (distance / time_diff_seconds) * 3600
    if speed_kmh > 900:
        return 100  # Faster than commercial flight (~900 km/h)
    elif speed_kmh > 500:
        return 85
    elif speed_kmh > 200:
        return 50
    else:
        return 0

@udf(returnType=IntegerType())
def amount_score(amount, avg_amount):
    """Amount score: ratio of transaction amount to cardholder average."""
    if amount is None or avg_amount is None or avg_amount == 0:
        return 0
    ratio = amount / avg_amount
    if ratio > 20:
        return 100
    elif ratio > 10:
        return 80
    elif ratio > 5:
        return 60
    elif ratio > 3:
        return 40
    elif ratio > 2:
        return 20
    else:
        return 0

@udf(returnType=IntegerType())
def category_risk_score(risk_tier):
    """Category risk score: based on merchant risk_tier."""
    mapping = {"VERY_HIGH": 80, "HIGH": 50, "MEDIUM": 20, "LOW": 0}
    return mapping.get(risk_tier, 10)

@udf(returnType=IntegerType())
def country_risk_score(ip_country, home_country, merchant_country):
    """Country risk score: compares IP country vs cardholder home country."""
    high_risk = {"RO", "NG", "CN", "RU", "UA"}
    if ip_country != home_country and ip_country in high_risk:
        return 70
    elif ip_country != home_country:
        return 30
    elif merchant_country in high_risk:
        return 40
    else:
        return 0

@udf(returnType=IntegerType())
def calculate_fraud_score(velocity, geo, amount, category, country):
    """Weighted average of all signal scores."""
    return int(velocity * 0.25 + geo * 0.30 + amount * 0.25 + category * 0.10 + country * 0.10)

@udf(returnType=StringType())
def get_decision(score):
    """Map score to decision."""
    if score is None:
        return "APPROVED"
    elif score <= 30:
        return "APPROVED"
    elif score <= 60:
        return "FLAGGED"
    else:
        return "BLOCKED"

print("Fraud scoring UDFs registered.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4: Stateful Velocity Processor
# MAGIC
# MAGIC This is the most advanced part of the pipeline. `transformWithState` lets us keep
# MAGIC **per-key state** across streaming micro-batches. For each `card_id`, we track:
# MAGIC
# MAGIC - **Transaction count** in the last 60 seconds (velocity)
# MAGIC - **Last known GPS coordinates** (for geo-impossibility detection)
# MAGIC - **Last transaction timestamp** (for time-between-transactions)
# MAGIC
# MAGIC State automatically expires after 60 seconds of inactivity via TTL, so we
# MAGIC don't accumulate stale state for cards that stop transacting.
# MAGIC
# MAGIC We use the Python **row-based** `transformWithState` API (supported in Real-Time Mode
# MAGIC on Databricks Runtime 16.3+). See
# MAGIC [Real-time mode examples](https://docs.databricks.com/aws/en/structured-streaming/real-time-examples)
# MAGIC for the official reference.

# COMMAND ----------

import math
from typing import Iterator
from pyspark.sql import Row
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle

# State schema: per-card velocity tracking
velocity_state_schema = StructType([
    StructField("count", LongType()),
    StructField("last_latitude", DoubleType()),
    StructField("last_longitude", DoubleType()),
    StructField("last_timestamp", TimestampType()),
])

# Output schema: transaction enriched with velocity + geo features
enriched_output_schema = StructType([
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
    StructField("event_time", TimestampType()),
    StructField("velocity_60s", LongType()),
    StructField("last_latitude", DoubleType()),
    StructField("last_longitude", DoubleType()),
    StructField("last_event_time", TimestampType()),
    StructField("geo_distance_km", DoubleType()),
    StructField("time_diff_seconds", LongType()),
])


def _haversine_km(lat1, lon1, lat2, lon2):
    """Haversine formula: distance in km between two GPS coordinates."""
    R = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (math.sin(d_lat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(d_lon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class VelocityProcessor(StatefulProcessor):
    """
    Per-card stateful processor that tracks transaction velocity and geo movement.

    In Real-Time Mode, handleInputRows is called once per row (the iterator
    yields a single value). State auto-expires after 60s of inactivity via TTL.
    """

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.velocity_state = handle.getValueState(
            "velocity", velocity_state_schema, 60000  # 60s TTL in milliseconds
        )

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        for row in rows:
            state = self.velocity_state.get()

            if state is None:
                count = 0
                last_lat = row["latitude"]
                last_lon = row["longitude"]
                last_ts = row["event_time"]
            else:
                count = state[0]
                last_lat = state[1]
                last_lon = state[2]
                last_ts = state[3]

            if count > 0:
                geo_distance_km = _haversine_km(last_lat, last_lon, row["latitude"], row["longitude"])
                time_diff_seconds = int((row["event_time"].timestamp() - last_ts.timestamp()))
            else:
                geo_distance_km = 0.0
                time_diff_seconds = 0

            new_count = count + 1
            self.velocity_state.update((new_count, row["latitude"], row["longitude"], row["event_time"]))

            yield Row(
                transaction_id=row["transaction_id"],
                card_id=row["card_id"],
                merchant_id=row["merchant_id"],
                merchant_category=row["merchant_category"],
                amount_usd=row["amount_usd"],
                currency=row["currency"],
                channel=row["channel"],
                ip_country=row["ip_country"],
                device_fingerprint=row["device_fingerprint"],
                latitude=row["latitude"],
                longitude=row["longitude"],
                event_time=row["event_time"],
                velocity_60s=new_count,
                last_latitude=last_lat,
                last_longitude=last_lon,
                last_event_time=last_ts,
                geo_distance_km=geo_distance_km,
                time_diff_seconds=time_diff_seconds,
            )

    def close(self) -> None:
        pass


print("VelocityProcessor defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5: Streaming Pipeline
# MAGIC
# MAGIC This section wires everything together into the end-to-end real-time pipeline:
# MAGIC 1. **Read** raw transaction JSON from Kafka
# MAGIC 2. **Track velocity** via stateful processing
# MAGIC 3. **Enrich** with merchant and card profile data via dictionary lookups
# MAGIC 4. **Score** each transaction using the fraud UDFs
# MAGIC 5. **Route** to output Kafka topics based on the decision

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Read from Kafka & Parse JSON

# COMMAND ----------

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
    .option("subscribe", input_topic)
    .option("maxPartitions", kafka_max_partitions)
    .option("startingOffsets", "latest")
    .option("kafka.security.protocol", "SSL")
    .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(F.from_json(col("json_value"), transaction_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", F.to_timestamp(col("event_time")))
)

print(f"Kafka source configured: topic={input_topic}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Stateful Velocity Tracking

# COMMAND ----------

velocity_enriched_stream = (
    parsed_stream
    .groupBy("card_id")
    .transformWithState(
        statefulProcessor=VelocityProcessor(),
        outputStructType=enriched_output_schema,
        outputMode="Update",
        timeMode="processingTime",
    )
)

print("Velocity tracking applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Enrich with Merchant & Card Data (Dictionary Lookups)
# MAGIC
# MAGIC Instead of broadcast joins (which add BroadcastExchange stages to the plan and
# MAGIC consume additional RTM slots), we collect the small reference tables into Python
# MAGIC dictionaries and look them up inside UDFs. For tables this small (10 merchants,
# MAGIC 5 cards), dict lookups are faster and keep the physical plan to exactly
# MAGIC `maxPartitions + shuffle_partitions` stages.

# COMMAND ----------

_merchant_lookup = {
    row["merchant_id"]: row.asDict()
    for row in spark.table("merchants").collect()
}
_card_lookup = {
    row["card_id"]: row.asDict()
    for row in spark.table("card_profiles").collect()
}

@udf(returnType=StructType([
    StructField("merchant_name", StringType()),
    StructField("merchant_risk_tier", StringType()),
    StructField("merchant_country", StringType()),
]))
def enrich_merchant(merchant_id):
    m = _merchant_lookup.get(merchant_id, {})
    return (
        m.get("merchant_name", "UNKNOWN"),
        m.get("risk_tier", "MEDIUM"),
        m.get("merchant_country", "US"),
    )

@udf(returnType=StructType([
    StructField("home_country", StringType()),
    StructField("avg_txn_amount", DoubleType()),
    StructField("max_txn_30d", DoubleType()),
    StructField("card_risk_segment", StringType()),
]))
def enrich_card(card_id):
    c = _card_lookup.get(card_id, {})
    return (
        c.get("home_country", "US"),
        c.get("avg_txn_amount", 100.0),
        c.get("max_txn_30d", 500.0),
        c.get("risk_segment", "LOW"),
    )

fully_enriched_stream = (
    velocity_enriched_stream
    .withColumn("_m", enrich_merchant(col("merchant_id")))
    .withColumn("merchant_name", col("_m.merchant_name"))
    .withColumn("merchant_risk_tier", col("_m.merchant_risk_tier"))
    .withColumn("merchant_country", col("_m.merchant_country"))
    .drop("_m")
    .withColumn("_c", enrich_card(col("card_id")))
    .withColumn("home_country", col("_c.home_country"))
    .withColumn("avg_txn_amount", col("_c.avg_txn_amount"))
    .withColumn("max_txn_30d", col("_c.max_txn_30d"))
    .withColumn("card_risk_segment", col("_c.card_risk_segment"))
    .drop("_c")
)

print("Reference data enrichment configured (dict lookups — no broadcast exchange stages).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Calculate Fraud Score & Decision

# COMMAND ----------

scored_stream = (
    fully_enriched_stream
    .withColumn("velocity_score", velocity_score(col("velocity_60s")))
    .withColumn("geo_score", geo_score(col("geo_distance_km"), col("time_diff_seconds")))
    .withColumn("amount_score", amount_score(col("amount_usd"), col("avg_txn_amount")))
    .withColumn("category_score", category_risk_score(col("merchant_risk_tier")))
    .withColumn("country_score", country_risk_score(col("ip_country"), col("home_country"), col("merchant_country")))
    .withColumn("fraud_score", calculate_fraud_score(
        col("velocity_score"), col("geo_score"), col("amount_score"),
        col("category_score"), col("country_score"),
    ))
    .withColumn("decision", get_decision(col("fraud_score")))
    .withColumn("scored_time", current_timestamp())
    .withColumn("processing_latency_ms",
        (unix_timestamp(col("scored_time")) - unix_timestamp(col("event_time"))) * 1000
    )
)

output_stream = scored_stream.select(
    col("transaction_id"), col("card_id"), col("merchant_id"),
    col("merchant_name"), col("merchant_category"), col("amount_usd"),
    col("channel"), col("ip_country"), col("home_country"),
    col("velocity_60s"), col("geo_distance_km"), col("time_diff_seconds"),
    col("velocity_score"), col("geo_score"), col("amount_score"),
    col("category_score"), col("country_score"),
    col("fraud_score"), col("decision"),
    col("event_time"), col("scored_time"), col("processing_latency_ms"),
)

print("Fraud scoring pipeline configured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 Write to Kafka Output Topics
# MAGIC
# MAGIC Each decision category gets its own streaming query writing to a dedicated Kafka topic.
# MAGIC This gives each stream an independent checkpoint (fault isolation) and uses the native
# MAGIC Spark Kafka connector (exactly-once write semantics).
# MAGIC
# MAGIC The `realTime="30 seconds"` parameter sets the checkpoint interval — data is processed
# MAGIC continuously with sub-300ms latency, while checkpoints are committed every 30 seconds
# MAGIC for durability.
# MAGIC
# MAGIC **Slot budget:** Each query holds `maxPartitions + shuffle_partitions` slots continuously.
# MAGIC With 3 queries that's `3 × (maxPartitions + shuffle_partitions)`. Tune `kafka_max_partitions`
# MAGIC and `spark.sql.shuffle.partitions` in `00_config` to fit your cluster.

# COMMAND ----------

# DBTITLE 1,Stop existing queries & clear stale checkpoints
for q in spark.streams.active:
    print(f"Stopping existing query: {q.name} ({q.id})")
    q.stop()

dbutils.fs.rm(checkpoint_location, True)
print(f"Cleared checkpoint directory: {checkpoint_location}")

# COMMAND ----------

def write_decision_to_kafka(stream, decision, topic, checkpoint):
    """Write a single decision category to its own Kafka topic."""
    return (
        stream
        .filter(col("decision") == decision)
        .select(
            col("transaction_id").cast("string").alias("key"),
            to_json(struct("*")).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("kafka.security.protocol", "SSL")
        .option("topic", topic)
        .option("checkpointLocation", checkpoint)
        .outputMode("update")
        .trigger(realTime="30 seconds")
        .start()
    )

approved_query = write_decision_to_kafka(output_stream, "APPROVED", output_topic_approved, f"{checkpoint_location}/approved")
flagged_query  = write_decision_to_kafka(output_stream, "FLAGGED",  output_topic_flagged,  f"{checkpoint_location}/flagged")
blocked_query  = write_decision_to_kafka(output_stream, "BLOCKED",  output_topic_blocked,  f"{checkpoint_location}/blocked")

slots_per_query = kafka_max_partitions + shuffle_partitions
print(f"""
Streaming queries started!
  APPROVED -> {output_topic_approved}
  FLAGGED  -> {output_topic_flagged}
  BLOCKED  -> {output_topic_blocked}

Slot usage: 3 queries × ({kafka_max_partitions} source + {shuffle_partitions} shuffle) = {3 * slots_per_query} slots
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 6: Monitoring & Observability
# MAGIC
# MAGIC Monitor query latency, throughput, and inspect live results.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Query Progress Metrics

# COMMAND ----------

def display_query_metrics(query):
    """Display streaming query progress including RTM latency metrics."""
    progress = query.lastProgress
    if progress is not None:
        print(f"Query: {query.name}")
        print(f"  Batch ID:        {progress.get('batchId')}")
        print(f"  Input Rows:      {progress.get('numInputRows')}")
        print(f"  Processing Rate: {progress.get('processedRowsPerSecond')} rows/sec")

        duration_ms = progress.get("durationMs", {})
        print(f"  Trigger Time:    {duration_ms.get('triggerExecution')} ms")

        rtm = progress.get("rtmMetrics")
        if rtm:
            proc = rtm.get("processingLatencyMs", {})
            e2e = rtm.get("e2eLatencyMs", {})
            print(f"  Processing Latency P50: {proc.get('P50')} ms")
            print(f"  Processing Latency P99: {proc.get('P99')} ms")
            print(f"  E2E Latency P50:        {e2e.get('P50')} ms")
            print(f"  E2E Latency P99:        {e2e.get('P99')} ms")
        else:
            print("  Real-Time Metrics: N/A (waiting for data)")
    else:
        print(f"No progress yet for query: {query.name}")
    print()

display_query_metrics(approved_query)
display_query_metrics(flagged_query)
display_query_metrics(blocked_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 In-Memory Monitoring Table

# COMMAND ----------

monitoring_query = (
    output_stream
    .writeStream
    .format("memory")
    .queryName("fraud_monitoring")
    .outputMode("update")
    .trigger(processingTime="5 minutes")
    .start()
)

display(spark.sql("SELECT * FROM fraud_monitoring ORDER BY scored_time DESC LIMIT 100"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Active Query Status

# COMMAND ----------

for query in spark.streams.active:
    print(f"Query: {query.name}, ID: {query.id}, Status: {query.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 7: Data Generator (Python)
# MAGIC
# MAGIC This Python cell defines functions to produce test transactions into Kafka.
# MAGIC It generates three types of traffic:
# MAGIC
# MAGIC - **Normal transactions:** small amounts, US locations, common merchants
# MAGIC - **Fraud patterns:**
# MAGIC   - `velocity` -- 5 rapid small purchases + 1 big purchase (card testing)
# MAGIC   - `geo` -- transaction in NYC, then London 60s later (impossible travel)
# MAGIC   - `amount` -- a $3000 jewelry purchase from a high-risk country (spending spike)

# COMMAND ----------

# MAGIC %run ./resources/00_datagenerator

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 8: Demo Execution
# MAGIC
# MAGIC Run these cells top-to-bottom during the live demo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Generate Baseline Traffic (normal transactions)

# COMMAND ----------

run_baseline_generator(duration_seconds=60, tps=5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Check Metrics After Baseline

# COMMAND ----------

time.sleep(15)
display_query_metrics(approved_query)
display_query_metrics(flagged_query)
display_query_metrics(blocked_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Inject Fraud Pattern 1: Velocity Burst
# MAGIC
# MAGIC **What happens:** 5 rapid small purchases ($0.99-$5) from a high-risk country,
# MAGIC followed by a large purchase ($800-$2000). The velocity counter increments each time.
# MAGIC By the 5th transaction, velocity_score = 85. The final big purchase should be BLOCKED.

# COMMAND ----------

inject_fraud_pattern("velocity")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.4 Inject Fraud Pattern 2: Impossible Travel
# MAGIC
# MAGIC **What happens:** A card swipes at a restaurant in New York, then 60 seconds later
# MAGIC makes an online purchase in London (5,570 km away). The implied travel speed is
# MAGIC ~334,000 km/h -- clearly impossible. geo_score = 100.

# COMMAND ----------

time.sleep(10)
inject_fraud_pattern("geo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.5 Inject Fraud Pattern 3: Amount Spike
# MAGIC
# MAGIC **What happens:** A card that normally spends $50-$65 suddenly makes a $3000+
# MAGIC jewelry purchase from Romania. The amount ratio is 40-60x the average.
# MAGIC amount_score = 100, country_score = 70.

# COMMAND ----------

time.sleep(10)
inject_fraud_pattern("amount")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.6 Inspect Results

# COMMAND ----------

display(spark.sql("""
    SELECT decision, COUNT(*) as count, ROUND(AVG(fraud_score), 1) as avg_score
    FROM fraud_monitoring
    GROUP BY decision
    ORDER BY decision
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT transaction_id, card_id, amount_usd, fraud_score, decision,
           velocity_score, geo_score, amount_score, country_score
    FROM fraud_monitoring
    ORDER BY scored_time DESC
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.7 Verify Kafka Output Topics

# COMMAND ----------

from kafka import KafkaConsumer

ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=kafka_brokers,
    security_protocol="SSL",
    ssl_context=ssl_ctx,
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000
)

count = 0
for msg in consumer:
    count += 1
    if count >= 3:
        print(f"Sample message: {msg.value.decode('utf-8')[:200]}")
        break

print(f"Found {count}+ messages in Kafka topic: {input_topic}")
consumer.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 9: Cleanup
# MAGIC
# MAGIC Stop all streaming queries and uncache reference tables.
# MAGIC **Run this before re-running the notebook** to avoid stale checkpoints.

# COMMAND ----------

for q in spark.streams.active:
    print(f"Stopping query: {q.name} ({q.id})")
    q.stop()

spark.catalog.uncacheTable("merchants")
spark.catalog.uncacheTable("card_profiles")

print("All queries stopped and tables uncached.")

# COMMAND ----------

dbutils.fs.rm(checkpoint_location, True)
print("Checkpoints cleared.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## End of Notebook
# MAGIC
# MAGIC **References:**
# MAGIC - [Databricks Real-Time Mode Documentation](https://docs.databricks.com/aws/en/structured-streaming/real-time.html)
# MAGIC - [Real-Time Mode Examples](https://docs.databricks.com/aws/en/structured-streaming/real-time-examples)
# MAGIC - [Stateful Applications with transformWithState](https://docs.databricks.com/aws/en/stateful-applications/)
# MAGIC - [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)