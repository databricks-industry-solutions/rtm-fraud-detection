# Databricks notebook source
# DBTITLE 1,Install dependencies
# MAGIC %pip install kafka-python -q

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 1: Setup & Configuration
# MAGIC
# MAGIC This section verifies the cluster is properly configured for Real-Time Mode,
# MAGIC loads Kafka credentials from Databricks Secrets, and defines per-user topic names
# MAGIC so multiple people can demo simultaneously without conflicts.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Verify Real-Time Mode

# COMMAND ----------

realtime_mode_enabled = spark.conf.get("spark.databricks.streaming.realTimeMode.enabled", "false")

print(f"Real-time mode enabled: {realtime_mode_enabled} (should be true)")

if realtime_mode_enabled != "true":
    print("""
    WARNING: Real-time mode is not enabled!

    To enable it, add this to your cluster's Spark config:
    spark.databricks.streaming.realTimeMode.enabled true

    Also ensure:
    - Photon is disabled
    - Autoscaling is disabled
    - Spot instances are disabled
    - Access mode is set to Dedicated (formerly Single User)
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Kafka Credentials & Topic Configuration

# COMMAND ----------

import pyspark.sql.functions as F
import ssl, time

# --- Kafka credentials from Databricks Secrets (uses widget values from parent notebook) ---
secret_scope = dbutils.widgets.get("secret_scope")
if not secret_scope:
    raise ValueError(
        "Widget 'secret_scope' is empty. Please set your Databricks Secret Scope name "
        "in the widget at the top of this notebook. See README for setup instructions."
    )

kafka_bootstrap_servers_tls = dbutils.secrets.get(secret_scope, "kafka-bootstrap-servers-tls")
kafka_bootstrap_servers_plaintext = dbutils.secrets.get(secret_scope, "kafka-bootstrap-servers-plaintext")

use_tls = True
kafka_brokers = kafka_bootstrap_servers_tls if use_tls else kafka_bootstrap_servers_plaintext

if use_tls and "9094" not in kafka_brokers:
    print("WARNING: Using TLS but port is not 9094. Check your bootstrap server address!")

# --- Per-user topic names (enables concurrent demos) ---
username = spark.sql("SELECT current_user()").collect()[0][0]
user = username.split("@")[0].replace(".", "_")

input_topic = f"{user}_raw_transactions"
output_topic_approved = f"{user}_approved_txns"
output_topic_flagged = f"{user}_flagged_txns"
output_topic_blocked = f"{user}_blocked_txns"

# --- Checkpoint & project paths ---
project_dir = f"/home/{username}/fraud_detection_demo"
checkpoint_location = f"{project_dir}/checkpoints"

# --- Cluster sizing for Real-Time Mode ---
# In RTM, all stages run simultaneously, so total slots needed =
#   num_queries × (source_partitions + shuffle_partitions)
#
# Notebook 1 starts 3 RTM write queries + 1 micro-batch monitor.
# With kafka_max_partitions=2 and shuffle_partitions=4:
#   3 RTM queries × (2 + 4) = 18 slots (held continuously)
#   1 micro-batch monitor   ≈  6 slots (intermittent)
#   Peak                    = 24 slots
#
# Adjust these if your cluster has more/fewer cores:
#   Total executor cores = workers × cores_per_worker
kafka_max_partitions = 2
spark.conf.set("spark.sql.shuffle.partitions", "4")

# For lowest latency with Python UDFs in Real-Time Mode
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1")

# --- Share config with other notebooks (data generator, Notebook 2) via spark.conf ---
spark.conf.set("demo.kafka.brokers.tls", kafka_bootstrap_servers_tls)
spark.conf.set("demo.kafka.brokers.plaintext", kafka_bootstrap_servers_plaintext)
spark.conf.set("demo.kafka.brokers", kafka_brokers)
spark.conf.set("demo.kafka.useTLS", str(use_tls))
spark.conf.set("demo.topic.input", input_topic)
spark.conf.set("demo.topic.approved", output_topic_approved)
spark.conf.set("demo.topic.flagged", output_topic_flagged)
spark.conf.set("demo.topic.blocked", output_topic_blocked)
spark.conf.set("demo.checkpoint.location", checkpoint_location)

shuffle_partitions = int(spark.conf.get("spark.sql.shuffle.partitions"))
slots_per_query = kafka_max_partitions + shuffle_partitions

print(f"""
Configuration Summary
=====================
Kafka Brokers:      {kafka_brokers}
TLS Enabled:        {use_tls}

Topics:
  Input:    {input_topic}
  Approved: {output_topic_approved}
  Flagged:  {output_topic_flagged}
  Blocked:  {output_topic_blocked}

Checkpoint:         {checkpoint_location}

RTM Slot Calculation:
  maxPartitions (source):   {kafka_max_partitions}
  shuffle.partitions:       {shuffle_partitions}
  Slots per RTM query:      {kafka_max_partitions} + {shuffle_partitions} = {slots_per_query}
  3 RTM write queries:      3 × {slots_per_query} = {3 * slots_per_query} slots
  Ensure total executor cores >= {3 * slots_per_query} (workers × cores_per_worker)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2: Create Kafka Topics
# MAGIC
# MAGIC Before starting the streaming pipeline, we need clean Kafka topics.
# MAGIC This cell checks if the topics already exist -- if so, it **deletes and recreates**
# MAGIC them to purge any stale data from previous demo runs. This guarantees a clean slate.

# COMMAND ----------

from kafka.admin import KafkaAdminClient, NewTopic

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_bootstrap_servers_tls,
    security_protocol="SSL",
    ssl_context=ssl_context
)

TOPIC_PARTITIONS = 8
REPLICATION_FACTOR = 2
all_topics = [input_topic, output_topic_approved, output_topic_flagged, output_topic_blocked]

existing_topics = set(admin_client.list_topics())
topics_existing = [t for t in all_topics if t in existing_topics]

if topics_existing:
    print(f"Deleting existing topics to purge old data: {topics_existing}")
    admin_client.delete_topics(topics_existing)
    print("Waiting for topic deletion to propagate...")
    time.sleep(5)

topics_to_create = [
    NewTopic(name=t, num_partitions=TOPIC_PARTITIONS, replication_factor=REPLICATION_FACTOR)
    for t in all_topics
]

for attempt in range(3):
    try:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        print("All topics created successfully!")
        break
    except Exception as e:
        if attempt < 2 and "TopicAlreadyExists" in str(e):
            print(f"Topics still deleting, retrying in 5s... (attempt {attempt + 1}/3)")
            time.sleep(5)
        else:
            raise e

admin_client.close()

print(f"""
Kafka Topics Ready (clean):
  Input:    {input_topic}
  Approved: {output_topic_approved}
  Flagged:  {output_topic_flagged}
  Blocked:  {output_topic_blocked}
""")
