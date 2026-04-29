# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Mode Solutions Accelerator: Fraud Detection
# MAGIC
# MAGIC ## The Problem
# MAGIC
# MAGIC Fraud detection demands sub-second latency — a fraudulent transaction must be blocked
# MAGIC *before* it settles. Historically this has meant licensing **separate specialized streaming
# MAGIC engines** outside the data platform, adding cost, operational complexity, and yet another
# MAGIC tool to integrate and maintain. Traditional micro-batch processing on the data platform
# MAGIC can't meet the latency these workloads require, forcing teams to operate two parallel
# MAGIC stacks.
# MAGIC
# MAGIC ## The Solution
# MAGIC
# MAGIC With [**Real-Time Mode**](https://www.databricks.com/blog/announcing-general-availability-real-time-mode-apache-spark-structured-streaming-databricks)
# MAGIC (GA March 2026), Databricks brings **sub-300ms end-to-end latency** directly into Spark
# MAGIC Structured Streaming — same APIs, same platform, no separate engine needed. Combined
# MAGIC with stateful processing (`transformWithState`), ML scoring (MLflow), and an online feature
# MAGIC store (Lakebase), this accelerator provides a production-ready foundation for real-time
# MAGIC fraud detection on the Databricks Data Intelligence Platform.
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC See the architecture diagram in the [README](../README.md) (`images/Image1.png`)
# MAGIC for the visual flow. Text summary:
# MAGIC
# MAGIC ```
# MAGIC Kafka  ──events──>  Spark Real-Time Mode  ──processed──>  Kafka / Lakebase  ──served──>  Databricks App
# MAGIC                     (Parse → Velocity →
# MAGIC                      Enrich → Score → Route)
# MAGIC ```
# MAGIC
# MAGIC The RTM pipeline runs five stages continuously: parse Kafka JSON, track
# MAGIC per-card velocity with `transformWithState`, enrich with merchant/card
# MAGIC profile data via in-memory dictionary lookups (no `BroadcastExchange` overhead),
# MAGIC score with weighted multi-signal UDFs, and route by decision into separate
# MAGIC output Kafka topics. Lakebase holds the online feature store, MLflow tracks
# MAGIC the model, and the Databricks App provides the live monitoring dashboard.
# MAGIC
# MAGIC ## What's Included
# MAGIC
# MAGIC - **Quick Start** (`RTM_00_Quick_Start`) — Self-contained RTM demo with zero external dependencies.
# MAGIC   Uses a `rate` source for synthetic data and `display()` for output. Run this first to verify
# MAGIC   your cluster is configured correctly.
# MAGIC
# MAGIC - **Part 1: Rule-Based Detection** (`RTM_01_Introduction_fraud_detection`) — End-to-end Kafka
# MAGIC   pipeline with stateful velocity tracking (`transformWithState`), dictionary-based enrichment,
# MAGIC   weighted multi-signal fraud scoring, and routing to output Kafka topics — all at sub-300ms latency.
# MAGIC   *Requires: Kafka (AWS MSK or Confluent Cloud)*
# MAGIC
# MAGIC - **Part 2: ML-Powered Detection** (`RTM_02_Advanced_fraud_detection_ml`) — Upgrades from rules
# MAGIC   to ML: streams features to Lakebase (online feature store) using the public `foreach` sink and
# MAGIC   a custom `LakebaseFeatureWriter` (see `resources/00_lakebase_writer`), trains a RandomForest
# MAGIC   model tracked in MLflow, and scores transactions with the model loaded as a Spark UDF.
# MAGIC   *Requires: Kafka + Lakebase instance*
# MAGIC
# MAGIC - **Dashboard App** (`app/`) — Streamlit-based Databricks App that provides a live fraud detection
# MAGIC   dashboard reading from Lakebase — total transactions, decision breakdown, recent scores, and
# MAGIC   fraud probability distribution. *Requires: Lakebase (populated by Part 2)*
# MAGIC
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC ### Cluster Configuration
# MAGIC - **Databricks Runtime 18 LTS** or above
# MAGIC - **Dedicated** (Single User) access mode
# MAGIC - **Photon disabled**
# MAGIC - **Autoscaling disabled**
# MAGIC - Spark config: `spark.databricks.streaming.realTimeMode.enabled true`
# MAGIC
# MAGIC ### External Services (for Parts 1 & 2)
# MAGIC - **Kafka cluster** (AWS MSK, Confluent Cloud, or self-managed) with TLS on port 9094
# MAGIC - **Databricks Secret Scope** with your Kafka bootstrap servers (see README for setup)
# MAGIC - **Lakebase instance** (for Part 2 and Dashboard App)
# MAGIC
# MAGIC ## Getting Started
# MAGIC
# MAGIC 1. **Verify your cluster** — Create a cluster with the prerequisites above
# MAGIC 2. **Run Quick Start** — Open `RTM_00_Quick_Start` and run all cells. This validates RTM works on your cluster with zero external dependencies.
# MAGIC 3. **Run Part 1** — Open `RTM_01_Introduction_fraud_detection` for the full Kafka-based pipeline with rule-based scoring
# MAGIC 4. **Run Part 2** — Open `RTM_02_Advanced_fraud_detection_ml` for ML-powered scoring with Lakebase and MLflow
# MAGIC 5. **Deploy Dashboard** — Deploy the Streamlit app from `apps/` for a live fraud monitoring dashboard
# MAGIC
# MAGIC ## Dashboard App Setup
# MAGIC
# MAGIC The dashboard app (`apps/`) connects to Lakebase to display live fraud scores.
# MAGIC To deploy it:
# MAGIC
# MAGIC 1. **Create a Databricks App** in the workspace UI (Compute > Apps > Create App)
# MAGIC 2. **Add a Lakebase resource** to the app:
# MAGIC    - Resource name: `lakebase-db` (or any name)
# MAGIC    - Instance name: **your Lakebase instance name** (the same one used in Part 2's `lakebase_instance` widget)
# MAGIC    - Database name: `databricks_postgres`
# MAGIC    - Permission: `CAN_CONNECT_AND_CREATE`
# MAGIC 3. **Deploy** the app with source code path pointing to the `apps/` folder
# MAGIC 4. The app reads from tables `fraud_scores` and `card_features` — these are created
# MAGIC    automatically when you run Part 2 (`RTM_02`)
# MAGIC
# MAGIC > **Important:** The Lakebase resource binding automatically injects `PG*` environment
# MAGIC > variables (`PGHOST`, `PGUSER`, etc.) into the app. You do **not** need to configure
# MAGIC > connection strings manually.
# MAGIC
# MAGIC ## References
# MAGIC
# MAGIC - [Real-Time Mode Documentation](https://docs.databricks.com/aws/en/structured-streaming/real-time.html)
# MAGIC - [Real-Time Mode Examples](https://docs.databricks.com/aws/en/structured-streaming/real-time-examples)
# MAGIC - [Stateful Applications with transformWithState](https://docs.databricks.com/aws/en/stateful-applications/)
# MAGIC - [Lakebase Documentation](https://docs.databricks.com/en/database/lakebase.html)
# MAGIC - [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)