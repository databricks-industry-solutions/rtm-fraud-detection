# Databricks Real-Time Mode Solution Accelerator: Fraud Detection

[![Databricks](https://img.shields.io/badge/Databricks-Solution_Accelerator-FF3621?style=for-the-badge&logo=databricks)](https://databricks.com)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00A1C9?style=for-the-badge)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
[![Real-Time Mode](https://img.shields.io/badge/Real--Time_Mode-Sub_300ms-00C851?style=for-the-badge)](https://docs.databricks.com/aws/en/structured-streaming/real-time.html)

---

## Overview

This repository contains an end-to-end implementation of **real-time fraud detection** using Databricks **Real-Time Mode** in Apache Spark Structured Streaming. It demonstrates how to detect and block fraudulent transactions in **sub-300ms** — directly on the Databricks Data Intelligence Platform, with no separate streaming engine required.

Fraud detection demands sub-second latency — a fraudulent transaction must be blocked *before* it settles. Historically this has meant licensing separate specialized streaming engines outside the data platform, adding cost, operational complexity, and yet another tool to integrate and maintain. Traditional micro-batch processing simply can't meet the latency these workloads require, forcing teams to operate two parallel stacks.

With [**Real-Time Mode**](https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming), Databricks brings **sub-300ms end-to-end latency** directly into Spark Structured Streaming — same APIs, same platform, no separate engine needed.

### What This Solution Does

This accelerator shows you how to:
- **Detect fraud in sub-300ms** using Real-Time Mode's `trigger(realTime=...)` with continuous processing
- **Track per-card velocity** using `transformWithState` for stateful processing with TTL-based expiration
- **Enrich transactions** with merchant and card profile data via dictionary-based lookups (no BroadcastExchange overhead)
- **Score transactions with ML** using a RandomForest model trained with MLflow, served as a Spark UDF
- **Stream features to Lakebase** (managed PostgreSQL) as an online feature store for sub-millisecond reads
- **Monitor fraud in real time** with a Streamlit-based Databricks App dashboard

While this example focuses on financial fraud, the architecture and approach are **reusable for many other low-latency streaming scenarios** including:
- Real-time bidding and ad fraud detection
- IoT anomaly detection
- Network intrusion detection
- E-commerce session scoring
- Supply chain event processing


The solution leverages several Databricks technologies:

1. **Real-Time Mode** — Sub-300ms streaming with `RealTimeTrigger` for continuous processing
2. **transformWithState** — Per-key stateful processing with TTL for velocity tracking
3. **Lakebase** — Managed PostgreSQL as an online feature store via `jdbcStreaming`
4. **MLflow** — Model training, versioning, and serving as a Spark UDF
5. **Databricks Apps** — Streamlit-based live fraud monitoring dashboard
6. **Delta Lake & Unity Catalog** — Unified data storage and governance

## Quick Start

### Installation with Databricks Asset Bundles

The fastest way to deploy this solution is using Databricks Asset Bundles, which automates the entire setup process.

#### Prerequisites

- Databricks workspace with **classic compute** support
- Databricks CLI installed and configured ([installation guide](https://docs.databricks.com/en/dev-tools/cli/install))
- Unity Catalog enabled in your workspace

#### Cluster Requirements

> **Important:** Real-Time Mode requires a specific cluster configuration. The Asset Bundle handles this automatically, but if running manually, ensure your cluster has:
> - **Databricks Runtime 18 LTS** or above
> - **Dedicated** (Single User) access mode
> - **Photon disabled** (runtime engine: STANDARD)
> - **Autoscaling disabled** (fixed worker count)
> - Spark config: `spark.databricks.streaming.realTimeMode.enabled true`

#### Kafka Secret Scope Setup (Required for Parts 1 & 2)

> **Before deploying**, set up a Databricks Secret Scope with your Kafka bootstrap servers. The Quick Start notebook (`RTM_00`) does **not** require this — only the Kafka-based notebooks do.

```bash
# 1. Create a secret scope
databricks secrets create-scope my-kafka-scope

# 2. Store your Kafka TLS bootstrap servers (port 9094)
databricks secrets put-secret my-kafka-scope kafka-bootstrap-servers-tls \
  --string-value "b-1.your-cluster.kafka.region.amazonaws.com:9094,b-2.your-cluster.kafka.region.amazonaws.com:9094"

# 3. (Optional) Store plaintext bootstrap servers (port 9092)
databricks secrets put-secret my-kafka-scope kafka-bootstrap-servers-plaintext \
  --string-value "b-1.your-cluster.kafka.region.amazonaws.com:9092"
```

Then set the **`secret_scope` widget** at the top of each notebook (e.g., `RTM_01`, `RTM_02`) to your scope name (e.g., `my-kafka-scope`).

#### Deploy the Solution

1. **Clone this repository** into your Databricks workspace:
   ```shell
   git clone https://github.com/databricks-industry-solutions/rtm-fraud-detection.git
   cd rtm-fraud-detection
   ```

2. **Deploy and run the accelerator**:
   ```shell
   databricks bundle deploy
   databricks bundle run rtm_fraud_detection_workflow
   ```

The bundle will automatically:
- Create a dedicated cluster with the correct RTM configuration
- Run the introduction and quick start notebooks
- Execute the full fraud detection pipeline
- Train and deploy the ML model with MLflow

Alternatively, you can deploy from the **Databricks UI**:
1. Open the cloned folder in your workspace
2. Open the **Asset Bundle Editor**
3. Click **Deploy**, then navigate to the Deployments tab and click **Run**

#### Cleanup

When you're finished, remove all assets created by the accelerator:

```shell
bash scripts/cleanup.sh
```

## Manual Installation

If you prefer to run each step individually, follow this notebook-by-notebook approach.

> **Before you begin:** Create a cluster with the prerequisites above and attach it to each notebook.

### Notebook Overview

#### **00_Introduction**
**Start here!** This notebook provides the full context — the business problem, solution architecture, what's included in each notebook, and prerequisites. Read this first to understand the end-to-end flow.

#### **RTM_00_Quick_Start**
Self-contained RTM demo with **zero external dependencies**. Uses a built-in `rate` source for synthetic transactions and `display()` for output. Run this first to validate that Real-Time Mode works on your cluster in under 5 minutes.

#### **RTM_01_Introduction_fraud_detection**
End-to-end Kafka pipeline with:
- **Stateful velocity tracking** via `transformWithState` (per-card transaction counting with TTL)
- **Dictionary-based enrichment** for merchant/card profile lookups
- **Weighted multi-signal fraud scoring** with explainable features
- **Routing** to approved/flagged/blocked output Kafka topics

*Requires: Kafka (AWS MSK or Confluent Cloud) with TLS on port 9094. See [Kafka Secret Scope Setup](#kafka-secret-scope-setup-required-for-parts-1--2) to configure your credentials.*

#### **RTM_02_Advanced_fraud_detection_ml**
Upgrades from rules to ML:
- **Streams features to Lakebase** (online feature store via `jdbcStreaming` with upserts)
- **Trains a RandomForest model** tracked in MLflow
- **Scores transactions in real-time** using the model loaded as a Spark UDF

*Requires: Kafka + Lakebase instance.*

#### **Dashboard App** (`apps/`)
Streamlit-based Databricks App providing a live fraud detection dashboard:
- Total transactions and decision breakdown
- Recent fraud scores with card-level detail
- Fraud probability distribution
- Auto-refreshing every 10 seconds

*Requires: Lakebase (populated by RTM_02).*

### Supporting Resources

The `notebooks/resources/` directory contains shared utilities:
- **00_config** — Cluster validation, Kafka credentials, per-user topic naming
- **00_datagenerator** — Kafka producer for synthetic transaction data
- **00_reference_data** — Merchant and card profile reference tables

## Cost Considerations

The cost of running this accelerator depends on:
- Cluster compute time (dedicated classic cluster with 6 workers)
- Kafka cluster costs (if using AWS MSK or Confluent Cloud)
- Lakebase instance size and uptime (for Part 2)
- App serving hours (for the dashboard)

It is the user's responsibility to monitor and manage associated costs. The Quick Start notebook (`RTM_00`) has no external dependencies and minimal cost — start there.

## References

- [Introducing Real-Time Mode for Apache Spark Structured Streaming](https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming)
- [Real-Time Mode Documentation](https://docs.databricks.com/aws/en/structured-streaming/real-time.html)
- [Real-Time Mode Examples](https://docs.databricks.com/aws/en/structured-streaming/real-time-examples)
- [Stateful Applications with transformWithState](https://docs.databricks.com/aws/en/stateful-applications/)
- [Lakebase Documentation](https://docs.databricks.com/en/database/lakebase.html)
- [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)

## Authors

- **Sixuan He** ([sixuan.he@databricks.com](mailto:sixuan.he@databricks.com)) — Databricks Field Engineering

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Security

For security concerns, please review [SECURITY.md](SECURITY.md).

## License

&copy; 2026 Databricks, Inc. All rights reserved.

The source in this project is provided subject to the [Databricks License](https://databricks.com/db-license-source). All included or referenced third-party libraries are subject to the licenses set forth below.

| Package | License | Copyright |
|---------|---------|-----------|
| kafka-python | Apache 2.0 | Dana Powers, David Arthur, Thomas Siber |
| streamlit | Apache 2.0 | Snowflake Inc. |
| psycopg | LGPL 3.0 | Daniele Varrazzo |
| pandas | BSD 3-Clause | AQR Capital Management |
| databricks-sdk | Apache 2.0 | Databricks, Inc. |

## Support

For questions or issues:
1. Check the [Databricks documentation](https://docs.databricks.com)
2. Open an issue in this repository
3. Contact Databricks support if you're a customer

---

**Ready to detect fraud in real time?** Start by cloning this repo and running `RTM_00_Quick_Start` — you'll see sub-300ms streaming in under 5 minutes!
