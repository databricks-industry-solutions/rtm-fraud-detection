# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Feature Writer (`foreach` sink)
# MAGIC
# MAGIC `%run`-importable utility that defines `LakebaseFeatureWriter`, a per-partition
# MAGIC `foreach` writer for streaming features into Lakebase (managed PostgreSQL) from
# MAGIC Real-Time Mode pipelines.
# MAGIC
# MAGIC ## Why this exists
# MAGIC
# MAGIC `jdbcStreaming` is a managed sink that handles connection lifecycle, credential
# MAGIC refresh, batching, and upsert SQL internally. It is currently in **Private
# MAGIC Preview** and so cannot be referenced in public-facing content like blogs or
# MAGIC public Solution Accelerators.
# MAGIC
# MAGIC `foreach` is the supported public-API alternative for custom sinks in Real-Time
# MAGIC Mode. This module reproduces the key things `jdbcStreaming` was doing
# MAGIC internally so that the calling notebook stays clean:
# MAGIC
# MAGIC 1. Accepts host, user, and a pre-generated Lakebase token directly
# MAGIC 2. Buffers rows and flushes via Postgres `INSERT ... ON CONFLICT` (upsert)
# MAGIC 3. Reconnects on transient `OperationalError` / `InterfaceError`
# MAGIC 4. Cleans up cleanly on stream stop
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC ```python
# MAGIC %run ./resources/00_lakebase_writer
# MAGIC
# MAGIC writer = LakebaseFeatureWriter(
# MAGIC     host=LAKEBASE_HOST,
# MAGIC     user=LAKEBASE_USER,
# MAGIC     password=LAKEBASE_TOKEN,
# MAGIC     table=FEATURE_TABLE,
# MAGIC     columns=[...],          # column order must match feature_output.select(...)
# MAGIC     key_columns=["card_id"]
# MAGIC )
# MAGIC
# MAGIC feature_query = (
# MAGIC     feature_output.select(*FEATURE_COLUMNS)
# MAGIC     .writeStream
# MAGIC     .foreach(writer)
# MAGIC     .option("checkpointLocation", checkpoint_feature_store)
# MAGIC     .trigger(realTime="30 seconds")
# MAGIC     .outputMode("update")
# MAGIC     .start()
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ## Tuning knobs
# MAGIC
# MAGIC - `BUFFER_SIZE` — rows buffered in memory before each `INSERT` round-trip.
# MAGIC   Higher = better throughput, slightly higher latency between flushes.
# MAGIC
# MAGIC ## Caveat: 1-hour token TTL
# MAGIC
# MAGIC The Lakebase token expires after ~60 minutes, after which the streaming
# MAGIC query will fail. Generate a new token from the Lakebase UI Connect dialog
# MAGIC and restart the streaming query. Suitable for short test/demo runs;
# MAGIC for long-running production streams wait for `jdbcStreaming` GA
# MAGIC (which handles credential refresh internally).

# COMMAND ----------

import time

import psycopg


class LakebaseFeatureWriter:
    """Per-partition `foreach` writer that upserts streaming features into Lakebase.

    Buffers rows in memory and flushes via Postgres `INSERT ... ON CONFLICT` for
    upsert semantics.

    ## Auth model

    Accepts a pre-generated Lakebase token (from the Lakebase UI Connect dialog)
    directly as the ``password`` parameter. No Databricks SDK calls are made.

    ## Limitation: 1-hour token TTL

    Lakebase tokens expire after ~60 minutes. Generate a new token from the
    Lakebase UI and restart the streaming query for runs longer than ~50 minutes.
    """

    BUFFER_SIZE = 200                    # rows; tune for throughput vs. freshness

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        table: str,
        columns: list,
        key_columns: list,
        database: str = "databricks_postgres",
        port: int = 5432,
    ):
        """All connection details are captured eagerly on the driver.

        Args:
            host:          DNS hostname from the Lakebase Connect dialog, e.g.
                           ``<endpoint-id>.database.<region>.azuredatabricks.net``
            user:          Databricks user email.
            password:      Lakebase token (generate from the Lakebase UI Connect dialog).
            table:         Target PostgreSQL table name.
            columns:       Column names in insert order.
            key_columns:   Primary key column(s) for ON CONFLICT upsert.
            database:      PostgreSQL database name (default ``databricks_postgres``).
            port:          PostgreSQL port (default ``5432``).
        """
        self.table = table
        self.columns = list(columns)
        self.key_columns = list(key_columns)
        self.database = database
        self.port = port
        self.host = host
        self.user = user
        self.password = password

        # Pre-build the upsert SQL once on the driver so we don't redo string
        # concatenation in every executor's open().
        self._upsert_sql = self._build_upsert_sql()

        # State below is reinitialized in open() on each executor.
        self.conn = None
        self.buffer = []

    def _build_upsert_sql(self) -> str:
        cols = ", ".join(f'"{c}"' for c in self.columns)
        placeholders = ", ".join(["%s"] * len(self.columns))
        keys = ", ".join(f'"{c}"' for c in self.key_columns)
        non_key = [c for c in self.columns if c not in self.key_columns]
        if not non_key:
            return (
                f'INSERT INTO "{self.table}" ({cols}) VALUES ({placeholders}) '
                f'ON CONFLICT ({keys}) DO NOTHING'
            )
        updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_key)
        return (
            f'INSERT INTO "{self.table}" ({cols}) VALUES ({placeholders}) '
            f'ON CONFLICT ({keys}) DO UPDATE SET {updates}'
        )

    def _connect(self) -> None:
        # Executor-safe: uses only plain fields captured on the driver.
        self.conn = psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
            sslmode="require",
            autocommit=False,
        )

    def _flush(self) -> None:
        if not self.buffer:
            return
        try:
            with self.conn.cursor() as cur:
                cur.executemany(self._upsert_sql, self.buffer)
            self.conn.commit()
        except (psycopg.OperationalError, psycopg.InterfaceError):
            # Transient connection drop -- reconnect using the existing
            # driver-supplied credential and retry once. If the credential
            # itself has expired, this will fail and the task will be retried
            # by Spark, which will re-deserialize the writer from the driver
            # (still with the original credential -- so for runs >1hr you must
            # restart the streaming query).
            self._connect()
            with self.conn.cursor() as cur:
                cur.executemany(self._upsert_sql, self.buffer)
            self.conn.commit()
        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                pass
            raise
        finally:
            self.buffer = []

    # ---- ForeachWriter contract ---------------------------------------------------

    def open(self, partition_id, epoch_id) -> bool:
        self._connect()
        self.buffer = []
        return True

    def process(self, row) -> None:
        # row is pyspark.sql.Row -- extract values in declared column order
        self.buffer.append(tuple(row[c] for c in self.columns))
        if len(self.buffer) >= self.BUFFER_SIZE:
            self._flush()

    def close(self, error) -> None:
        try:
            if error is None:
                self._flush()
        finally:
            if self.conn is not None:
                try:
                    self.conn.close()
                finally:
                    self.conn = None


# COMMAND ----------

# MAGIC %md
# MAGIC ## Smoke test (optional)
# MAGIC
# MAGIC Run this cell only when developing the writer in isolation. It is a no-op
# MAGIC when the notebook is loaded via `%run` from another notebook (because the
# MAGIC `LAKEBASE_PROJECT_NAME` and `FEATURE_TABLE` widgets won't be set there).

# COMMAND ----------

print("LakebaseFeatureWriter loaded.")
print("  BUFFER_SIZE =", LakebaseFeatureWriter.BUFFER_SIZE)
