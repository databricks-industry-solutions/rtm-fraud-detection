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
# MAGIC Mode. This module reproduces the five things `jdbcStreaming` was doing
# MAGIC internally so that the calling notebook stays clean:
# MAGIC
# MAGIC 1. Resolves the Lakebase instance and opens a `psycopg` connection
# MAGIC 2. Refreshes the short-lived Lakebase OAuth credential before its 60-min TTL expires
# MAGIC 3. Buffers rows and flushes via Postgres `INSERT ... ON CONFLICT` (upsert)
# MAGIC 4. Reconnects on transient `OperationalError` / `InterfaceError`
# MAGIC 5. Cleans up cleanly on stream stop
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC ```python
# MAGIC %run ./resources/00_lakebase_writer
# MAGIC
# MAGIC writer = LakebaseFeatureWriter(
# MAGIC     instance_name=LAKEBASE_INSTANCE_NAME,
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
# MAGIC ## Caveat: 1-hour credential TTL
# MAGIC
# MAGIC The Databricks SDK is only callable from the **driver** (notebook auth env
# MAGIC vars are not propagated to executor Python workers), so this writer
# MAGIC resolves the Lakebase host/user/token on the driver in `__init__` and
# MAGIC ships them as plain fields to executors. The token expires after ~60
# MAGIC minutes, after which the streaming query will fail. Suitable for
# MAGIC short test runs; for long-running production streams either pass a
# MAGIC long-lived service principal PAT as `password=...`, or wait for
# MAGIC `jdbcStreaming` GA (which handles refresh internally).

# COMMAND ----------

import time

import psycopg


class LakebaseFeatureWriter:
    """Per-partition `foreach` writer that upserts streaming features into Lakebase.

    Buffers rows in memory and flushes via Postgres `INSERT ... ON CONFLICT` for
    upsert semantics.

    ## Auth model

    The Databricks SDK is only callable from the **driver** (the notebook process)
    because Databricks runtime auth env vars are not propagated to executor
    Python workers. This class therefore resolves the Lakebase host, port, user,
    and short-lived OAuth token on the driver in `__init__` and ships them as
    plain serializable fields to executors. Executors only call `psycopg.connect()`.

    ## Limitation: 1-hour credential TTL

    Lakebase short-lived credentials expire after ~60 minutes. Because executors
    cannot mint new credentials on their own, this writer is suitable for
    streaming runs that finish within ~50 minutes, or for streams that you
    restart periodically. For unbounded production streams, either:
      - wait for `jdbcStreaming` GA (handles refresh internally), or
      - use a service principal with a longer-lived PAT and pass `password=...`
        to the constructor instead of letting it generate one.
    """

    BUFFER_SIZE = 200                    # rows; tune for throughput vs. freshness

    def __init__(
        self,
        instance_name: str,
        table: str,
        columns: list,
        key_columns: list,
        database: str = "databricks_postgres",
        host: str = None,
        port: int = 5432,
        user: str = None,
        password: str = None,
    ):
        """All connection details are captured eagerly on the driver. If `host`,
        `user`, or `password` are not provided, they are resolved via the
        Databricks SDK at construction time (driver-side only).
        """
        self.instance_name = instance_name
        self.table = table
        self.columns = list(columns)
        self.key_columns = list(key_columns)
        self.database = database
        self.port = port

        # Resolve any unspecified connection details on the DRIVER. Executors
        # never call the Databricks SDK -- they get plain strings from `self`.
        if host is None or user is None or password is None:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            instance = w.database.get_database_instance(name=instance_name)
            cred = w.database.generate_database_credential(instance_names=[instance_name])
            host = host or instance.read_write_dns
            user = user or w.current_user.me().user_name
            password = password or cred.token

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
# MAGIC `LAKEBASE_INSTANCE_NAME` and `FEATURE_TABLE` widgets won't be set there).

# COMMAND ----------

print("LakebaseFeatureWriter loaded.")
print("  BUFFER_SIZE =", LakebaseFeatureWriter.BUFFER_SIZE)
