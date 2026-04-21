import os
import time
import traceback

import streamlit as st
import pandas as pd
import psycopg
from databricks import sdk
from psycopg_pool import ConnectionPool

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCORES_TABLE = "fraud_scores"
FEATURES_TABLE = "card_features"
REFRESH_INTERVAL = 10

# ---------------------------------------------------------------------------
# Database connection (uses PG* env vars injected by Lakebase resource binding)
# ---------------------------------------------------------------------------
workspace_client = sdk.WorkspaceClient()
postgres_password = None
last_password_refresh = 0
connection_pool = None


def refresh_oauth_token():
    """Refresh OAuth token if expired."""
    global postgres_password, last_password_refresh
    if postgres_password is None or time.time() - last_password_refresh > 900:
        print("Refreshing PostgreSQL OAuth token")
        postgres_password = workspace_client.config.oauth_token().access_token
        last_password_refresh = time.time()


def get_connection_pool():
    """Get or create the connection pool."""
    global connection_pool
    if connection_pool is None:
        refresh_oauth_token()
        conn_string = (
            f"dbname={os.getenv('PGDATABASE')} "
            f"user={os.getenv('PGUSER')} "
            f"password={postgres_password} "
            f"host={os.getenv('PGHOST')} "
            f"port={os.getenv('PGPORT')} "
            f"sslmode={os.getenv('PGSSLMODE', 'require')} "
            f"application_name={os.getenv('PGAPPNAME')}"
        )
        connection_pool = ConnectionPool(conn_string, min_size=2, max_size=10)
    return connection_pool


def get_connection():
    """Get a connection from the pool."""
    global connection_pool
    if postgres_password is None or time.time() - last_password_refresh > 900:
        if connection_pool:
            connection_pool.close()
            connection_pool = None
    return get_connection_pool().connection()


def query(sql: str) -> pd.DataFrame:
    """Execute a SQL query and return a DataFrame."""
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(page_title="RTM Fraud Detection Dashboard", layout="wide")

st.markdown("""
<style>
    .block-container { padding-top: 1rem; }
    [data-testid="stMetric"] {
        background-color: rgba(128, 128, 128, 0.15);
        border: 1px solid rgba(128, 128, 128, 0.25);
        border-radius: 8px;
        padding: 12px 16px;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("Real-Time Fraud Detection Dashboard")
st.caption("Live ML fraud scoring results from Lakebase")

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("Settings")
    st.text_input("Lakebase Host", value=os.getenv("PGHOST", "N/A"), disabled=True)
    st.text_input("Database", value=os.getenv("PGDATABASE", "N/A"), disabled=True)
    st.text_input("Scores Table", value=SCORES_TABLE, disabled=True)
    st.text_input("Features Table", value=FEATURES_TABLE, disabled=True)
    auto_refresh = st.toggle("Auto-refresh", value=False)
    st.caption(f"Refresh interval: {REFRESH_INTERVAL}s")
    st.divider()
    st.markdown(
        "**RTM Solutions Accelerator**  \n"
        "Real-time fraud detection powered by  \n"
        "Databricks Structured Streaming,  \n"
        "Lakebase, and MLflow."
    )

# ---------------------------------------------------------------------------
# Main dashboard
# ---------------------------------------------------------------------------
try:
    # Test connection
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    st.success("Connected to Lakebase!")

    # --- Summary metrics ---
    summary_df = query(f"""
        SELECT
            ml_decision,
            COUNT(*)                                    AS cnt,
            ROUND(AVG(fraud_probability)::numeric, 4)   AS avg_prob,
            ROUND(MIN(fraud_probability)::numeric, 4)   AS min_prob,
            ROUND(MAX(fraud_probability)::numeric, 4)   AS max_prob,
            ROUND(AVG(amount_usd)::numeric, 2)          AS avg_amount
        FROM {SCORES_TABLE}
        GROUP BY ml_decision
        ORDER BY avg_prob
    """)

    total = int(summary_df["cnt"].sum())
    decisions = summary_df.set_index("ml_decision")

    def _get(decision, col, default=0):
        return decisions.loc[decision, col] if decision in decisions.index else default

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Scored", f"{total:,}")
    m2.metric("Approved", f"{int(_get('APPROVED', 'cnt')):,}")
    m3.metric("Flagged", f"{int(_get('FLAGGED', 'cnt')):,}")
    m4.metric("Blocked", f"{int(_get('BLOCKED', 'cnt')):,}")

    # --- Charts ---
    left, right = st.columns(2)

    with left:
        st.subheader("Decision Breakdown")
        if not summary_df.empty:
            chart_df = summary_df[["ml_decision", "cnt"]].rename(
                columns={"ml_decision": "Decision", "cnt": "Count"}
            )
            st.bar_chart(chart_df, x="Decision", y="Count", color="Decision")

    with right:
        st.subheader("Avg Fraud Probability by Decision")
        if not summary_df.empty:
            prob_df = summary_df[["ml_decision", "avg_prob"]].rename(
                columns={"ml_decision": "Decision", "avg_prob": "Avg Probability"}
            )
            st.bar_chart(prob_df, x="Decision", y="Avg Probability", color="Decision")

    # --- Amount distribution ---
    st.subheader("Amount Distribution by Decision")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Average Amount by Decision**")
        avg_amt = summary_df[["ml_decision", "avg_amount"]].rename(
            columns={"ml_decision": "Decision", "avg_amount": "Avg Amount ($)"}
        )
        st.bar_chart(avg_amt, x="Decision", y="Avg Amount ($)", color="Decision")
    with col_b:
        st.markdown("**Probability Distribution**")
        prob_dist = query(f"""
            SELECT
                CASE
                    WHEN fraud_probability < 0.2 THEN '0.0-0.2'
                    WHEN fraud_probability < 0.4 THEN '0.2-0.4'
                    WHEN fraud_probability < 0.6 THEN '0.4-0.6'
                    WHEN fraud_probability < 0.8 THEN '0.6-0.8'
                    ELSE '0.8-1.0'
                END AS prob_bucket,
                COUNT(*) AS count
            FROM {SCORES_TABLE}
            GROUP BY prob_bucket
            ORDER BY prob_bucket
        """)
        st.bar_chart(prob_dist, x="prob_bucket", y="count")

    # --- Recent transactions ---
    st.subheader("Recent Transactions")
    recent_df = query(f"""
        SELECT
            transaction_id,
            card_id,
            amount_usd,
            ROUND(fraud_probability::numeric, 4)  AS fraud_prob,
            ml_decision,
            model_version,
            scored_at
        FROM {SCORES_TABLE}
        ORDER BY scored_at DESC
        LIMIT 100
    """)
    st.dataframe(recent_df, use_container_width=True, height=400)

    # --- Card features ---
    st.subheader("Card Feature Store (Lakebase)")
    try:
        features_df = query(f"""
            SELECT * FROM {FEATURES_TABLE} ORDER BY updated_at DESC
        """)
        st.dataframe(features_df, use_container_width=True)
    except Exception as fe:
        st.info(f"Feature store table not found: {fe}")

except Exception as e:
    st.error(f"Error: {e}")
    st.code(traceback.format_exc())
    st.markdown("""
    **Troubleshooting:**
    1. Is the Lakebase instance running?
    2. Did you run Notebook 2 (`RTM_02`) to create and populate the tables?
    3. Does the app have a Lakebase resource configured?
    """)

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------
if auto_refresh:
    time.sleep(REFRESH_INTERVAL)
    st.rerun()
