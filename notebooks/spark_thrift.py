import marimo

__generated_with = "0.11.0"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import os
    return mo, os, pd


@app.cell
def _(mo):
    mo.md(
        """
        # ⚡ Spark Thrift Server — Iceberg via SQL

        Connects to Spark's HiveServer2 (Thrift) endpoint using PyHive.
        Spark executes SQL against Iceberg tables stored in the Nessie catalog on S3.

        **Path:** `PyHive → Thrift (port 10000) → Spark → Iceberg → Nessie → S3`

        This is the same interface that dbt-spark uses. Full SQL support including
        DDL, DML, and Spark-specific functions.
        """
    )
    return


@app.cell
def _(mo, os):
    spark_con = None
    try:
        from pyhive import hive

        host = os.environ.get("SPARK_THRIFT_HOST", "spark")
        port = int(os.environ.get("SPARK_THRIFT_PORT", "10000"))
        spark_con = hive.connect(host=host, port=port, auth="NOSASL")
        spark_con.cursor().execute("USE db")
        mo.md(f"✅ Connected to Spark Thrift Server (`{host}:{port}`, schema: `db`)")
    except Exception as e:
        mo.md(f"❌ Connection failed: `{e}`")
    return (spark_con,)


@app.cell
def _(spark_con, pd):
    def query(sql):
        if spark_con is None:
            return pd.DataFrame({"error": ["Not connected"]})
        try:
            cursor = spark_con.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            return pd.DataFrame({"error": [str(e)]})

    return (query,)


@app.cell
def _(mo):
    mo.md("## Available Tables")
    return


@app.cell
def _(mo, query):
    tables = query("SHOW TABLES")
    mo.ui.table(tables)
    return


@app.cell
def _(mo):
    mo.md("## Table Details — `customer_orders`")
    return


@app.cell
def _(mo, query):
    schema = query("DESCRIBE TABLE customer_orders")
    mo.ui.table(schema)
    return


@app.cell
def _(mo):
    mo.md("## Iceberg Table Metadata")
    return


@app.cell
def _(mo, query):
    # Iceberg-specific: show table properties, snapshots, history
    try:
        props = query("SHOW TBLPROPERTIES customer_orders")
        mo.vstack([
            mo.md("### Table Properties"),
            mo.ui.table(props),
        ])
    except Exception as e:
        mo.md(f"_Could not read properties: {e}_")
    return


@app.cell
def _(mo, query):
    try:
        history = query("SELECT * FROM db.customer_orders.history")
        mo.vstack([
            mo.md("### Snapshot History"),
            mo.ui.table(history),
        ])
    except Exception:
        # Iceberg history syntax varies by version
        mo.md("_Snapshot history not available via this Spark version_")
    return


@app.cell
def _(mo):
    mo.md("## Customer Orders Data")
    return


@app.cell
def _(mo, query):
    data = query("SELECT * FROM customer_orders ORDER BY total_revenue DESC")
    mo.ui.table(data)
    return


@app.cell
def _(mo):
    mo.md("## Revenue by Tier")
    return


@app.cell
def _(mo, query):
    tier_data = query("""
        SELECT
            customer_tier,
            COUNT(*) as customer_count,
            SUM(total_orders) as total_orders,
            ROUND(SUM(total_revenue), 2) as total_revenue,
            ROUND(AVG(total_revenue), 2) as avg_revenue
        FROM customer_orders
        GROUP BY customer_tier
        ORDER BY total_revenue DESC
    """)
    mo.ui.table(tier_data)
    return (tier_data,)


@app.cell
def _(mo, tier_data):
    try:
        import altair as alt

        chart = (
            alt.Chart(tier_data)
            .mark_bar()
            .encode(
                x=alt.X("customer_tier:N", title="Tier", sort="-y"),
                y=alt.Y("total_revenue:Q", title="Revenue (£)"),
                color=alt.Color(
                    "customer_tier:N",
                    scale=alt.Scale(
                        domain=["high", "medium", "low"],
                        range=["#2ecc71", "#f39c12", "#e74c3c"],
                    ),
                    legend=None,
                ),
                tooltip=["customer_tier", "customer_count", "total_orders", "total_revenue"],
            )
            .properties(width=500, height=300, title="Revenue by Customer Tier (Spark)")
        )
        mo.ui.altair_chart(chart)
    except ImportError:
        mo.md("_Install altair for charts_")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Custom SQL

        Run any Spark SQL query — DDL, DML, Iceberg-specific operations.
        Examples:
        - `SELECT * FROM customer_orders WHERE customer_tier = 'high'`
        - `DESCRIBE EXTENDED customer_orders`
        - `SELECT snapshot_id, committed_at FROM db.customer_orders.snapshots`
        """
    )
    return


@app.cell
def _(mo):
    sql_input = mo.ui.code_editor(
        value="SELECT * FROM customer_orders WHERE total_orders >= 2 ORDER BY total_revenue DESC",
        language="sql",
        min_height=80,
    )
    sql_input
    return (sql_input,)


@app.cell
def _(mo, query, sql_input):
    if sql_input.value.strip():
        result = query(sql_input.value)
        mo.ui.table(result)
    return


if __name__ == "__main__":
    app.run()
