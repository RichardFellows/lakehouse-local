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
        # 🦆 DuckDB — Local File

        The simplest path: DuckDB reads directly from a local `.duckdb` file
        created by `dbt-duckdb`. No network, no S3, no catalog — just a file.

        **Path:** `DuckDB → local file (lakehouse.duckdb)`

        This is the lightweight dev mode. dbt-duckdb writes to a single file,
        and DuckDB reads it in-process with zero overhead.
        """
    )
    return


@app.cell
def _(mo, os):
    con = None
    try:
        import duckdb

        db_path = os.environ.get("LAKEHOUSE_DB", "/opt/dbt/lakehouse.duckdb")
        con = duckdb.connect(db_path, read_only=True)

        # Get file size
        file_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
        size_str = f"{file_size / 1024:.1f} KB" if file_size < 1048576 else f"{file_size / 1048576:.1f} MB"

        mo.md(
            f"✅ DuckDB connected\n\n"
            f"- File: `{db_path}`\n"
            f"- Size: {size_str}\n"
            f"- Read-only: yes (avoids locks with Airflow DAG runs)"
        )
    except Exception as e:
        mo.md(f"❌ Connection failed: `{e}`")
    return (con,)


@app.cell
def _(con, pd):
    def query(sql):
        if con is None:
            return pd.DataFrame({"error": ["Not connected"]})
        try:
            return con.execute(sql).fetchdf()
        except Exception as e:
            return pd.DataFrame({"error": [str(e)]})

    return (query,)


@app.cell
def _(mo):
    mo.md("## Database Objects")
    return


@app.cell
def _(mo, query):
    tables = query("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
    """)
    mo.ui.table(tables)
    return


@app.cell
def _(mo):
    mo.md("## Table Schema — `customer_orders`")
    return


@app.cell
def _(mo, query):
    schema = query("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'customer_orders'
        ORDER BY ordinal_position
    """)
    mo.ui.table(schema)
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
            .properties(width=500, height=300, title="Revenue by Customer Tier (DuckDB local)")
        )
        mo.ui.altair_chart(chart)
    except ImportError:
        mo.md("_Install altair for charts_")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## DuckDB Internals

        DuckDB stores data in a columnar format optimised for analytical queries.
        """
    )
    return


@app.cell
def _(mo, query):
    # Database size and table stats
    storage = query("""
        SELECT
            table_name,
            estimated_size,
            column_count,
            index_count
        FROM duckdb_tables()
        WHERE schema_name = 'main'
    """)
    mo.ui.table(storage)
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Custom SQL

        Full DuckDB SQL — window functions, CTEs, regex, list comprehensions, etc.
        """
    )
    return


@app.cell
def _(mo):
    sql_input = mo.ui.code_editor(
        value="SELECT\n    first_name || ' ' || last_name as full_name,\n    total_orders,\n    total_revenue,\n    RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank\nFROM customer_orders",
        language="sql",
        min_height=100,
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
