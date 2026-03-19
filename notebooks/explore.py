import marimo

__generated_with = "0.11.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        """
        # Lakehouse Explorer

        Query the lakehouse after dbt has run. Supports two connection modes:

        - **DuckDB** (default) — connects to the local `.duckdb` file
        - **Spark** — connects to Spark Thrift Server via PyHive

        Set the `LAKEHOUSE_ENGINE` environment variable to `spark` to use Spark mode.

        **Prerequisites:** Run `dbt-run` first to populate the tables.
        """
    )
    return


@app.cell
def _(mo):
    import os

    engine = os.environ.get("LAKEHOUSE_ENGINE", "duckdb")
    mo.md(f"**Engine:** `{engine}`")
    return engine, os


@app.cell
def _(engine, mo, os):
    con = None
    engine_label = engine

    if engine == "spark":
        try:
            from pyhive import hive

            spark_host = os.environ.get("SPARK_THRIFT_HOST", "spark")
            spark_port = int(os.environ.get("SPARK_THRIFT_PORT", "10000"))
            con = hive.connect(host=spark_host, port=spark_port)
            engine_label = f"Spark Thrift ({spark_host}:{spark_port})"
            mo.md(f"✅ Connected to **{engine_label}**")
        except ImportError:
            mo.md("❌ `pyhive` not installed. Install with: `pip install PyHive`")
        except Exception as e:
            mo.md(f"❌ Failed to connect to Spark Thrift: `{e}`")
    else:
        try:
            import duckdb

            db_path = os.environ.get("LAKEHOUSE_DB", "dbt_project/lakehouse.duckdb")
            con = duckdb.connect(db_path, read_only=True)
            engine_label = f"DuckDB ({db_path})"
            mo.md(f"✅ Connected to **{engine_label}**")
        except Exception as e:
            mo.md(f"❌ Failed to connect to DuckDB: `{e}`")

    return con, engine_label


@app.cell
def _(con, engine, mo):
    import pandas as pd

    def run_query(sql):
        """Execute SQL and return a pandas DataFrame, works with both DuckDB and PyHive."""
        if con is None:
            return pd.DataFrame()
        if engine == "spark":
            cursor = con.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        else:
            return con.execute(sql).fetchdf()

    mo.md("_Query helper ready_")
    return pd, run_query


@app.cell
def _(mo):
    mo.md("## Tables")
    return


@app.cell
def _(engine, mo, run_query):
    try:
        if engine == "spark":
            tables_df = run_query("SHOW TABLES")
        else:
            tables_df = run_query("SHOW TABLES")
        mo.ui.table(tables_df)
    except Exception as e:
        mo.md(f"_Could not list tables: {e}_")
    return (tables_df,)


@app.cell
def _(mo):
    mo.md("## Staging: Customers")
    return


@app.cell
def _(mo, run_query):
    try:
        customers_df = run_query("SELECT * FROM stg_customers")
        mo.ui.table(customers_df)
    except Exception as e:
        mo.md(f"_Table not found: {e}. Run dbt first._")
    return (customers_df,)


@app.cell
def _(mo):
    mo.md("## Staging: Orders")
    return


@app.cell
def _(mo, run_query):
    try:
        orders_df = run_query("SELECT * FROM stg_orders")
        mo.ui.table(orders_df)
    except Exception as e:
        mo.md(f"_Table not found: {e}. Run dbt first._")
    return (orders_df,)


@app.cell
def _(mo):
    mo.md("## Mart: Customer Orders")
    return


@app.cell
def _(mo, run_query):
    try:
        mart_df = run_query("SELECT * FROM customer_orders ORDER BY total_revenue DESC")
        mo.ui.table(mart_df)
    except Exception as e:
        mo.md(f"_Table not found: {e}. Run dbt first._")
    return (mart_df,)


@app.cell
def _(mo):
    mo.md(
        """
        ## Revenue by Customer Tier

        Breakdown of total revenue and order count by the customer tier
        calculated in the `customer_orders` mart.
        """
    )
    return


@app.cell
def _(mo, run_query):
    try:
        tier_df = run_query(
            """
            SELECT
                customer_tier,
                COUNT(*) as customer_count,
                SUM(total_orders) as total_orders,
                ROUND(SUM(total_revenue), 2) as total_revenue,
                ROUND(AVG(total_revenue), 2) as avg_revenue_per_customer
            FROM customer_orders
            GROUP BY customer_tier
            ORDER BY total_revenue DESC
            """
        )
        mo.ui.table(tier_df)
    except Exception as e:
        mo.md(f"_Query failed: {e}_")
    return (tier_df,)


@app.cell
def _(mo, tier_df):
    try:
        import altair as alt

        chart_data = tier_df.copy()
        chart = mo.ui.altair_chart(
            alt.Chart(chart_data)
            .mark_bar()
            .encode(
                x=alt.X("customer_tier:N", title="Customer Tier", sort="-y"),
                y=alt.Y("total_revenue:Q", title="Total Revenue (£)"),
                color=alt.Color(
                    "customer_tier:N",
                    scale=alt.Scale(
                        domain=["high", "medium", "low"],
                        range=["#2ecc71", "#f39c12", "#e74c3c"],
                    ),
                ),
                tooltip=["customer_tier", "customer_count", "total_orders", "total_revenue"],
            )
            .properties(width=400, height=300, title="Revenue by Customer Tier")
        )
        chart
    except ImportError:
        mo.md("_Install altair for charts: `pip install altair`_")
    except Exception as e:
        mo.md(f"_Chart error: {e}_")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Custom Query

        Write your own SQL against the lakehouse.
        """
    )
    return


@app.cell
def _(mo):
    query_input = mo.ui.code_editor(
        value="SELECT * FROM customer_orders WHERE total_orders >= 2",
        language="sql",
        min_height=80,
    )
    query_input
    return (query_input,)


@app.cell
def _(mo, query_input, run_query):
    if query_input.value.strip():
        try:
            result_df = run_query(query_input.value)
            mo.ui.table(result_df)
        except Exception as e:
            mo.md(f"❌ Query error: `{e}`")
    return


@app.cell
def _(engine, engine_label, mo):
    mo.md(
        f"""
        ---
        **Connection:** {engine_label} | **Engine:** `{engine}`

        Switch engines by setting `LAKEHOUSE_ENGINE=spark` or `LAKEHOUSE_ENGINE=duckdb`
        """
    )
    return


if __name__ == "__main__":
    app.run()
