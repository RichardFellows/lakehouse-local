import marimo

__generated_with = "0.11.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Lakehouse Explorer

        Query the DuckDB lakehouse after dbt has run.
        This notebook connects to the same DuckDB database that dbt writes to
        and lets you explore the staged and mart tables interactively.

        **Prerequisites:** Run `.\lakehouse.ps1 dbt-run` first to populate the tables.
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    import duckdb
    import os
    return duckdb, mo, os


@app.cell
def _(duckdb, mo, os):
    # Connect to the dbt DuckDB database
    db_path = os.environ.get("LAKEHOUSE_DB", "dbt_project/lakehouse.duckdb")
    try:
        con = duckdb.connect(db_path, read_only=True)
        mo.md(f"✅ Connected to `{db_path}`")
    except Exception as e:
        mo.md(f"❌ Failed to connect to `{db_path}`: {e}")
        con = None
    return con, db_path


@app.cell
def _(con, mo):
    # Show all tables
    if con:
        tables = con.execute("SHOW TABLES").fetchdf()
        mo.md("## Tables in the lakehouse")
    return tables,


@app.cell
def _(mo, tables):
    mo.ui.table(tables) if tables is not None and len(tables) > 0 else mo.md("_No tables found. Run dbt first._")
    return


@app.cell
def _(mo):
    mo.md("## Staging: Customers")
    return


@app.cell
def _(con, mo):
    if con:
        try:
            customers_df = con.execute("SELECT * FROM stg_customers").fetchdf()
            mo.ui.table(customers_df)
        except Exception as e:
            mo.md(f"_Table not found: {e}. Run `dbt seed` and `dbt run` first._")
    return customers_df,


@app.cell
def _(mo):
    mo.md("## Staging: Orders")
    return


@app.cell
def _(con, mo):
    if con:
        try:
            orders_df = con.execute("SELECT * FROM stg_orders").fetchdf()
            mo.ui.table(orders_df)
        except Exception as e:
            mo.md(f"_Table not found: {e}. Run `dbt seed` and `dbt run` first._")
    return orders_df,


@app.cell
def _(mo):
    mo.md("## Mart: Customer Orders")
    return


@app.cell
def _(con, mo):
    if con:
        try:
            mart_df = con.execute("SELECT * FROM customer_orders ORDER BY total_revenue DESC").fetchdf()
            mo.ui.table(mart_df)
        except Exception as e:
            mo.md(f"_Table not found: {e}. Run `dbt run` first._")
    return mart_df,


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
def _(con, mo):
    if con:
        try:
            tier_df = con.execute(
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
            ).fetchdf()
            mo.ui.table(tier_df)
        except Exception as e:
            mo.md(f"_Query failed: {e}_")
    return tier_df,


@app.cell
def _(mo, tier_df):
    # Bar chart of revenue by tier
    try:
        chart = mo.ui.altair_chart(
            _create_chart(tier_df),
        )
        chart
    except Exception:
        mo.md("_Install altair for charts: `pip install altair`_")
    return chart,


@app.cell
def _():
    def _create_chart(df):
        import altair as alt

        return (
            alt.Chart(df)
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

    return (_create_chart,)


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
    return query_input,


@app.cell
def _(con, mo, query_input):
    if con and query_input.value.strip():
        try:
            result_df = con.execute(query_input.value).fetchdf()
            mo.ui.table(result_df)
        except Exception as e:
            mo.md(f"❌ Query error: `{e}`")
    return result_df,


if __name__ == "__main__":
    app.run()
