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
        # Lakehouse Explorer — Side by Side

        The same dbt models queried from **two different engines** simultaneously.
        Demonstrates that identical SQL transformations produce identical results
        regardless of the underlying compute engine.

        **Prerequisites:** Run `.\lakehouse.ps1 dbt-run` to populate both engines.
        """
    )
    return


@app.cell
def _(mo, os):
    # Connect to DuckDB
    duckdb_con = None
    duckdb_status = ""
    try:
        import duckdb

        db_path = os.environ.get("LAKEHOUSE_DB", "/opt/dbt/lakehouse.duckdb")
        duckdb_con = duckdb.connect(db_path, read_only=True)
        duckdb_status = f"✅ DuckDB connected (`{db_path}`)"
    except Exception as e:
        duckdb_status = f"❌ DuckDB: `{e}`"

    # Connect to Spark Thrift
    spark_con = None
    spark_status = ""
    try:
        from pyhive import hive

        spark_host = os.environ.get("SPARK_THRIFT_HOST", "spark")
        spark_port = int(os.environ.get("SPARK_THRIFT_PORT", "10000"))
        spark_con = hive.connect(host=spark_host, port=spark_port)
        spark_status = f"✅ Spark Thrift connected (`{spark_host}:{spark_port}`)"
    except ImportError:
        spark_status = "❌ Spark: `pyhive` not installed"
    except Exception as e:
        spark_status = f"❌ Spark: `{e}`"

    mo.md(f"{duckdb_status}\n\n{spark_status}")
    return duckdb_con, spark_con, duckdb_status, spark_status


@app.cell
def _(duckdb_con, spark_con, pd):
    def query_duckdb(sql):
        if duckdb_con is None:
            return pd.DataFrame({"error": ["DuckDB not connected"]})
        try:
            return duckdb_con.execute(sql).fetchdf()
        except Exception as e:
            return pd.DataFrame({"error": [str(e)]})

    def query_spark(sql):
        if spark_con is None:
            return pd.DataFrame({"error": ["Spark not connected"]})
        try:
            cursor = spark_con.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            return pd.DataFrame({"error": [str(e)]})

    def query_both(sql):
        return query_duckdb(sql), query_spark(sql)

    return query_duckdb, query_spark, query_both


@app.cell
def _(mo):
    mo.md("## Customer Orders Mart")
    return


@app.cell
def _(mo, query_both):
    duck_mart, spark_mart = query_both(
        "SELECT * FROM customer_orders ORDER BY total_revenue DESC"
    )

    mo.hstack(
        [
            mo.vstack([
                mo.md("### 🦆 DuckDB"),
                mo.ui.table(duck_mart),
            ]),
            mo.vstack([
                mo.md("### ⚡ Spark"),
                mo.ui.table(spark_mart),
            ]),
        ],
        widths="equal",
    )
    return duck_mart, spark_mart


@app.cell
def _(mo):
    mo.md("## Revenue by Customer Tier")
    return


@app.cell
def _(mo, query_both):
    tier_sql = """
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
    duck_tier, spark_tier = query_both(tier_sql)

    mo.hstack(
        [
            mo.vstack([
                mo.md("### 🦆 DuckDB"),
                mo.ui.table(duck_tier),
            ]),
            mo.vstack([
                mo.md("### ⚡ Spark"),
                mo.ui.table(spark_tier),
            ]),
        ],
        widths="equal",
    )
    return duck_tier, spark_tier


@app.cell
def _(mo, duck_tier, spark_tier):
    try:
        import altair as alt

        def make_tier_chart(df, title):
            return (
                alt.Chart(df)
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
                .properties(width=300, height=250, title=title)
            )

        combined = alt.hconcat(
            make_tier_chart(duck_tier, "🦆 DuckDB"),
            make_tier_chart(spark_tier, "⚡ Spark"),
        )
        mo.ui.altair_chart(combined)
    except ImportError:
        mo.md("_Install altair for charts_")
    except Exception as e:
        mo.md(f"_Chart error: {e}_")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Data Comparison

        Verify both engines produce identical results.
        """
    )
    return


@app.cell
def _(duck_mart, mo, spark_mart):
    try:
        # Normalise column names for comparison (Spark may prefix with schema)
        duck_compare = duck_mart.copy()
        spark_compare = spark_mart.copy()

        # Strip any schema prefix from Spark column names (e.g. "db.column" -> "column")
        spark_compare.columns = [c.split(".")[-1] for c in spark_compare.columns]
        duck_compare.columns = [c.split(".")[-1] for c in duck_compare.columns]

        # Sort both by customer_id for stable comparison
        if "customer_id" in duck_compare.columns and "customer_id" in spark_compare.columns:
            duck_sorted = duck_compare.sort_values("customer_id").reset_index(drop=True)
            spark_sorted = spark_compare.sort_values("customer_id").reset_index(drop=True)

            # Compare values (with tolerance for float differences)
            matches = True
            mismatches = []
            for col in duck_sorted.columns:
                if col in spark_sorted.columns:
                    try:
                        duck_vals = duck_sorted[col].astype(str).tolist()
                        spark_vals = spark_sorted[col].astype(str).tolist()
                        if duck_vals != spark_vals:
                            matches = False
                            mismatches.append(col)
                    except Exception:
                        pass

            if matches:
                mo.md("✅ **Results match** — both engines produced identical output.")
            else:
                mo.md(f"⚠️ **Differences found** in columns: {', '.join(mismatches)}")
        else:
            if "error" in duck_compare.columns or "error" in spark_compare.columns:
                mo.md("⚠️ One or both engines returned an error. Run `dbt-run` on both first.")
            else:
                mo.md("⚠️ Cannot compare — different column structures.")
    except Exception as e:
        mo.md(f"_Comparison error: {e}_")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Custom Query

        Write SQL and run it against both engines simultaneously.
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
def _(mo, query_both, query_input):
    if query_input.value.strip():
        duck_custom, spark_custom = query_both(query_input.value)

        mo.hstack(
            [
                mo.vstack([
                    mo.md("### 🦆 DuckDB"),
                    mo.ui.table(duck_custom),
                ]),
                mo.vstack([
                    mo.md("### ⚡ Spark"),
                    mo.ui.table(spark_custom),
                ]),
            ],
            widths="equal",
        )
    return


if __name__ == "__main__":
    app.run()
