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
        # Lakehouse Explorer — Three Engines, One Truth

        The same dbt models queried from **three different engines** simultaneously.
        Demonstrates that identical data can be accessed via different compute paths
        — all reading from the same Iceberg tables in the Nessie catalog.

        | Engine | Path | JVM Required |
        |--------|------|:---:|
        | 🦆 **DuckDB** | DuckDB → local file | No |
        | ⚡ **Spark** | PyHive → Thrift Server → Iceberg/Nessie → S3 | Yes |
        | 🧊 **PyIceberg** | PyIceberg → Nessie REST → S3 (direct read) | No |
        | 🦆🧊 **DuckDB+Iceberg** | DuckDB → Iceberg extension → S3 | No |

        **Prerequisites:** Run both DAGs in Airflow to populate the engines.
        """
    )
    return


@app.cell
def _(mo, os):
    # ── DuckDB (local file) ──
    duckdb_con = None
    duckdb_status = ""
    try:
        import duckdb

        db_path = os.environ.get("LAKEHOUSE_DB", "/opt/dbt/lakehouse.duckdb")
        duckdb_con = duckdb.connect(db_path, read_only=True)
        duckdb_status = f"✅ DuckDB connected (`{db_path}`)"
    except Exception as e:
        duckdb_status = f"❌ DuckDB: `{e}`"

    # ── Spark (Thrift Server) ──
    spark_con = None
    spark_status = ""
    try:
        from pyhive import hive

        spark_host = os.environ.get("SPARK_THRIFT_HOST", "spark")
        spark_port = int(os.environ.get("SPARK_THRIFT_PORT", "10000"))
        spark_con = hive.connect(host=spark_host, port=spark_port, auth="NOSASL")
        spark_con.cursor().execute("USE db")
        spark_status = f"✅ Spark Thrift connected (`{spark_host}:{spark_port}`)"
    except ImportError:
        spark_status = "❌ Spark: `pyhive` not installed"
    except Exception as e:
        spark_status = f"❌ Spark: `{e}`"

    # ── PyIceberg (direct catalog read via Nessie Iceberg REST) ──
    iceberg_catalog = None
    iceberg_status = ""
    try:
        from pyiceberg.catalog import load_catalog

        nessie_uri = os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg/")
        s3_endpoint = os.environ.get("S3_ENDPOINT", "http://localstack:4566")

        iceberg_catalog = load_catalog(
            "nessie",
            **{
                "type": "rest",
                "uri": nessie_uri,
                "warehouse": "warehouse",
                "s3.endpoint": s3_endpoint,
                "s3.access-key-id": "test",
                "s3.secret-access-key": "test",
                "s3.path-style-access": "true",
                "s3.region": "eu-west-2",
            },
        )
        # Test connection by listing namespaces
        namespaces = iceberg_catalog.list_namespaces()
        iceberg_status = f"✅ PyIceberg connected (namespaces: {['.'.join(n) for n in namespaces]})"
    except ImportError:
        iceberg_status = "❌ PyIceberg: not installed"
    except Exception as e:
        iceberg_status = f"❌ PyIceberg: `{e}`"

    # ── DuckDB + Iceberg extension ──
    duckdb_ice_con = None
    duckdb_ice_status = ""
    try:
        import duckdb as ddb_ice

        duckdb_ice_con = ddb_ice.connect()
        duckdb_ice_con.execute("INSTALL iceberg; LOAD iceberg;")
        duckdb_ice_con.execute("INSTALL httpfs; LOAD httpfs;")
        s3_ep = os.environ.get("S3_ENDPOINT", "http://localstack:4566")
        duckdb_ice_con.execute(f"""
            SET s3_endpoint='{s3_ep.replace("http://", "")}';
            SET s3_access_key_id='test';
            SET s3_secret_access_key='test';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            SET s3_region='eu-west-2';
        """)
        duckdb_ice_status = f"✅ DuckDB+Iceberg ready (S3 via `{s3_ep}`)"
    except Exception as e:
        duckdb_ice_status = f"❌ DuckDB+Iceberg: `{e}`"

    mo.md(
        f"### Connection Status\n\n"
        f"{duckdb_status}\n\n"
        f"{spark_status}\n\n"
        f"{iceberg_status}\n\n"
        f"{duckdb_ice_status}"
    )
    return duckdb_con, spark_con, iceberg_catalog, duckdb_ice_con, duckdb_status, spark_status, iceberg_status, duckdb_ice_status


@app.cell
def _(duckdb_con, spark_con, iceberg_catalog, duckdb_ice_con, pd):
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

    def query_pyiceberg(table_name, sql_filter=None):
        """Read an Iceberg table directly via PyIceberg. Returns a pandas DataFrame."""
        if iceberg_catalog is None:
            return pd.DataFrame({"error": ["PyIceberg not connected"]})
        try:
            table = iceberg_catalog.load_table(f"db.{table_name}")
            scan = table.scan()
            df = scan.to_pandas()
            return df
        except Exception as e:
            return pd.DataFrame({"error": [str(e)]})

    def query_duckdb_iceberg(metadata_path, sql=None):
        """Query Iceberg table via DuckDB iceberg extension using metadata path."""
        if duckdb_ice_con is None:
            return pd.DataFrame({"error": ["DuckDB+Iceberg not connected"]})
        try:
            if sql:
                return duckdb_ice_con.execute(sql).fetchdf()
            return duckdb_ice_con.execute(
                f"SELECT * FROM iceberg_scan('{metadata_path}')"
            ).fetchdf()
        except Exception as e:
            return pd.DataFrame({"error": [str(e)]})

    return query_duckdb, query_spark, query_pyiceberg, query_duckdb_iceberg


@app.cell
def _(mo):
    mo.md("## Customer Orders Mart — All Engines")
    return


@app.cell
def _(mo, query_duckdb, query_spark, query_pyiceberg):
    duck_mart = query_duckdb(
        "SELECT * FROM customer_orders ORDER BY total_revenue DESC"
    )
    spark_mart = query_spark(
        "SELECT * FROM customer_orders ORDER BY total_revenue DESC"
    )
    iceberg_mart = query_pyiceberg("customer_orders")
    if "error" not in iceberg_mart.columns and "total_revenue" in iceberg_mart.columns:
        iceberg_mart = iceberg_mart.sort_values("total_revenue", ascending=False)

    mo.hstack(
        [
            mo.vstack([
                mo.md("### 🦆 DuckDB (local)"),
                mo.ui.table(duck_mart),
            ]),
            mo.vstack([
                mo.md("### ⚡ Spark (Thrift)"),
                mo.ui.table(spark_mart),
            ]),
            mo.vstack([
                mo.md("### 🧊 PyIceberg (direct)"),
                mo.ui.table(iceberg_mart),
            ]),
        ],
        widths="equal",
    )
    return duck_mart, spark_mart, iceberg_mart


@app.cell
def _(mo):
    mo.md("## Revenue by Customer Tier")
    return


@app.cell
def _(mo, query_duckdb, query_spark, query_pyiceberg, pd):
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
    duck_tier = query_duckdb(tier_sql)
    spark_tier = query_spark(tier_sql)

    # PyIceberg: read full table then aggregate in pandas
    ice_raw = query_pyiceberg("customer_orders")
    if "error" not in ice_raw.columns and "customer_tier" in ice_raw.columns:
        ice_tier = (
            ice_raw.groupby("customer_tier")
            .agg(
                customer_count=("customer_tier", "count"),
                total_orders=("total_orders", "sum"),
                total_revenue=("total_revenue", "sum"),
                avg_revenue_per_customer=("total_revenue", "mean"),
            )
            .reset_index()
            .sort_values("total_revenue", ascending=False)
        )
        ice_tier["total_revenue"] = ice_tier["total_revenue"].round(2)
        ice_tier["avg_revenue_per_customer"] = ice_tier["avg_revenue_per_customer"].round(2)
    else:
        ice_tier = ice_raw

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
            mo.vstack([
                mo.md("### 🧊 PyIceberg"),
                mo.ui.table(ice_tier),
            ]),
        ],
        widths="equal",
    )
    return duck_tier, spark_tier, ice_tier


@app.cell
def _(mo, duck_tier, spark_tier, ice_tier):
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
                .properties(width=250, height=250, title=title)
            )

        combined = alt.hconcat(
            make_tier_chart(duck_tier, "🦆 DuckDB"),
            make_tier_chart(spark_tier, "⚡ Spark"),
            make_tier_chart(ice_tier, "🧊 PyIceberg"),
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

        Verify all engines produce identical results.
        """
    )
    return


@app.cell
def _(duck_mart, spark_mart, iceberg_mart, mo):
    def compare_dataframes(df1, df2, name1, name2):
        """Compare two DataFrames, return (match: bool, details: str)."""
        if "error" in df1.columns or "error" in df2.columns:
            return False, f"⚠️ One or both engines returned an error"

        # Normalise columns
        d1 = df1.copy()
        d2 = df2.copy()
        d1.columns = [c.split(".")[-1] for c in d1.columns]
        d2.columns = [c.split(".")[-1] for c in d2.columns]

        if "customer_id" in d1.columns and "customer_id" in d2.columns:
            d1 = d1.sort_values("customer_id").reset_index(drop=True)
            d2 = d2.sort_values("customer_id").reset_index(drop=True)

            mismatches = []
            for col in d1.columns:
                if col in d2.columns:
                    try:
                        if d1[col].astype(str).tolist() != d2[col].astype(str).tolist():
                            mismatches.append(col)
                    except Exception:
                        pass

            if not mismatches:
                return True, f"✅ **{name1} ↔ {name2}**: identical"
            else:
                return False, f"⚠️ **{name1} ↔ {name2}**: differences in {', '.join(mismatches)}"
        else:
            return False, f"⚠️ **{name1} ↔ {name2}**: cannot compare (different structure)"

    results = []
    pairs = [
        (duck_mart, spark_mart, "DuckDB", "Spark"),
        (duck_mart, iceberg_mart, "DuckDB", "PyIceberg"),
        (spark_mart, iceberg_mart, "Spark", "PyIceberg"),
    ]
    for df1, df2, n1, n2 in pairs:
        _, detail = compare_dataframes(df1, df2, n1, n2)
        results.append(detail)

    mo.md("\n\n".join(results))
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## DuckDB + Iceberg Extension

        Read Iceberg tables directly from S3 using DuckDB's `iceberg_scan()`.
        This bypasses Spark entirely — DuckDB reads the Iceberg metadata and Parquet files from S3.

        > **Note:** This requires knowing the metadata file path. The Nessie catalog stores this,
        > and we resolve it via PyIceberg below.
        """
    )
    return


@app.cell
def _(mo, iceberg_catalog, duckdb_ice_con, pd):
    if iceberg_catalog is None:
        mo.md("❌ PyIceberg not available — cannot resolve metadata path for DuckDB+Iceberg")
    elif duckdb_ice_con is None:
        mo.md("❌ DuckDB+Iceberg not available")
    else:
        try:
            # Resolve the Iceberg metadata location via PyIceberg
            table = iceberg_catalog.load_table("db.customer_orders")
            metadata_location = table.metadata_location
            mo.md(f"📍 Metadata location: `{metadata_location}`")

            # Convert s3a:// to s3:// for DuckDB
            metadata_path = metadata_location.replace("s3a://", "s3://")

            result = duckdb_ice_con.execute(
                f"SELECT * FROM iceberg_scan('{metadata_path}') ORDER BY total_revenue DESC"
            ).fetchdf()

            mo.vstack([
                mo.md("### 🦆🧊 DuckDB + Iceberg Extension (direct S3 read)"),
                mo.ui.table(result),
            ])
        except Exception as e:
            mo.md(f"❌ DuckDB+Iceberg query failed: `{e}`")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Custom Query

        Write SQL and run it against DuckDB and Spark simultaneously.
        *(PyIceberg uses scan API, not SQL — custom queries are engine-specific)*
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
def _(mo, query_duckdb, query_spark, query_input):
    if query_input.value.strip():
        duck_custom = query_duckdb(query_input.value)
        spark_custom = query_spark(query_input.value)

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
