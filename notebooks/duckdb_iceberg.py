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
        # 🦆🧊 DuckDB + Iceberg Extension

        DuckDB reads Iceberg tables directly from S3 using `iceberg_scan()`.
        **No JVM, no Spark** — DuckDB parses the Iceberg metadata JSON and reads
        the underlying Parquet files itself.

        **Path:** `DuckDB → iceberg_scan() → S3 (metadata.json + Parquet files)`

        We use PyIceberg to resolve the metadata file location from the Nessie catalog,
        then hand that path to DuckDB. This gives you full SQL on Iceberg tables
        without any Java infrastructure.
        """
    )
    return


@app.cell
def _(mo, os):
    # ── Set up PyIceberg catalog for metadata resolution ──
    catalog = None
    catalog_status = ""
    try:
        from pyiceberg.catalog import load_catalog

        nessie_uri = os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg/")
        s3_endpoint = os.environ.get("S3_ENDPOINT", "http://localstack:4566")

        catalog = load_catalog(
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
        catalog_status = f"✅ PyIceberg catalog connected (metadata resolver)"
    except Exception as e:
        catalog_status = f"❌ PyIceberg: `{e}`"

    # ── Set up DuckDB with Iceberg + S3 ──
    duckdb_con = None
    duckdb_status = ""
    try:
        import duckdb

        duckdb_con = duckdb.connect()
        duckdb_con.execute("INSTALL iceberg; LOAD iceberg;")
        duckdb_con.execute("INSTALL httpfs; LOAD httpfs;")
        s3_ep = os.environ.get("S3_ENDPOINT", "http://localstack:4566")
        duckdb_con.execute(f"""
            SET s3_endpoint='{s3_ep.replace("http://", "")}';
            SET s3_access_key_id='test';
            SET s3_secret_access_key='test';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            SET s3_region='eu-west-2';
        """)
        duckdb_status = f"✅ DuckDB ready with Iceberg + S3 extensions"
    except Exception as e:
        duckdb_status = f"❌ DuckDB: `{e}`"

    mo.md(f"{catalog_status}\n\n{duckdb_status}")
    return catalog, duckdb_con


@app.cell
def _(catalog):
    def resolve_metadata(table_name):
        """Resolve Iceberg metadata location from Nessie catalog."""
        if catalog is None:
            return None
        table = catalog.load_table(table_name)
        # DuckDB needs s3:// not s3a://
        return table.metadata_location.replace("s3a://", "s3://")

    return (resolve_metadata,)


@app.cell
def _(duckdb_con, pd, resolve_metadata):
    def iceberg_query(table_name, sql_transform=None):
        """Query an Iceberg table via DuckDB iceberg_scan.

        Args:
            table_name: Fully qualified name (e.g. 'db.customer_orders')
            sql_transform: Optional SQL to wrap the scan, use {table} as placeholder.
                          e.g. "SELECT * FROM {table} WHERE total_orders >= 2"
        """
        if duckdb_con is None:
            return pd.DataFrame({"error": ["DuckDB not connected"]})

        metadata_path = resolve_metadata(table_name)
        if metadata_path is None:
            return pd.DataFrame({"error": ["Could not resolve metadata path"]})

        try:
            scan_expr = f"iceberg_scan('{metadata_path}')"
            if sql_transform:
                sql = sql_transform.replace("{table}", scan_expr)
            else:
                sql = f"SELECT * FROM {scan_expr}"
            return duckdb_con.execute(sql).fetchdf()
        except Exception as e:
            return pd.DataFrame({"error": [str(e)]})

    return (iceberg_query,)


@app.cell
def _(mo):
    mo.md("## Metadata Resolution")
    return


@app.cell
def _(catalog, mo, pd):
    if catalog:
        rows = []
        for ns in catalog.list_namespaces():
            for table_id in catalog.list_tables(ns):
                full_name = f"{table_id[0]}.{table_id[1]}"
                table = catalog.load_table(full_name)
                metadata = table.metadata_location.replace("s3a://", "s3://")
                rows.append({
                    "table": full_name,
                    "metadata_location": metadata,
                    "format_version": table.metadata.format_version,
                    "snapshot_count": len(table.metadata.snapshots),
                })
        mo.vstack([
            mo.md("PyIceberg resolves the metadata file path from Nessie, then DuckDB reads it directly from S3:"),
            mo.ui.table(pd.DataFrame(rows)),
        ])
    return


@app.cell
def _(mo):
    mo.md("## Customer Orders — via `iceberg_scan()`")
    return


@app.cell
def _(iceberg_query, mo):
    data = iceberg_query(
        "db.customer_orders",
        "SELECT * FROM {table} ORDER BY total_revenue DESC",
    )
    mo.ui.table(data)
    return (data,)


@app.cell
def _(mo):
    mo.md("## Revenue by Tier — SQL Aggregation in DuckDB")
    return


@app.cell
def _(iceberg_query, mo):
    tier_data = iceberg_query(
        "db.customer_orders",
        """
        SELECT
            customer_tier,
            COUNT(*) as customer_count,
            SUM(total_orders) as total_orders,
            ROUND(SUM(total_revenue), 2) as total_revenue,
            ROUND(AVG(total_revenue), 2) as avg_revenue
        FROM {table}
        GROUP BY customer_tier
        ORDER BY total_revenue DESC
        """,
    )
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
            .properties(width=500, height=300, title="Revenue by Customer Tier (DuckDB+Iceberg)")
        )
        mo.ui.altair_chart(chart)
    except ImportError:
        mo.md("_Install altair for charts_")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Iceberg Metadata — via DuckDB

        DuckDB can also read Iceberg metadata files directly.
        """
    )
    return


@app.cell
def _(duckdb_con, mo, resolve_metadata):
    metadata_path = resolve_metadata("db.customer_orders")
    if metadata_path and duckdb_con:
        try:
            meta = duckdb_con.execute(
                f"SELECT * FROM iceberg_metadata('{metadata_path}')"
            ).fetchdf()
            mo.vstack([
                mo.md("### Iceberg Metadata (data files)"),
                mo.ui.table(meta),
            ])
        except Exception as e:
            mo.md(f"_Metadata query failed: {e}_")
    return


@app.cell
def _(duckdb_con, mo, resolve_metadata):
    metadata_path2 = resolve_metadata("db.customer_orders")
    if metadata_path2 and duckdb_con:
        try:
            snaps = duckdb_con.execute(
                f"SELECT * FROM iceberg_snapshots('{metadata_path2}')"
            ).fetchdf()
            mo.vstack([
                mo.md("### Iceberg Snapshots"),
                mo.ui.table(snaps),
            ])
        except Exception as e:
            mo.md(f"_Snapshots query failed: {e}_")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Custom Query

        Write SQL against Iceberg tables. Use `{table}` as placeholder for the `iceberg_scan()`.
        """
    )
    return


@app.cell
def _(mo):
    sql_input = mo.ui.code_editor(
        value="SELECT customer_tier, COUNT(*) as n FROM {table} GROUP BY customer_tier",
        language="sql",
        min_height=80,
    )
    sql_input
    return (sql_input,)


@app.cell
def _(iceberg_query, mo, sql_input):
    if sql_input.value.strip():
        result = iceberg_query("db.customer_orders", sql_input.value)
        mo.ui.table(result)
    return


if __name__ == "__main__":
    app.run()
