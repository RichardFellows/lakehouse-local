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
        # 🧊 PyIceberg — Direct Table Access

        Reads Iceberg tables directly from S3 using the Nessie Iceberg REST catalog.
        **No JVM, no Spark, no Thrift Server** — pure Python reading Parquet files.

        **Path:** `PyIceberg → Nessie REST Catalog → S3 (Parquet files)`

        PyIceberg uses Apache Arrow under the hood, so reads are fast and memory-efficient.
        The trade-off: no SQL engine — you get scan/filter/select at the Iceberg level,
        then process in pandas or Arrow.
        """
    )
    return


@app.cell
def _(mo, os):
    catalog = None
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
        namespaces = catalog.list_namespaces()
        mo.md(
            f"✅ PyIceberg connected to Nessie REST Catalog\n\n"
            f"- URI: `{nessie_uri}`\n"
            f"- Namespaces: {['.'.join(n) for n in namespaces]}"
        )
    except Exception as e:
        mo.md(f"❌ Connection failed: `{e}`")
    return (catalog,)


@app.cell
def _(mo):
    mo.md("## Catalog Contents")
    return


@app.cell
def _(catalog, mo, pd):
    if catalog:
        rows = []
        for ns in catalog.list_namespaces():
            for table_id in catalog.list_tables(ns):
                rows.append({"namespace": ".".join(ns), "table": table_id[1]})
        table_list = pd.DataFrame(rows)
        mo.ui.table(table_list)
    else:
        mo.md("_Not connected_")
    return


@app.cell
def _(mo):
    mo.md("## Table Metadata — `db.customer_orders`")
    return


@app.cell
def _(catalog, mo):
    if catalog:
        try:
            table = catalog.load_table("db.customer_orders")
            schema_info = []
            for field in table.schema().fields:
                schema_info.append({
                    "field_id": field.field_id,
                    "name": field.name,
                    "type": str(field.field_type),
                    "required": field.required,
                })

            import pandas as _pd
            schema_df = _pd.DataFrame(schema_info)

            mo.vstack([
                mo.md(f"**Location:** `{table.metadata_location}`"),
                mo.md(f"**Format version:** {table.metadata.format_version}"),
                mo.md(f"**Current snapshot:** `{table.metadata.current_snapshot_id}`"),
                mo.md("### Schema"),
                mo.ui.table(schema_df),
            ])
        except Exception as e:
            mo.md(f"_Error loading table: {e}_")
    return (table,)


@app.cell
def _(mo):
    mo.md(
        """
        ## Snapshots & History

        Iceberg maintains a full history of table snapshots. Each snapshot
        represents a consistent view of the table at a point in time.
        """
    )
    return


@app.cell
def _(mo, pd, table):
    if table:
        try:
            snapshots = []
            for snap in table.metadata.snapshots:
                snapshots.append({
                    "snapshot_id": snap.snapshot_id,
                    "timestamp_ms": snap.timestamp_ms,
                    "operation": snap.summary.operation if snap.summary else "unknown",
                    "added_files": snap.summary.get("added-data-files-count", "?") if snap.summary else "?",
                    "added_records": snap.summary.get("added-records-count", "?") if snap.summary else "?",
                    "manifest_list": snap.manifest_list,
                })
            snap_df = pd.DataFrame(snapshots)
            mo.vstack([
                mo.md("### Snapshots"),
                mo.ui.table(snap_df),
            ])
        except Exception as e:
            mo.md(f"_Error reading snapshots: {e}_")
    return


@app.cell
def _(mo):
    mo.md("## Scan Data")
    return


@app.cell
def _(catalog, mo, pd):
    if catalog:
        try:
            tbl = catalog.load_table("db.customer_orders")
            # Full scan → pandas
            df = tbl.scan().to_pandas()
            df = df.sort_values("total_revenue", ascending=False)
            mo.vstack([
                mo.md(f"**{len(df)} rows** scanned directly from Parquet on S3"),
                mo.ui.table(df),
            ])
        except Exception as e:
            mo.md(f"_Scan failed: {e}_")
    return (df,)


@app.cell
def _(mo):
    mo.md(
        """
        ## Filtered Scan

        PyIceberg supports predicate pushdown — filters are applied at the Iceberg level,
        so only matching data files are read from S3.
        """
    )
    return


@app.cell
def _(catalog, mo):
    if catalog:
        try:
            from pyiceberg.expressions import GreaterThanOrEqual

            tbl = catalog.load_table("db.customer_orders")
            filtered = tbl.scan(
                row_filter=GreaterThanOrEqual("total_orders", 2),
                selected_fields=("customer_id", "first_name", "last_name", "total_orders", "total_revenue", "customer_tier"),
            ).to_pandas()
            mo.vstack([
                mo.md(f"**Filter:** `total_orders >= 2` | **Selected fields:** 6 of 9 | **Rows:** {len(filtered)}"),
                mo.ui.table(filtered),
            ])
        except Exception as e:
            mo.md(f"_Filtered scan failed: {e}_")
    return


@app.cell
def _(mo):
    mo.md("## Arrow Integration")
    return


@app.cell
def _(catalog, mo):
    if catalog:
        try:
            tbl = catalog.load_table("db.customer_orders")
            arrow_table = tbl.scan().to_arrow()
            mo.vstack([
                mo.md(
                    f"**Arrow table:** {arrow_table.num_rows} rows × {arrow_table.num_columns} columns\n\n"
                    f"**Memory:** {arrow_table.nbytes:,} bytes\n\n"
                    f"**Schema:**\n```\n{arrow_table.schema}\n```"
                ),
            ])
        except Exception as e:
            mo.md(f"_Arrow conversion failed: {e}_")
    return


@app.cell
def _(mo):
    mo.md(
        """
        ## Pandas Aggregation

        Since PyIceberg doesn't have a SQL engine, we aggregate in pandas.
        The data is already in memory from the scan — this is just transformation.
        """
    )
    return


@app.cell
def _(df, mo):
    if df is not None and "error" not in df.columns:
        tier_summary = (
            df.groupby("customer_tier")
            .agg(
                customer_count=("customer_tier", "count"),
                total_orders=("total_orders", "sum"),
                total_revenue=("total_revenue", "sum"),
                avg_revenue=("total_revenue", "mean"),
            )
            .reset_index()
            .sort_values("total_revenue", ascending=False)
        )
        tier_summary["total_revenue"] = tier_summary["total_revenue"].round(2)
        tier_summary["avg_revenue"] = tier_summary["avg_revenue"].round(2)

        mo.ui.table(tier_summary)
    return (tier_summary,)


@app.cell
def _(mo, tier_summary):
    try:
        import altair as alt

        chart = (
            alt.Chart(tier_summary)
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
            .properties(width=500, height=300, title="Revenue by Customer Tier (PyIceberg → pandas)")
        )
        mo.ui.altair_chart(chart)
    except ImportError:
        mo.md("_Install altair for charts_")
    return


if __name__ == "__main__":
    app.run()
