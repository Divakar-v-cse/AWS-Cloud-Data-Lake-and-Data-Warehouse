"""AWS Glue (PySpark) job: silver -> gold.

Managed-AWS twin of ``src/gold.py``. Same nine business datasets plus the four
supporting frames, same column names and the same metric definitions, so the
Redshift tables, the dbt models and the React dashboard keep working unchanged.

Definitions (identical to src/gold.py)
    gross_revenue    sum(order_total) where status in (completed, returned)
    refunds          sum(refund_amount) where returns.status in (completed, approved)
    net_revenue      gross_revenue - refunds
    aov              net_revenue / shipped_orders
    active customer  customer with >= 1 shipped order in the period

Layout difference from the local run, on purpose
    Local (pandas + pyarrow) writes one object per dataset:
        s3://gold/daily_sales/daily_sales.parquet
    Spark writes a directory of part files:
        s3://gold/daily_sales/part-00000-....snappy.parquet
    Both are valid Parquet prefixes. The Glue crawler and ``COPY ... FROM``
    address the directory, not the file, so nothing downstream cares.

Job parameters
    --silver_bucket   s3://divakar-lake-silver-dev-<acct>-<region>
    --gold_bucket     s3://divakar-lake-gold-dev-<acct>-<region>

Status: deployable artifact. Not executed here - no AWS credentials exist in
this environment, so nothing in the repository claims this job has ever run.
"""
import json
import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window, functions as F

SHIPPED = ("completed", "returned")
APPROVED_RETURN = ("completed", "approved")

# (dataset, partition columns) - mirrors the catalog in src/gold.py
DEFINITIONS = {
    "daily_sales": "Daily order, revenue and refund metrics",
    "monthly_revenue": "Monthly revenue with growth and AOV",
    "product_performance": "Per-product revenue, profit, returns",
    "customer_analytics": "Customer 360 with RFM, CLV, segmentation",
    "regional_sales": "Revenue by region and state",
    "return_analytics": "Returns by category and reason",
    "order_analytics": "Order status distribution",
    "fulfilment": "Carrier performance and processing time",
    "cohort_retention": "Monthly cohort retention matrix",
    "payment_analytics": "Payment success by method",
    "payment_analytics_by_status": "Payment totals by status",
    "payment_analytics_by_method_status": "Payment totals by method and status",
    "return_analytics_monthly": "Returns and refunds by month",
    "return_analytics_by_status": "Returns and refunds by status",
}


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def load(spark, silver: str) -> dict:
    """Read every silver dataset and add the derived date columns gold needs.

    ``src/silver.py`` derives order_date/order_month while cleaning; the Glue
    job only guarantees ingest_date partitioning, so derive them here instead of
    assuming they exist. ``F.to_date`` on a timestamp truncates to midnight,
    which is what ``dt.normalize()`` does locally.
    """
    def read(source):
        return spark.read.parquet(f"{silver}/{source}/")

    t = {s: read(s) for s in
         ("customers", "products", "orders", "order_items", "payments", "shipments", "returns")}

    t["orders"] = (t["orders"]
                   .withColumn("order_date", F.to_date("order_ts"))
                   .withColumn("order_month", F.date_format("order_ts", "yyyy-MM")))

    # child tables inherit the order's month so monthly facts line up exactly;
    # orphan rows get 'unknown' rather than being dropped.
    months = t["orders"].select("order_id", "order_month")
    for source in ("order_items", "payments", "shipments", "returns"):
        if "order_month" not in t[source].columns:
            t[source] = (t[source]
                         .join(months, on="order_id", how="left")
                         .withColumn("order_month", F.coalesce(F.col("order_month"), F.lit("unknown"))))
    return t


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------
def build_daily_sales(t: dict):
    refunds = (t["returns"].filter(F.col("status").isin(APPROVED_RETURN))
               .groupBy("order_id").agg(F.sum("refund_amount").alias("refund")))
    o = (t["orders"]
         .join(refunds, on="order_id", how="left")
         .withColumn("refund", F.coalesce(F.col("refund"), F.lit(0.0)))
         .withColumn("is_shipped", F.col("status").isin(SHIPPED)))

    g = o.groupBy("order_date").agg(
        F.count("order_id").alias("orders"),
        F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_orders"),
        F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancelled_orders"),
        F.sum(F.when(F.col("status") == "returned", 1).otherwise(0)).alias("returned_orders"),
        F.sum(F.when(F.col("status") == "pending", 1).otherwise(0)).alias("pending_orders"),
        F.sum("item_count").alias("units_sold"),
        F.sum("refund").alias("refunds"),
        F.countDistinct("customer_id").alias("active_customers"),
    )
    shipped = (o.filter("is_shipped")
               .groupBy("order_date").agg(
                   F.count("order_id").alias("shipped_orders"),
                   F.sum("order_total").alias("gross_revenue")))

    g = (g.join(shipped, on="order_date", how="left")
         .fillna({"shipped_orders": 0, "gross_revenue": 0.0})
         .withColumn("net_revenue", F.round(F.col("gross_revenue") - F.col("refunds"), 2))
         .withColumn("aov", F.when(F.col("shipped_orders") > 0,
                                   F.round(F.col("net_revenue") / F.col("shipped_orders"), 2))
                     .otherwise(F.lit(0.0)))
         .withColumn("year", F.year("order_date")))

    w = Window.orderBy("order_date").rowsBetween(-6, 0)
    return (g.withColumn("revenue_7d_ma", F.round(F.avg("net_revenue").over(w), 2))
            .select("order_date", "year", "orders", "completed_orders", "cancelled_orders",
                    "returned_orders", "pending_orders", "shipped_orders", "units_sold",
                    "gross_revenue", "refunds", "net_revenue", "aov", "active_customers",
                    "revenue_7d_ma")
            .orderBy("order_date"))


def build_monthly_revenue(t: dict):
    refunds = (t["returns"].filter(F.col("status").isin(APPROVED_RETURN))
               .groupBy("order_id").agg(F.sum("refund_amount").alias("refund")))
    o = (t["orders"]
         .join(refunds, on="order_id", how="left")
         .withColumn("refund", F.coalesce(F.col("refund"), F.lit(0.0)))
         .withColumn("is_shipped", F.col("status").isin(SHIPPED)))

    m = o.groupBy("order_month").agg(
        F.count("order_id").alias("orders"),
        F.sum("refund").alias("refunds"),
        F.countDistinct("customer_id").alias("customers"))
    shipped = (o.filter("is_shipped")
               .groupBy("order_month").agg(
                   F.count("order_id").alias("shipped_orders"),
                   F.sum("order_total").alias("gross_revenue")))

    m = (m.join(shipped, on="order_month", how="left")
         .fillna({"shipped_orders": 0, "gross_revenue": 0.0})
         .withColumn("net_revenue", F.round(F.col("gross_revenue") - F.col("refunds"), 2))
         .withColumn("aov", F.when(F.col("shipped_orders") > 0,
                                   F.round(F.col("net_revenue") / F.col("shipped_orders"), 2))
                     .otherwise(F.lit(0.0))))

    w = Window.orderBy("order_month")
    return (m.withColumn("prev", F.lag("net_revenue").over(w))
            .withColumn("revenue_growth_pct",
                        F.when(F.col("prev").isNull() | (F.col("prev") == 0), F.lit(0.0))
                        .otherwise(F.round((F.col("net_revenue") - F.col("prev"))
                                           / F.col("prev") * 100, 2)))
            .withColumn("year", F.substring("order_month", 1, 4).cast("int"))
            .select("order_month", "year", "orders", "shipped_orders", "customers",
                    "gross_revenue", "refunds", "net_revenue", "aov", "revenue_growth_pct")
            .orderBy("order_month"))


def build_product_performance(t: dict):
    oi = (t["order_items"]
          .join(t["orders"].select("order_id", "status"), on="order_id", how="left")
          .filter(F.col("status").isin(SHIPPED)))

    prod = t["products"].select("product_id", "product_name", "category", "subcategory",
                                "brand", "unit_price", "unit_cost")
    p = (oi.join(prod, on="product_id", how="left")
         .withColumn("revenue", F.col("line_total"))
         .withColumn("profit", F.round(F.col("line_total") - F.col("unit_cost") * F.col("quantity"), 2)))

    ret = (t["returns"].filter(F.col("status").isin(APPROVED_RETURN))
           .groupBy("product_id").agg(
               F.sum("quantity").alias("returned_units"),
               F.sum("refund_amount").alias("refund_amount")))

    g = (p.groupBy("product_id", "product_name", "category", "subcategory", "brand").agg(
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("orders"),
        F.sum("revenue").alias("revenue"),
        F.sum("profit").alias("profit"),
        F.avg("unit_price").alias("avg_selling_price"))
        .join(ret, on="product_id", how="left")
        .fillna({"returned_units": 0, "refund_amount": 0.0}))

    w = Window.orderBy(F.col("revenue").desc())
    return (g.withColumn("return_rate", F.when(F.col("units_sold") > 0,
                                               F.round(F.col("returned_units") / F.col("units_sold"), 4))
                         .otherwise(F.lit(0.0)))
            .withColumn("revenue", F.round("revenue", 2))
            .withColumn("profit", F.round("profit", 2))
            .withColumn("margin_pct", F.when(F.col("revenue") > 0,
                                             F.round(F.col("profit") / F.col("revenue") * 100, 2))
                        .otherwise(F.lit(0.0)))
            .withColumn("revenue_rank", F.dense_rank().over(w))
            .orderBy(F.col("revenue").desc()))


def build_customer_analytics(t: dict):
    shipped = t["orders"].filter(F.col("status").isin(SHIPPED))
    snapshot = shipped.agg(F.max("order_date").alias("d")).collect()[0]["d"]

    agg = shipped.groupBy("customer_id").agg(
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.count("order_id").alias("orders"),
        F.round(F.sum("order_total"), 2).alias("monetary"),
        F.round(F.avg("order_total"), 2).alias("avg_order_value"),
        F.sum("item_count").alias("total_items"))

    agg = (agg
           .withColumn("recency_days", F.datediff(F.lit(snapshot), F.col("last_order_date")))
           .withColumn("tenure_days", F.datediff("last_order_date", "first_order_date") + 1)
           .withColumn("is_repeat_customer", F.col("orders") > 1)
           # clipped at one day so a single-order customer does not divide by 365.25
           .withColumn("purchase_frequency_per_year",
                       F.round(F.col("orders")
                               / (F.greatest(F.col("tenure_days") / 365.25, F.lit(1 / 365.25))), 2))
           .withColumn("predicted_clv",
                       F.round(F.col("avg_order_value") * F.col("purchase_frequency_per_year") * 2, 2)))

    # RFM quintiles. ntile over recency ascending puts the freshest 20% in
    # bucket 1, so r_score inverts it; frequency and monetary ascend naturally.
    r_w = Window.orderBy(F.col("recency_days").asc())
    f_w = Window.orderBy(F.col("orders").asc())
    m_w = Window.orderBy(F.col("monetary").asc())
    agg = (agg
           .withColumn("r_score", 6 - F.ntile(5).over(r_w))
           .withColumn("f_score", F.ntile(5).over(f_w))
           .withColumn("m_score", F.ntile(5).over(m_w))
           .withColumn("rfm_score", F.col("r_score") * 100 + F.col("f_score") * 10 + F.col("m_score")))

    r, f, m = F.col("r_score"), F.col("f_score"), F.col("m_score")
    agg = agg.withColumn("segment", F.when((r >= 4) & (f >= 4) & (m >= 4), "Champions")
                         .when((r >= 3) & (f >= 3), "Loyal Customers")
                         .when((r >= 4) & (f <= 2), "New Customers")
                         .when((r >= 3) & (f <= 2), "Potential Loyalists")
                         .when((r <= 2) & (f >= 3), "At Risk")
                         .when((r <= 2) & (m >= 3), "Cant Lose Them")
                         .otherwise("Hibernating"))

    cust = t["customers"].select("customer_id", "first_name", "last_name", "email", "city",
                                 "state", "region", "signup_date", "loyalty_tier")
    return (agg.join(cust, on="customer_id", how="left")
            .withColumn("customer_name",
                        F.trim(F.concat_ws(" ", F.coalesce("first_name", F.lit("")),
                                           F.coalesce("last_name", F.lit("")))))
            .orderBy(F.col("monetary").desc()))


def build_regional_sales(t: dict):
    o = t["orders"].filter(F.col("status").isin(SHIPPED))
    g = o.groupBy("region", "state").agg(
        F.count("order_id").alias("orders"),
        F.countDistinct("customer_id").alias("customers"),
        F.sum("order_total").alias("revenue"))
    total = g.agg(F.sum("revenue").alias("t")).collect()[0]["t"] or 0.0
    return (g.withColumn("revenue", F.round("revenue", 2))
            .withColumn("aov", F.round(F.col("revenue") / F.col("orders"), 2))
            .withColumn("revenue_share_pct",
                        F.round(F.col("revenue") / F.lit(total) * 100, 2) if total else F.lit(0.0))
            .orderBy(F.col("revenue").desc()))


def build_payment_analytics(t: dict):
    p = t["payments"]
    by_method = p.groupBy("payment_method").agg(
        F.count("payment_id").alias("attempts"),
        F.sum("amount").alias("amount"),
        F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_count"),
        F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_count"),
        F.sum(F.when(F.col("status") == "refunded", 1).otherwise(0)).alias("refunded_count"),
        F.sum(F.when(F.col("status") == "pending", 1).otherwise(0)).alias("pending_count"))
    by_method = (by_method
                 .withColumn("success_rate",
                             F.round(F.col("success_count") / F.col("attempts") * 100, 2))
                 .withColumn("failure_rate",
                             F.round(F.col("failed_count") / F.col("attempts") * 100, 2))
                 .withColumn("amount", F.round("amount", 2)))

    by_method_status = (p.groupBy("payment_method", "status").agg(
        F.count("payment_id").alias("payments"),
        F.round(F.sum("amount"), 2).alias("amount")))

    by_status = (p.groupBy("status").agg(
        F.count("payment_id").alias("payments"),
        F.round(F.sum("amount"), 2).alias("amount"))
        .withColumn("method", F.lit("ALL")))

    return {"by_method": by_method,
            "by_method_status": by_method_status,
            "by_status": by_status}


def build_return_analytics(t: dict):
    r = t["returns"].join(t["products"].select("product_id", "category"), on="product_id", how="left")

    by_category = (r.groupBy("category", "reason").agg(
        F.count("return_id").alias("returns"),
        F.sum("quantity").alias("units"),
        F.round(F.sum("refund_amount"), 2).alias("refund_amount")))

    monthly = (r.groupBy("order_month").agg(
        F.count("return_id").alias("returns"),
        F.round(F.sum("refund_amount"), 2).alias("refund_amount")))

    by_status = (r.groupBy("status").agg(
        F.count("return_id").alias("returns"),
        F.round(F.sum("refund_amount"), 2).alias("refund_amount")))

    return {"by_category_reason": by_category, "monthly": monthly, "by_status": by_status}


def build_order_analytics(t: dict):
    o = t["orders"]
    total = o.count() or 1
    return (o.groupBy("status").agg(
        F.count("order_id").alias("orders"),
        F.round(F.sum("order_total"), 2).alias("revenue"))
        .withColumn("share_pct", F.round(F.col("orders") / F.lit(total) * 100, 2)))


def build_fulfilment(t: dict):
    s = (t["shipments"]
         .join(t["orders"].select("order_id", "order_ts", "status"), on="order_id", how="left")
         .filter(F.col("delivered_ts").isNotNull())
         .withColumn("processing_days",
                     (F.col("delivered_ts").cast("long") - F.col("order_ts").cast("long")) / 86400.0))
    g = s.groupBy("carrier").agg(
        F.count("shipment_id").alias("shipments"),
        F.round(F.avg("processing_days"), 2).alias("avg_processing_days"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.sum(F.col("is_late").cast("int")).alias("late_shipments"))
    return g.withColumn("late_rate", F.round(F.col("late_shipments") / F.col("shipments") * 100, 2))


def build_cohort_retention(t: dict):
    o = (t["orders"].filter(F.col("status").isin(SHIPPED))
         .withColumn("cohort_month", F.trunc("order_ts", "month")))
    w = Window.partitionBy("customer_id")
    o = o.withColumn("cohort", F.min("cohort_month").over(w))
    o = o.withColumn("month_index",
                     (F.year("cohort_month") - F.year("cohort")) * 12
                     + (F.month("cohort_month") - F.month("cohort")))
    sizes = o.groupBy("cohort").agg(F.countDistinct("customer_id").alias("cohort_size"))
    activity = (o.groupBy("cohort", "month_index").agg(F.countDistinct("customer_id").alias("customers"))
                .join(sizes, on="cohort", how="left")
                .withColumn("retention_pct", F.round(F.col("customers") / F.col("cohort_size") * 100, 2))
                .withColumn("cohort", F.date_format("cohort", "yyyy-MM")))
    return activity.orderBy("cohort", "month_index")


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------
def write_dataset(df, gold: str, name: str):
    """Overwrite the dataset prefix so a re-run never leaves stale part files."""
    df.write.mode("overwrite").option("compression", "snappy").parquet(f"{gold}/{name}/")
    return df.count(), len(df.columns)


def write_catalog(gold: str, summary: dict, kpis: dict):
    """Same object the local pipeline writes, so both environments agree.

    The crawler owns table definitions in the Data Catalog; this file is the
    lightweight contract the FastAPI layer reads when it runs against the lake
    directly instead of Redshift.
    """
    body = {
        "datasets": [
            {"dataset": name, "description": DEFINITIONS.get(name, name),
             "rows": stats["rows"], "columns": stats["columns"],
             "format": "PARQUET", "location": f"{gold}/{name}/",
             "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            for name, stats in summary.items()
        ],
        "kpis": kpis,
    }
    try:
        import boto3

        bucket, prefix = gold.replace("s3://", "").split("/", 1)
        boto3.client("s3").put_object(
            Bucket=bucket, Key=f"{prefix.rstrip('/')}/_catalog.json",
            Body=json.dumps(body, default=str, indent=2).encode("utf-8"),
            ContentType="application/json")
        print(f"[ok] catalog -> {gold}/_catalog.json")
    except Exception as exc:  # never fail the job over a metadata sidecar
        print(f"[warn] catalog not written: {exc}")


# Columns the Redshift staging tables expect, in DDL order. The export drops the
# Spark bookkeeping columns (_ingested_at, ingest_date) and the Hive partition
# path, so ``COPY ... FORMAT AS PARQUET`` into a table with the same column list
# is a straight positional match with nothing extra to map or ignore.
WAREHOUSE_EXPORT = {
    "customers": ["customer_id", "first_name", "last_name", "email", "phone", "city",
                  "state", "region", "country", "postal_code", "signup_date",
                  "loyalty_tier", "is_active", "updated_at"],
    "products": ["product_id", "sku", "product_name", "category", "subcategory", "brand",
                 "unit_cost", "unit_price", "weight_kg", "is_active", "launch_date",
                 "updated_at"],
    "orders": ["order_id", "customer_id", "order_ts", "status", "channel", "region",
               "state", "city", "item_count", "subtotal", "discount_amount",
               "shipping_cost", "tax_amount", "order_total", "updated_at"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price",
                    "unit_cost", "discount_pct", "line_subtotal", "line_total", "order_ts"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_method", "amount",
                 "status", "attempt_number"],
    "shipments": ["shipment_id", "order_id", "shipped_ts", "delivered_ts", "promised_date",
                  "carrier", "status", "shipping_cost", "delivery_days", "is_late"],
    "returns": ["return_id", "order_id", "order_item_id", "product_id", "return_ts",
                "reason", "quantity", "refund_amount", "status", "restocked"],
}


def export_warehouse(t: dict, gold: str) -> dict:
    """Flatten silver into s3://<gold>/warehouse/<source>/ for Redshift COPY.

    Silver is Hive-partitioned by ingest_date and carries two Spark-only
    columns. COPY from that layout would need every partition registered and an
    exact column mapping, which is fragile. One unpartitioned, column-pruned
    copy per source makes the load a plain positional match.
    """
    out = {}
    for source, cols in WAREHOUSE_EXPORT.items():
        missing = [c for c in cols if c not in t[source].columns]
        if missing:
            raise ValueError(f"{source}: silver is missing {missing}")
        df = t[source].select(*cols)
        df.write.mode("overwrite").option("compression", "snappy").parquet(
            f"{gold}/warehouse/{source}/")
        n = df.count()
        out[source] = n
        print(f"[ok] warehouse/{source}: {n:,} rows")
    return out


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "silver_bucket", "gold_bucket"])

    sc = SparkContext()
    glue = GlueContext(sc)
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    silver, gold = args["silver_bucket"].rstrip("/"), args["gold_bucket"].rstrip("/")

    t = load(spark, silver)
    pay = build_payment_analytics(t)
    ret = build_return_analytics(t)

    frames = {
        "daily_sales": build_daily_sales(t),
        "monthly_revenue": build_monthly_revenue(t),
        "product_performance": build_product_performance(t),
        "customer_analytics": build_customer_analytics(t),
        "regional_sales": build_regional_sales(t),
        "return_analytics": ret["by_category_reason"],
        "order_analytics": build_order_analytics(t),
        "fulfilment": build_fulfilment(t),
        "cohort_retention": build_cohort_retention(t),
        "payment_analytics": pay["by_method"],
        "payment_analytics_by_status": pay["by_status"],
        "payment_analytics_by_method_status": pay["by_method_status"],
        "return_analytics_monthly": ret["monthly"],
        "return_analytics_by_status": ret["by_status"],
    }

    summary, total_rows = {}, 0
    for name, df in frames.items():
        rows, cols = write_dataset(df, gold, name)
        total_rows += rows
        summary[name] = {"rows": rows, "columns": cols}
        print(f"[ok] {name}: {rows:,} rows, {cols} columns -> {gold}/{name}/")

    orders = t["orders"]
    kpi = orders.agg(F.count("order_id").alias("total_orders"),
                     F.countDistinct("customer_id").alias("total_customers"),
                     F.min("order_date").alias("first_day"),
                     F.max("order_date").alias("last_day")).collect()[0]
    kpis = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_orders": int(kpi["total_orders"]),
        "total_customers": int(kpi["total_customers"]),
        "total_products": int(t["products"].count()),
        "date_range": [str(kpi["first_day"]), str(kpi["last_day"])],
    }
    write_catalog(gold, summary, kpis)
    export_warehouse(t, gold)

    print(f"[done] {len(frames)} datasets, {total_rows:,} rows")
    job.commit()


if __name__ == "__main__":
    main()
