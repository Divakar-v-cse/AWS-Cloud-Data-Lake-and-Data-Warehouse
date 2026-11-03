import sys
import psycopg

CONN = "host=localhost port=5433 dbname=analytics user=deuser password=depass"

SQL = """
select table_schema || '.' || table_name as t
from information_schema.tables
where table_schema in ('dwh','staging','marts','ops')
order by 1
"""

with psycopg.connect(CONN) as c:
    cur = c.cursor()
    cur.execute(SQL)
    rows = [r[0] for r in cur.fetchall()]
    for r in rows:
        print(" ", r)

    print("\n-- row counts --")
    for t in ["dwh.dim_customer", "dwh.dim_product", "dwh.dim_date",
              "dwh.dim_payment_method", "dwh.fact_orders", "dwh.fact_order_items",
              "dwh.fact_payments", "dwh.fact_shipments", "dwh.fact_returns"]:
        try:
            cur.execute(f"select count(*) from {t}")
            print(f"  {t:28s} {cur.fetchone()[0]:>8,}")
        except Exception as exc:
            c.rollback()
            print(f"  {t:28s} ERR {type(exc).__name__}")

    print("\n-- recent runs --")
    try:
        cur.execute("""
            select run_id, pipeline, stage, source, status,
                   coalesce(duration_sec,0), coalesce(records_out,0),
                   left(coalesce(error_message,''), 120)
            from ops.pipeline_runs
            order by run_id desc limit 12
        """)
        for r in cur.fetchall():
            print("  ", r)
    except Exception as exc:
        c.rollback()
        print("  ERR", exc)
