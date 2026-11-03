import psycopg

CONN = "host=localhost port=5433 dbname=analytics user=deuser password=depass"

with psycopg.connect(CONN) as c:
    cur = c.cursor()
    for t, k in [("dim_customer", "customer_id"), ("dim_product", "product_id")]:
        cur.execute(f"select count(*), count(distinct {k}), "
                    f"sum(case when is_current then 1 else 0 end) from dwh.{t}")
        print(t, "total/distinct/current:", cur.fetchone())

    cur.execute("select column_name from information_schema.columns "
                "where table_schema='dwh' and table_name='dim_customer' order by ordinal_position")
    print("customer cols:", [r[0] for r in cur.fetchall()])

    cur.execute("""
        select customer_id, customer_sk, city, loyalty_tier, email,
               valid_from, valid_to, is_current, hash_diff
        from dwh.dim_customer
        where customer_id = (select customer_id from dwh.dim_customer group by 1 having count(*)>1 limit 1)
        order by customer_sk
    """)
    for r in cur.fetchall():
        print(r)
