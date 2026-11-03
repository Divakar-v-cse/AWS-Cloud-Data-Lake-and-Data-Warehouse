import psycopg

CONN = "host=localhost port=5433 dbname=analytics user=deuser password=depass"

with psycopg.connect(CONN, connect_timeout=5) as c:
    cur = c.cursor()
    cur.execute("""
        select pid, state, wait_event_type, wait_event,
               round(extract(epoch from (now() - query_start))::numeric, 0) as secs,
               left(regexp_replace(query, '[[:space:]]+', ' ', 'g'), 100) as q
        from pg_stat_activity
        where datname = 'analytics'
        order by query_start
    """)
    for r in cur.fetchall():
        print(r)
