"""Drop the warehouse/staging/marts schemas for a clean rebuild.

Kept as a script (not a raw psql call) because the host has no psql client.
ops.* run history is preserved on purpose: it is the audit trail.
"""
import psycopg

CONN = "host=localhost port=5433 dbname=analytics user=deuser password=depass"

with psycopg.connect(CONN, autocommit=True) as c:
    cur = c.cursor()
    for schema in ("dwh", "staging", "marts"):
        cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        print("dropped", schema)
    # Any run interrupted by a restart would otherwise stay 'running' forever.
    cur.execute("""
        UPDATE ops.pipeline_runs
        SET status='failed', finished_at=now(),
            error_message='interrupted by operator restart'
        WHERE status='running'
    """)
    print("stale running rows closed:", cur.rowcount)
