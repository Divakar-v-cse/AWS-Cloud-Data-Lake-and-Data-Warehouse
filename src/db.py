"""Warehouse connectivity (PostgreSQL locally, Redshift in AWS).

Redshift speaks the PostgreSQL wire protocol, so the same psycopg driver,
DDL and SQL work against both. Only the connection settings differ.
"""
from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine, text

from .config import settings

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        kwargs: dict = {}
        if settings.aws_mode and settings.warehouse_driver == "psycopg":
            # psycopg3 uses the extended protocol with prepared statements by
            # default, which Redshift does not support. None disables it.
            kwargs["connect_args"] = {"prepare_threshold": None}
        _engine = create_engine(
            settings.dsn,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=5,
            future=True,
            **kwargs,
        )
    return _engine


@contextmanager
def connection():
    eng = get_engine()
    with eng.begin() as conn:
        yield conn


def query_df(sql: str, params: dict | None = None):
    """Run SQL and return a pandas DataFrame (parameterised, no string concat)."""
    import pandas as pd

    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def execute(sql: str, params: dict | None = None) -> None:
    with connection() as conn:
        conn.execute(text(sql), params or {})


def wait_for_warehouse(timeout: int = 60) -> bool:
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with get_engine().connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            time.sleep(1.5)
    return False


OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    pipeline        TEXT        NOT NULL,
    stage           TEXT        NOT NULL,
    source          TEXT,
    status          TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    duration_sec    NUMERIC(10,2),
    records_in      BIGINT      DEFAULT 0,
    records_out     BIGINT      DEFAULT 0,
    records_rejected BIGINT     DEFAULT 0,
    bytes_written   BIGINT      DEFAULT 0,
    details         JSONB,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_started
    ON ops.pipeline_runs (started_at DESC);

CREATE TABLE IF NOT EXISTS ops.data_quality_results (
    result_id       BIGSERIAL PRIMARY KEY,
    run_id          BIGINT      REFERENCES ops.pipeline_runs(run_id) ON DELETE CASCADE,
    dataset         TEXT        NOT NULL,
    check_name      TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    failed_rows     BIGINT      DEFAULT 0,
    total_rows      BIGINT      DEFAULT 0,
    details         JSONB,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.watermarks (
    dataset         TEXT PRIMARY KEY,
    last_value      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def ensure_ops_schema() -> None:
    with connection() as conn:
        for stmt in [s.strip() for s in OPS_DDL.split(";") if s.strip()]:
            conn.execute(text(stmt))
