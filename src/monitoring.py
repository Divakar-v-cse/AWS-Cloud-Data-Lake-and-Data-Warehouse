"""Pipeline observability: run auditing, metrics and data-quality results.

Every stage writes a row to ``ops.pipeline_runs`` (start, finish, duration,
record counts, bytes, error) and every data-quality assertion writes to
``ops.data_quality_results``. This is what CloudWatch would receive in AWS,
and it is what the platform's own /api/ops endpoints report.
"""
from __future__ import annotations

import json
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field

from sqlalchemy import text

from .db import connection


@dataclass
class RunResult:
    run_id: int | None = None
    records_in: int = 0
    records_out: int = 0
    records_rejected: int = 0
    bytes_written: int = 0
    details: dict = field(default_factory=dict)


@contextmanager
def track_run(pipeline: str, stage: str, source: str | None = None):
    """Context manager that records success/failure and timing for a stage."""
    started = time.time()
    result = RunResult()
    with connection() as conn:
        result.run_id = conn.execute(
            text(
                """INSERT INTO ops.pipeline_runs (pipeline, stage, source, status)
                   VALUES (:p, :s, :src, 'running') RETURNING run_id"""
            ),
            {"p": pipeline, "s": stage, "src": source},
        ).scalar_one()

    try:
        yield result
    except Exception as exc:  # noqa: BLE001 - recorded then re-raised
        with connection() as conn:
            conn.execute(
                text(
                    """UPDATE ops.pipeline_runs
                       SET status='failed', finished_at=now(),
                           duration_sec=:d, records_in=:ri, records_out=:ro,
                           records_rejected=:rr, error_message=:err,
                           details=CAST(:details AS JSONB)
                       WHERE run_id=:rid"""
                ),
                {
                    "d": round(time.time() - started, 2),
                    "ri": result.records_in, "ro": result.records_out,
                    "rr": result.records_rejected,
                    "err": f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}",
                    "details": json.dumps(result.details, default=str),
                    "rid": result.run_id,
                },
            )
        raise
    else:
        with connection() as conn:
            conn.execute(
                text(
                    """UPDATE ops.pipeline_runs
                       SET status='success', finished_at=now(), duration_sec=:d,
                           records_in=:ri, records_out=:ro, records_rejected=:rr,
                           bytes_written=:bw, details=CAST(:details AS JSONB)
                       WHERE run_id=:rid"""
                ),
                {
                    "d": round(time.time() - started, 2),
                    "ri": result.records_in, "ro": result.records_out,
                    "rr": result.records_rejected, "bw": result.bytes_written,
                    "details": json.dumps(result.details, default=str),
                    "rid": result.run_id,
                },
            )


def record_quality_check(run_id: int, dataset: str, check_name: str,
                         passed: bool, failed_rows: int, total_rows: int,
                         details: dict | None = None) -> None:
    with connection() as conn:
        conn.execute(
            text(
                """INSERT INTO ops.data_quality_results
                   (run_id, dataset, check_name, status, failed_rows, total_rows, details)
                   VALUES (:rid, :ds, :cn, :st, :fr, :tr, CAST(:d AS JSONB))"""
            ),
            {
                "rid": run_id, "ds": dataset, "cn": check_name,
                "st": "passed" if passed else "failed",
                "fr": failed_rows, "tr": total_rows,
                "d": json.dumps(details or {}, default=str),
            },
        )


def get_watermark(dataset: str):
    with connection() as conn:
        row = conn.execute(
            text("SELECT last_value FROM ops.watermarks WHERE dataset=:d"),
            {"d": dataset},
        ).first()
    return row[0] if row else None


def set_watermark(dataset: str, value) -> None:
    with connection() as conn:
        conn.execute(
            text(
                """INSERT INTO ops.watermarks (dataset, last_value, updated_at)
                   VALUES (:d, :v, now())
                   ON CONFLICT (dataset) DO UPDATE
                     SET last_value = EXCLUDED.last_value, updated_at = now()"""
            ),
            {"d": dataset, "v": value},
        )
