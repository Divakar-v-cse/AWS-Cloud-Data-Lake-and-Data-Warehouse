"""End-to-end pipeline orchestrator (the local equivalent of EventBridge + Step Functions).

Stages, in dependency order:

  1. generate   synthetic source files (CSV / JSON / API payloads)
  2. ingest     source -> bronze lake (raw, immutable, hashed, watermarked)
  3. silver     bronze -> validated Parquet + data-quality gate + quarantine
  4. gold       silver -> business aggregates in the gold lake
  5. warehouse  gold -> staging -> SCD2 dimensions -> star-schema facts
  6. dbt        staging views + marts + tests
  7. report     consolidated run summary (JSON) for CI / dashboards

Every stage is idempotent and records its own row in ``ops.pipeline_runs``, so a
re-run is safe and the ops endpoints show the whole lineage.

Usage
-----
    python -m src.pipeline                       # full refresh, CSV ingestion
    python -m src.pipeline --format api          # demonstrate API ingestion
    python -m src.pipeline --skip-generate       # reuse existing raw files
    python -m src.pipeline --since 2026-06-01    # incremental re-ingest
    python -m src.pipeline --no-dbt              # skip the transformation step
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone

from .config import ROOT, settings


def _step(name: str) -> None:
    print(f"\n=== [{datetime.now():%H:%M:%S}] {name} " + "=" * (48 - len(name)), flush=True)


def run_generate() -> dict:
    from .generate_data import generate

    return generate()


def run_ingest(fmt: str, since: str | None) -> dict:
    from .ingest import SOURCES, ingest_source

    out = {}
    for source in SOURCES:
        res = ingest_source(source, fmt=fmt, since=since)
        out[source] = {
            "rows": res.get("rows"),
            "bytes": res.get("bytes"),
            "object": res.get("object"),
        }
        print(f"  {source:<12} {res.get('rows', 0):>8,} rows  -> {res.get('object', 'skipped')}")
    return out


def run_silver() -> dict:
    from .silver import run_silver as _run

    return _run()


def run_gold() -> dict:
    from .gold import run_gold as _run

    return _run()


def run_warehouse() -> dict:
    from .load_warehouse import run_load

    return run_load()


def run_dbt(command: str = "run") -> dict:
    """Invoke dbt through the current interpreter (dbt.exe is not on PATH)."""
    proc = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", command,
         "--project-dir", str(ROOT / "dbt"),
         "--profiles-dir", str(ROOT / "dbt")],
        cwd=str(ROOT), capture_output=True, text=True,
    )
    tail = [ln for ln in (proc.stdout or "").splitlines() if ln.strip()][-12:]
    err = [ln for ln in (proc.stderr or "").splitlines() if ln.strip()][-6:]
    return {"command": command, "returncode": proc.returncode,
            "output_tail": tail, "stderr_tail": err}


def summarise(results: dict, started: float) -> dict:
    from .db import query_df

    counts = {}
    for table in ["dim_customer", "dim_product", "dim_date", "dim_payment_method",
                  "fact_orders", "fact_order_items", "fact_payments",
                  "fact_shipments", "fact_returns"]:
        try:
            counts[table] = int(
                query_df(f"select count(*) as c from dwh.{table}")["c"].iloc[0])
        except Exception:  # noqa: BLE001
            counts[table] = None

    marts = []
    try:
        marts = query_df(
            "select table_name from information_schema.tables "
            "where table_schema = 'marts' order by table_name")["table_name"].tolist()
    except Exception:  # noqa: BLE001
        pass

    return {
        "project": "Divakar Cloud Data Lake and Data Warehouse Project",
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_sec": round(time.time() - started, 1),
        "lake_backend": settings.lake_backend,
        "warehouse": f"postgresql://{settings.warehouse_host}:{settings.warehouse_port}"
                     f"/{settings.warehouse_db}",
        "warehouse_counts": counts,
        "marts": marts,
        "stages": results,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the full lake + warehouse pipeline")
    parser.add_argument("--format", default="csv", choices=["csv", "json", "api"],
                        help="source file format read by the ingestion stage")
    parser.add_argument("--since", default=None,
                        help="ISO timestamp for incremental ingestion (watermark demo)")
    parser.add_argument("--skip-generate", action="store_true",
                        help="reuse the raw files already on disk")
    parser.add_argument("--skip-ingest", action="store_true")
    parser.add_argument("--no-dbt", action="store_true",
                        help="skip dbt run/test (warehouse only)")
    args = parser.parse_args(argv)

    started = time.time()
    results: dict = {}

    if not args.skip_generate:
        _step("1/6 generate source data")
        results["generate"] = run_generate()
        print(f"  generated {results['generate'].get('total_rows', 0):,} rows")

    if not args.skip_ingest:
        _step(f"2/6 ingest -> bronze ({args.format}"
              f"{', since ' + args.since if args.since else ''})")
        results["ingest"] = run_ingest(args.format, args.since)

    _step("3/6 bronze -> silver")
    results["silver"] = run_silver()
    print(f"  {json.dumps({k: (v.get('rows') if isinstance(v, dict) else v) for k, v in results['silver'].items()})[:400]}")

    _step("4/6 silver -> gold")
    results["gold"] = run_gold()

    _step("5/6 gold -> warehouse")
    results["warehouse"] = run_warehouse()

    if not args.no_dbt:
        _step("6/6 dbt run + test")
        results["dbt_run"] = run_dbt("run")
        results["dbt_test"] = run_dbt("test")
        ok = results["dbt_run"]["returncode"] == 0 and results["dbt_test"]["returncode"] == 0
        print(f"  dbt run rc={results['dbt_run']['returncode']} "
              f"test rc={results['dbt_test']['returncode']} -> {'OK' if ok else 'FAILED'}")
        for line in results["dbt_test"]["output_tail"]:
            print("  " + line)

    summary = summarise(results, started)
    (ROOT / "artifacts").mkdir(exist_ok=True)
    (ROOT / "artifacts" / "pipeline_summary.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8")

    print("\n" + "=" * 60)
    print(f"Pipeline finished in {summary['duration_sec']}s")
    for table, count in summary["warehouse_counts"].items():
        print(f"  {table:<20} {count:>10,}" if count is not None else f"  {table:<20} {'n/a':>10}")
    print(f"  marts: {len(summary['marts'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
