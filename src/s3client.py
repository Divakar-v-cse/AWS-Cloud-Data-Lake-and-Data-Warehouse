"""S3-compatible object storage for the data lake.

Two interchangeable backends expose the exact same functions and key layout:

  * ``s3``    - boto3 against Amazon S3, or MinIO when S3_ENDPOINT_URL is set
  * ``local`` - filesystem store rooted at LAKE_ROOT, using identical
                ``s3://<bucket>/<key>`` semantics (bucket = top-level folder)

Business logic never knows which backend is active, so moving from the local
environment to AWS is a configuration change, not a code change.
"""
from __future__ import annotations

import io
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .config import settings

_BACKEND = settings.lake_backend.lower()
_ROOT = Path(settings.lake_root)


# ---------------------------------------------------------------------------
# Local filesystem backend (S3 key semantics)
# ---------------------------------------------------------------------------
def _local_path(bucket: str, key: str) -> Path:
    safe = key.replace("\\", "/").lstrip("/")
    return _ROOT / bucket / safe


def _local_put(bucket: str, key: str, data: bytes) -> None:
    path = _local_path(bucket, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _local_get(bucket: str, key: str) -> bytes:
    path = _local_path(bucket, key)
    if not path.exists():
        raise FileNotFoundError(f"s3://{bucket}/{key} not found")
    return path.read_bytes()


def _local_list(bucket: str, prefix: str = "") -> list[str]:
    base = _ROOT / bucket
    if not base.exists():
        return []
    keys = []
    for p in base.rglob("*"):
        if p.is_file():
            rel = p.relative_to(base).as_posix()
            if rel.startswith(prefix):
                keys.append(rel)
    return sorted(keys)


def _local_exists(bucket: str, key: str) -> bool:
    return _local_path(bucket, key).exists()


# ---------------------------------------------------------------------------
# boto3 backend (AWS S3 / MinIO)
# ---------------------------------------------------------------------------
_s3 = None


def _client():
    global _s3
    if _s3 is None:
        import boto3

        kwargs: dict = {
            "aws_access_key_id": settings.aws_access_key_id,
            "aws_secret_access_key": settings.aws_secret_access_key,
            "region_name": settings.aws_region,
        }
        if settings.s3_endpoint_url:
            kwargs["endpoint_url"] = settings.s3_endpoint_url
        _s3 = boto3.client("s3", **kwargs)
    return _s3


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def ensure_bucket(bucket: str) -> None:
    if _BACKEND == "local":
        (_ROOT / bucket).mkdir(parents=True, exist_ok=True)
        return
    existing = {b["Name"] for b in _client().list_buckets().get("Buckets", [])}
    if bucket not in existing:
        _client().create_bucket(Bucket=bucket)


def put_bytes(bucket: str, key: str, data: bytes) -> None:
    if _BACKEND == "local":
        _local_put(bucket, key, data)
    else:
        _client().put_object(Bucket=bucket, Key=key, Body=data)


def put_text(bucket: str, key: str, text: str) -> None:
    put_bytes(bucket, key, text.encode("utf-8"))


def put_json(bucket: str, key: str, obj) -> None:
    put_text(bucket, key, json.dumps(obj, default=str, indent=2))


def put_dataframe_parquet(bucket: str, key: str, df, **pq_kwargs) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, **pq_kwargs)
    put_bytes(bucket, key, buf.getvalue())


def get_bytes(bucket: str, key: str) -> bytes:
    if _BACKEND == "local":
        return _local_get(bucket, key)
    return _client().get_object(Bucket=bucket, Key=key)["Body"].read()


def get_dataframe_parquet(bucket: str, key: str):
    import pandas as pd

    return pd.read_parquet(io.BytesIO(get_bytes(bucket, key)))


def list_keys(bucket: str, prefix: str = "") -> list[str]:
    if _BACKEND == "local":
        return _local_list(bucket, prefix)
    keys: list[str] = []
    paginator = _client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def key_exists(bucket: str, key: str) -> bool:
    if _BACKEND == "local":
        return _local_exists(bucket, key)
    from botocore.exceptions import ClientError

    try:
        _client().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def lake_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def backend_name() -> str:
    return _BACKEND
