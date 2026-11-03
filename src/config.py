"""Central configuration for Divakar Cloud Data Lake & Warehouse.

All settings are read from environment variables with sensible local defaults,
so the SAME code runs against MinIO (local) or real AWS S3 / Redshift without
code changes. This is the key abstraction that makes the project portable.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


@dataclass
class Settings:
    # ---- Storage: Amazon S3 or S3-compatible (MinIO) -----------------------
    # LAKE_BACKEND=s3    -> real Amazon S3 (boto3) or MinIO via S3_ENDPOINT_URL
    # LAKE_BACKEND=local -> filesystem backend with identical s3:// key layout
    #                       (used when no container registry / AWS creds exist)
    lake_backend: str = os.getenv("LAKE_BACKEND", "local")
    lake_root: Path = Path(os.getenv("LAKE_ROOT", ROOT / "data" / "lake"))
    s3_endpoint_url: str | None = os.getenv("S3_ENDPOINT_URL")
    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")

    # Medallion bucket names
    bronze_bucket: str = os.getenv("BRONZE_BUCKET", "bronze")
    silver_bucket: str = os.getenv("SILVER_BUCKET", "silver")
    gold_bucket: str = os.getenv("GOLD_BUCKET", "gold")
    artifacts_bucket: str = os.getenv("ARTIFACTS_BUCKET", "artifacts")

    # ---- Data warehouse: Amazon Redshift or PostgreSQL ---------------------
    # AWS_MODE flips the remaining defaults to the cloud deployment. It changes
    # nothing on its own: the host, credentials and buckets still come from the
    # environment, so the local stack keeps working with AWS_MODE unset.
    aws_mode: bool = os.getenv("AWS_MODE", "").strip().lower() in ("1", "true", "yes", "on")
    warehouse_host: str = os.getenv("WAREHOUSE_HOST", "localhost")
    warehouse_port: int = int(os.getenv("WAREHOUSE_PORT", "5433"))
    warehouse_db: str = os.getenv("WAREHOUSE_DB", "analytics")
    warehouse_user: str = os.getenv("WAREHOUSE_USER", "deuser")
    warehouse_password: str = os.getenv("WAREHOUSE_PASSWORD", "depass")
    warehouse_schema: str = os.getenv("WAREHOUSE_SCHEMA", "dwh")
    # psycopg (v3) is the local driver. Redshift's protocol dialect is happiest
    # on psycopg2; pin it with WAREHOUSE_DRIVER=psycopg2 in AWS mode.
    warehouse_driver: str = os.getenv("WAREHOUSE_DRIVER", "psycopg")
    # Empty -> infer: TLS for anything that is not a loopback host. Redshift
    # Serverless ships with require_ssl=true, so plaintext is refused outright.
    warehouse_ssl: str = os.getenv("WAREHOUSE_SSL", "")

    # ---- Data generation ---------------------------------------------------
    n_customers: int = int(os.getenv("N_CUSTOMERS", "20000"))
    n_products: int = int(os.getenv("N_PRODUCTS", "500"))
    n_orders: int = int(os.getenv("N_ORDERS", "120000"))
    random_seed: int = int(os.getenv("RANDOM_SEED", "42"))
    data_end_date: str = os.getenv("DATA_END_DATE", "2026-10-31")

    # ---- Paths -------------------------------------------------------------
    raw_dir: Path = field(
        default_factory=lambda: Path(os.getenv("RAW_DIR", ROOT / "data" / "raw"))
    )

    @property
    def warehouse_ssl_required(self) -> bool:
        """TLS is explicit when WAREHOUSE_SSL is set, otherwise inferred.

        Loopback hosts (the local PostgreSQL on 5433) do not use TLS; anything
        else - Redshift Serverless in particular - is required to.
        """
        value = self.warehouse_ssl.strip().lower()
        if value:
            return value in ("1", "true", "yes", "on", "require")
        return self.warehouse_host not in ("", "localhost", "127.0.0.1", "::1")

    @property
    def dsn(self) -> str:
        from urllib.parse import quote

        password = quote(self.warehouse_password, safe="")
        user = quote(self.warehouse_user, safe="")
        dsn = (
            f"postgresql+{self.warehouse_driver}://{user}:{password}"
            f"@{self.warehouse_host}:{self.warehouse_port}/{self.warehouse_db}"
        )
        if self.warehouse_ssl_required:
            dsn += "?sslmode=require"
        return dsn

    @property
    def is_local(self) -> bool:
        """True when targeting the local stand-ins (MinIO/PostgreSQL)."""
        return self.s3_endpoint_url is not None


settings = Settings()
