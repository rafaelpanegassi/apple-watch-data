import os
from pathlib import Path
import yaml
from loguru import logger


class Settings:
    def __init__(self):
        self.project_root = Path(__file__).resolve().parent.parent
        config_path = self.project_root / "config" / "settings.yaml"

        self.config_data = {}
        if config_path.exists():
            with open(config_path, "r") as f:
                try:
                    self.config_data = yaml.safe_load(f) or {}
                except Exception as e:
                    logger.warning(f"Failed to parse settings.yaml: {e}")

        minio_section = self.config_data.get("minio", {})
        self.minio_endpoint = os.getenv(
            "MINIO_ENDPOINT", minio_section.get("endpoint", "127.0.0.1:9000")
        )
        self.minio_access_key = os.getenv(
            "MINIO_ACCESS_KEY", minio_section.get("access_key", "minioadmin")
        )
        self.minio_secret_key = os.getenv(
            "MINIO_SECRET_KEY", minio_section.get("secret_key", "minioadminpassword")
        )
        self.minio_secure = os.getenv(
            "MINIO_SECURE", str(minio_section.get("secure", False))
        ).lower() in ("true", "1", "yes")

        buckets = minio_section.get("buckets", {})
        self.bucket_bronze = os.getenv(
            "MINIO_BUCKET_BRONZE", buckets.get("bronze", "bronze")
        )
        self.bucket_silver = os.getenv(
            "MINIO_BUCKET_SILVER", buckets.get("silver", "silver")
        )

        postgres_section = self.config_data.get("postgres", {})
        self.db_host = os.getenv("DB_HOST", postgres_section.get("host", "127.0.0.1"))
        self.db_port = int(
            os.getenv("DB_PORT", str(postgres_section.get("port", 5432)))
        )
        self.db_name = os.getenv("DB_NAME", postgres_section.get("db", "health_analytics"))
        self.db_user = os.getenv("DB_USER", postgres_section.get("user", "postgres"))
        self.db_password = os.getenv(
            "DB_PASSWORD", postgres_section.get("password", "postgrespassword")
        )
        self.db_schema = os.getenv(
            "DB_SCHEMA", postgres_section.get("schema", "gold")
        )

    def get_postgres_connection_uri(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    def get_postgres_duckdb_conn_str(self) -> str:
        return f"dbname={self.db_name} user={self.db_user} password={self.db_password} host={self.db_host} port={self.db_port}"


settings = Settings()
