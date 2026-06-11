import os
from pathlib import Path
from loguru import logger
from src.config import settings
from src.utils.minio_client import upload_file, ensure_bucket


def load_bronze() -> None:
    project_root = Path(__file__).resolve().parent.parent.parent
    data_dir = (project_root / "data").resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    zip_files = list(data_dir.glob("*.zip"))
    if not zip_files:
        raise FileNotFoundError("No user data zip found in 'data/' directory.")

    target_zip = zip_files[0]
    resolved_zip_path = target_zip.resolve()
    if not str(resolved_zip_path).startswith(str(data_dir) + os.sep):
        raise ValueError(f"Security Warning: Attempted path traversal via target zip: {target_zip}")

    logger.info(f"Found active user ZIP export: {target_zip.name}")
    ensure_bucket(settings.bucket_bronze)

    object_name = "apple_health_raw.zip"
    upload_file(settings.bucket_bronze, object_name, str(target_zip))
    logger.info(f"Bronze ingestion complete: {settings.bucket_bronze}/{object_name}")


if __name__ == "__main__":
    load_bronze()
