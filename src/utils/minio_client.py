from minio import Minio
from loguru import logger
from src.config import settings


def get_minio_client() -> Minio:
    return Minio(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def ensure_bucket(bucket_name: str) -> None:
    client = get_minio_client()
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logger.debug(f"Bucket '{bucket_name}' already exists.")
    except Exception as e:
        logger.error(f"Error ensuring bucket '{bucket_name}': {e}")
        raise e


def upload_file(bucket_name: str, object_name: str, file_path: str) -> None:
    ensure_bucket(bucket_name)
    client = get_minio_client()
    try:
        client.fput_object(bucket_name, object_name, file_path)
        logger.info(f"File '{file_path}' successfully uploaded to '{bucket_name}/{object_name}'.")
    except Exception as e:
        logger.error(f"Error uploading file '{file_path}' to '{bucket_name}/{object_name}': {e}")
        raise e


def download_file(bucket_name: str, object_name: str, file_path: str) -> None:
    client = get_minio_client()
    try:
        client.fget_object(bucket_name, object_name, file_path)
        logger.info(f"Object '{bucket_name}/{object_name}' successfully downloaded to '{file_path}'.")
    except Exception as e:
        logger.error(f"Error downloading object '{bucket_name}/{object_name}' to '{file_path}': {e}")
        raise e
