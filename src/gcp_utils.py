import logging
import os

from google.cloud import storage

logger = logging.getLogger(__name__)


def upload_local_to_gcs(local_path: str, bucket_name: str, gcs_path: str) -> None:
    """Uploads a local directory (like a Spark Parquet folder) to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            # Create a path that looks like a folder structure in GCS
            relative_path = os.path.relpath(local_file, local_path)
            blob_path = os.path.join(gcs_path, relative_path)

            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_file)
    logger.info(f"Successfully uploaded {local_path} to gs://{bucket_name}/{gcs_path}")
