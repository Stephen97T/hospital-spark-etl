import logging

from pyspark.sql import SparkSession, functions as F

from src.cleanse_data import clean_columns_names, apply_feature_engineering
from src.gcp_utils import upload_local_to_gcs
from src.kaggle_data import download_and_move_data, prepare_hospital_data

logger = logging.getLogger(__name__)


def run_pipeline(spark: SparkSession, env_state: str, bucket_name: str) -> None:
    download_and_move_data()
    path_hospital = prepare_hospital_data()
    sdf_bronze = spark.read.csv(path_hospital, header=True, inferSchema=True)

    sdf_silver = clean_columns_names(sdf_bronze)

    sdf_gold = apply_feature_engineering(sdf_silver)

    valid_data = sdf_gold.filter(F.col("stay_duration") >= 0)

    # PATH LOGIC
    local_output = "data/gold_hospital_data"

    # Write locally first (Spark always prefers local disk for speed)
    valid_data.write.mode("overwrite").partitionBy("medical_condition").parquet(
        local_output
    )

    # If PROD, move it to the Cloud Bucket
    if env_state == "prod" and bucket_name:
        upload_local_to_gcs(local_output, bucket_name, "gold_hospital_data")
    else:
        logger.info(f"Pipeline complete. Data stays in: {local_output}")
