import logging
import os
import sys

from pyspark.sql import SparkSession

from pipeline import run_pipeline

# Configure logging to show the level and message
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Environment Toggle
ENV_STATE = os.getenv("ENV_STATE", "dev").lower()
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "")

# Tell Spark to use the exact same python executable as your current environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_GATEWAY_SECRET"] = "1"

spark = (
    SparkSession.builder.appName("HospitalETL")
    # Uses all available CPU cores for parallelism
    .master("local[*]")
    # Standard networking configs for Windows/Local environments
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    # Limit memory usage (e.g., 2 Gigabytes) to keep your system stable
    .config("spark.driver.memory", "2g")
    # Optimizes partition handling for small datasets
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

if __name__ == "__main__":
    logger.info("Starting Hospital ETL Pipeline...")
    run_pipeline(spark, ENV_STATE, BUCKET_NAME)
    spark.stop()
    logger.info("Finished Hospital ETL Pipeline")
