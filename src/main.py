import os
import sys

from pyspark.sql import SparkSession

from src.cleanse_data import clean_columns_names
from src.kaggle_data import download_and_move_data, prepare_hospital_data

# Tell Spark to use the exact same python executable as your current environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_GATEWAY_SECRET"] = "1"

spark = (
    SparkSession.builder.appName("DebugHospital")
    .master("local[1]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
    .config("spark.python.worker.reuse", "false")
    .getOrCreate()
)

download_and_move_data()
path_hospital = prepare_hospital_data()
sdf = spark.read.csv(path_hospital, header=True, inferSchema=True)
sdf_cleaned = clean_columns_names(sdf)


def main() -> None:
    pass


if __name__ == "__main__":
    main()
