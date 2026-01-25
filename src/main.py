import os
import sys

import pandas as pd
from pyspark.sql import SparkSession

from src.cleanse_data import clean_columns_names

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

# Verify the paths Spark is seeing
print(f"Python Executable: {sys.executable}")
print(f"Spark Version: {spark.version}")

df = pd.read_excel("data/hospital-dataset.xlsx")
sdf = spark.createDataFrame(df)
sdf = clean_columns_names(sdf)
sdf.count()


def main() -> None:
    pass


if __name__ == "__main__":
    main()
