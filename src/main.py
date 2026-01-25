import os
import sys

import pandas as pd
from pyspark.sql import SparkSession

from src.cleanse_data import clean_columns_names

# Tell Spark to use the exact same python executable as your current environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_GATEWAY_SECRET"] = "1"

spark = SparkSession.builder.appName("HospitalETL").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = pd.read_excel("data/hospital-dataset.xlsx")
sdf = spark.createDataFrame(df)
sdf = clean_columns_names(sdf)
sdf.count()


def main():
    pass


if __name__ == "__main__":
    main()
