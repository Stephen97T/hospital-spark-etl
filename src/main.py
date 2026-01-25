import os
import sys

from pyspark.sql import SparkSession, functions as F

from src.cleanse_data import clean_columns_names
from src.kaggle_data import download_and_move_data, prepare_hospital_data

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


def run_pipeline():
    # 1. Ingestion
    download_and_move_data()
    path_hospital = prepare_hospital_data()
    sdf_bronze = spark.read.csv(path_hospital, header=True, inferSchema=True)

    # 2. Basic Cleaning
    sdf_silver = clean_columns_names(sdf_bronze)

    # 3. Feature Engineering
    # Calculate stay duration, normalize names, and create a high-billing flag
    sdf_gold = (
        sdf_silver.withColumn("name", F.initcap(F.lower(F.col("name"))))
        .withColumn("stay_duration", F.datediff("discharge_date", "date_of_admission"))
        .withColumn(
            "is_high_bill",
            F.when(F.col("billing_amount") > 30000, True).otherwise(False),
        )
    )

    # 4. Data Quality Check (Filter out impossible dates if any)
    valid_data = sdf_gold.filter(F.col("stay_duration") >= 0)

    # 5. Optimized Export (Parquet)
    # This creates a folder structure: output/gold_hospital_data/medical_condition=Asthma/...
    output_path = "data/gold_hospital_data"
    valid_data.write.mode("overwrite").partitionBy("medical_condition").parquet(
        output_path
    )

    print(f"Pipeline complete. Gold data saved to: {output_path}")


if __name__ == "__main__":
    run_pipeline()
    spark.stop()
