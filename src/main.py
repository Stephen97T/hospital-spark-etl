from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HospitalETL").master("local[*]").getOrCreate()


def main():
    pass


if __name__ == "__main__":
    main()
