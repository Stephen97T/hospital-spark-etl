import pyspark
from pyspark.sql import functions as F


def clean_columns_names(sdf: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """Cleanse the column names of a Spark DataFrame by converting them to lowercase
    and replacing spaces with underscores.
    """
    new_cols = [col.lower().replace(" ", "_") for col in sdf.columns]
    sdf = sdf.toDF(*new_cols)
    return sdf


def apply_feature_engineering(df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """Applies business logic transformations:
    - Normalizes Patient Names
    - Calculates Hospital Stay Duration
    - Flags High Billing Amounts (>30k)
    """
    return (
        df.withColumn("name", F.initcap(F.lower(F.col("name"))))
        .withColumn("stay_duration", F.datediff("discharge_date", "date_of_admission"))
        .withColumn(
            "is_high_bill",
            F.when(F.col("billing_amount") > 30000, True).otherwise(False),
        )
    )
