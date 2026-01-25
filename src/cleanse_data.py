import pyspark


def clean_columns_names(sdf: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """Cleanse the column names of a Spark DataFrame by converting them to lowercase
    and replacing spaces with underscores.
    """
    new_cols = [col.lower().replace(" ", "_") for col in sdf.columns]
    sdf = sdf.toDF(*new_cols)
    return sdf
