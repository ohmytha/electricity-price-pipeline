import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def clean_to_silver(bronze_path, ds):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("clean_to_silver") \
        .getOrCreate()

    df = spark.read.csv(bronze_path, header=True, inferSchema=True)

    df = df.withColumn(
        "frac_at_neg_RRP",
        when(col("frac_at_neg_RRP") < 0.0, 0.0)
        .when(col("frac_at_neg_RRP") > 1.0, 1.0)
        .otherwise(col("frac_at_neg_RRP"))
    )

    df = df.dropna(subset=["date", "demand"])

    output_dir = "/opt/airflow/datalake/silver/"
    output_path = os.path.join(output_dir, f"silver_{ds}.parquet")

    os.makedirs(output_dir, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

    spark.stop()

    return output_path
