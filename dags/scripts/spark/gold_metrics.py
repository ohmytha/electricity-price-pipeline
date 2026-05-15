import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def calculate_to_gold(silver_path, ds):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("calculate_to_gold") \
        .getOrCreate()

    df = spark.read.parquet(silver_path)

    df = df \
        .withColumn("Total_Cost_AUD", col("demand_pos_RRP") * col("RRP_positive")) \
        .withColumn("Saved_from_Neg_RRP", col("demand_neg_RRP") * col("RRP_negative")) \
        .withColumn("is_extreme_weather",
            (col("max_temperature") > 35) | (col("min_temperature") < 10)
        )

    output_dir = "/opt/airflow/datalake/gold/"
    output_path = os.path.join(output_dir, f"gold_{ds}.parquet")

    os.makedirs(output_dir, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

    spark.stop()

    return output_path
