import os
from pyspark.sql import SparkSession

environment = os.getenv("ENVIRONMENT", "dev")

print(f"Environment: {environment}")

spark = (
    SparkSession.builder
    .appName("my-pyspark-app")
    .getOrCreate()
)

df = spark.createDataFrame(
    [
        (1, "A"),
        (2, "B"),
        (3, "C"),
    ],
    ["id", "name"],
)

df.show()

spark.stop()