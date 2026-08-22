import os
from pyspark.sql import SparkSession
from vault_config import get_vault_config

config = get_vault_config()

environment = config["ENVIRONMENT"]
input_path = config["INPUT_PATH"]
output_path = config["OUTPUT_PATH"]

"""
environment  = os.getenv("ENVIRONMENT", "dev")

input_path = os.getenv(
    "INPUT_PATH",
    "/app/data/orders.csv"
)

output_path = os.getenv(
    "OUTPUT_PATH",
    "/app/output"
)
"""

print(f"Environment: {environment}")
print(f"Input path: {input_path}")
print(f"Output path: {output_path}")

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

df2 = spark.read.format('csv').option("header", True).option("inferSchema", True).load(input_path)

df = df.join(df2, df["id"] == df2["order_id"], "inner")

df.write.mode("overwrite").parquet(f"{output_path}/output_{environment}.parquet")
df.write.mode("overwrite").format("csv").save(f"{output_path}/output_{environment}.csv")

df.show()

spark.stop()