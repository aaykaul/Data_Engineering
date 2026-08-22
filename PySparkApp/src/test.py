import os
import hvac

from pyspark.sql import SparkSession


vault_addr = os.getenv("VAULT_ADDR", "http://127.0.0.1:8200")
vault_token = os.getenv("VAULT_TOKEN", "root")

client = hvac.Client(
    url=vault_addr,
    token=vault_token
)

response = client.secrets.kv.v2.read_secret_version(
    path="pyspark"
)

config = response["data"]["data"]

environment = config["ENVIRONMENT"]
input_path = config["INPUT_PATH"]
output_path = config["OUTPUT_PATH"]

print(f"Environment: {environment}")
print(f"Input path: {input_path}")
print(f"Output path: {output_path}")