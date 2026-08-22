import os
import hvac


def get_vault_config():
    vault_addr = os.getenv(
        "VAULT_ADDR",
        "http://127.0.0.1:8200"
    )

    vault_token = os.getenv(
        "VAULT_TOKEN",
        "root"
    )

    client = hvac.Client(
        url=vault_addr,
        token=vault_token
    )

    response = client.secrets.kv.v2.read_secret_version(
        path="pyspark"
    )

    return response["data"]["data"]