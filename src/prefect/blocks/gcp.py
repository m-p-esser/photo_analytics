import json
from prefect_gcp import GcpCredentials

def create_gcp_credentials_block(
    gcp_account_name: str,
):
    with open(f".secrets/{gcp_account_name}_keyfile.json") as f:
        service_account_info = json.load(f)
        GcpCredentials(
            service_account_info=service_account_info
            ).save(f"gcp--{gcp_account_name}", overwrite=True)
        print(f"Created Prefect Block for GcpCredentials for GCP User '{gcp_account_name}'")

if __name__ == '__main__':
    create_gcp_credentials_block(
        gcp_account_name='srv-user-etl'
    )