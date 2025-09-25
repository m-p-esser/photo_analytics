from prefect.blocks.system import Secret
from dotenv import load_dotenv
import os


def create_bright_data_credentials_block():
    # Datacenter Proxies
    user_name = os.getenv('BRIGHT_DATA_DATACENTER_PROXY_USERNAME')
    Secret(value=user_name).save(
        name='bright-data-datacenter-proxy-user-name', overwrite=True
    )
    print(f"Created Prefect Block for Secret for Brightdata User")

    password = os.getenv('BRIGHT_DATA_DATACENTER_PROXY_PASSWORD')
    Secret(value=password).save(
        name='bright-data-datacenter-proxy-password', overwrite=True
    )
    print(f"Created Prefect Block for Secret for Brightdata Password")

    # Residental Proxies
    user_name = os.getenv('BRIGHT_DATA_RESIDENTAL_PROXY_USERNAME')
    Secret(value=user_name).save(
        name='bright-data-residental-proxy-user-name', overwrite=True
    )
    print(f"Created Prefect Block for Secret for Brightdata User")

    password = os.getenv('BRIGHT_DATA_RESIDENTAL_PROXY_PASSWORD')
    Secret(value=password).save(
        name='bright-data-residental-proxy-password', overwrite=True
    )
    print(f"Created Prefect Block for Secret for Brightdata Password")


if __name__ == '__main__':
    load_dotenv()
    create_bright_data_credentials_block()