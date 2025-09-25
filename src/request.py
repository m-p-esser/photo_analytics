from prefect.blocks.system import Secret
import httpx
from fake_useragent import UserAgent
from random import randint 

def create_random_ua_string(
    min_version: float,
    platforms: str
) -> str:
    """Creata a random Useragent string"""
    ua = UserAgent(
        platforms=platforms,
        min_version=min_version
    )
    random_useragent_string = ua.random

    return random_useragent_string


def prepare_proxy_adresses(
    host: str = "brd.superproxy.io",
    port: int = 33335,
    prefect_block_prefix: str = "bright-data"
) -> dict:
    """Prepare proxy adress to it can be used in a request"""

    # username = Secret.load(f"{prefect_block_prefix}-datacenter-proxy-user-name").get()
    # password = Secret.load(f"{prefect_block_prefix}-datacenter-proxy-password").get()
    username = Secret.load(f"{prefect_block_prefix}-residental-proxy-user-name").get()
    password = Secret.load(f"{prefect_block_prefix}-residental-proxy-password").get()

    session_id = str(randint(1000000, 9999999))
    # Session guarantees IP rotations
    proxy_url = f"http://{username}-session-{session_id}:{password}@{host}:{port}"
    # proxy_url = f"http://{username}:{password}@{host}:{port}"

    proxies = {"http": proxy_url, "https": proxy_url}

    return proxies


def request_unsplash_napi(
        base_url: str,
        endpoint: str,
        params: dict,
        headers: dict,
        proxy: str
):
    url = base_url + endpoint

    with httpx.Client(proxy=proxy, verify=False, timeout=10.0) as client:
        response = client.get(
            url=url,
            params=params,
            headers=headers,
        )
        response.raise_for_status()

        if response.status_code == 200:
            print(f"Request successful, Status Code: {response.status_code}")

            return response