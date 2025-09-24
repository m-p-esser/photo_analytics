import httpx
from fake_useragent import UserAgent

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


def request_unsplash_napi(
        base_url: str,
        endpoint: str,
        params: dict,
        headers: dict
):
    url = base_url + endpoint

    response = httpx.get(
        url=url,
        params=params,
        headers=headers
    )
    response.raise_for_status()

    if response.status_code == 200:
        print(f"Request successful, Status Code: {response.status_code}")

        return response