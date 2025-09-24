import httpx
from fake_useragent import UserAgent
import pandas as pd
import fastparquet

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
        data = response.json()

        return data


def persist_data(
        data: dict,
        delta_load: bool = True
):
    df = pd.DataFrame(data)

    if delta_load:
        fastparquet.write(
            filename="temp.parquet",
            data=df,
            append=True
        )