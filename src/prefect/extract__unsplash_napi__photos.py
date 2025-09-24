"""Prefect Flow to extract data from Unsplash /photos endpoint"""

# Packages
import pandas as pd
from prefect import flow, task
# Custom modules
from request import (
    create_random_ua_string, request_unsplash_napi, persist_data
)
from gcs import upload_blob_from_dataframe


@task(log_prints=True)
def request_unsplash_napi_photos(
    base_url: str,
    endpoint: str,
    params: dict,
    headers: dict
):
    data = request_unsplash_napi(
        base_url,
        endpoint,
        params,
        headers
    )
    return data


@task(log_prints=True)
def store_unsplash_napi_photos_as_file(
    data: dict,
    delta_load: bool = True
):
    persist_data(
        data,
        delta_load
    )
    df = pd.read_parquet("temp.parquet")
    print(df.shape)
    

@task(log_prints=True)
def upload_unsplash_napi_photos_to_gcs_bucket(
    bucket_name: str,
    dataframe: pd.DataFrame,
    destination_file_name: str,
    gcp_credential_block_name: str
):
    upload_blob_from_dataframe(
        bucket_name,
        dataframe,
        destination_file_name,
        gcp_credential_block_name
    )


@flow(log_prints=True)
def extract_unsplash_napi_photos():

    useragent_string = create_random_ua_string(
        min_version=120.0,
        platforms='desktop'
    )
    print(f"Will be using '{useragent_string}' to make next requests")
    headers = {"User-Agent": useragent_string}  # Overwrite Useragent

    base_url = "https://unsplash.com/napi"
    params = {
        "page": 1,
        "per_page": 100, # limit seems to be 10
        "order_by": "oldest",
    }
    endpoint = "/photos"

    data = request_unsplash_napi_photos(
        base_url, endpoint, params, headers
    )

    persist_data(
        data, delta_load=True
    )
    df = pd.read_parquet("temp.parquet")
    print(df.shape)

    upload_blob_from_dataframe(
        bucket_name='test__unsplash_napi__photos',
        dataframe=df,
        destination_file_name='photos',
        gcp_credential_block_name='gcp--srv-user-etl'
    )


if __name__ == '__main__':
    extract_unsplash_napi_photos()



