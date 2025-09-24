"""Prefect Flow to extract data from Unsplash /photos endpoint"""

# Packages
import pandas as pd
from prefect import flow, task
from requests import Response
import fastparquet
# Custom modules
from request import (
    create_random_ua_string, request_unsplash_napi
)
from gcs import upload_blob_from_dataframe


@task(log_prints=True)
def request_unsplash_napi_photos(
    base_url: str,
    endpoint: str,
    params: dict,
    headers: dict
):
    response = request_unsplash_napi(
        base_url,
        endpoint,
        params,
        headers
    )
    return response


@task(log_prints=True)
def store_unsplash_napi_photos_as_file(
    response: Response,
    delta_load: bool = True
):
    data = response.json()
    df = pd.DataFrame(data)
    # add response header infos
    df['unsplash_request_id'] = response.headers['x-request-id']
    df['unsplash_requested_at'] = response.headers['date']

    if delta_load:
        fastparquet.write(
            filename="temp.parquet",
            data=df,
            append=True
        )
    if delta_load == False: # full load
        fastparquet.write(
            filename="temp.parquet",
            data=df,
            append=False
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
def extract_gcs_unsplash_napi_photos():

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

    response = request_unsplash_napi_photos(
        base_url, endpoint, params, headers
    )

    store_unsplash_napi_photos_as_file(
        response, delta_load=True
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
    extract_gcs_unsplash_napi_photos()



