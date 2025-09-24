"""Prefect Flow to extract data from Unsplash /photos endpoint"""

# Packages
import pandas as pd
from prefect import flow, task, get_run_logger
from requests import Response
from urllib.parse import urlparse, parse_qs
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
    logger = get_run_logger()
    response = request_unsplash_napi(
        base_url,
        endpoint,
        params,
        headers
    )
    logger.info(f"Response contains {len(response.json())} Items")

    return response


@task(log_prints=True)
def identify_last_api_page(response: Response) -> int:
    logger = get_run_logger()
    # Response Header value looks like this:
    # <https://unsplash.com/photos?page=1&per_page=5>; rel="first", <https://unsplash.com/photos?page=1&per_page=5>; rel="prev", <https://unsplash.com/photos?page=62485&per_page=5>; rel="last", <https://unsplash.com/photos?page=3&per_page=5>; rel="next"
    link = response.headers['link']
    links = link.split(',')
    relevant_link = links[0] # rel="last", expected at first position
    relevant_url = relevant_link.split(';')[0]
    page_number = int(parse_qs(urlparse(relevant_url).query).get('page', None)[0])
    logger.info(
        f"Last page number of API endpoint is '{page_number}'"
    )
    return page_number


@task(log_prints=True)
def store_unsplash_napi_photos_as_dataframe(
    response: Response
):
    logger = get_run_logger()
    data = response.json()
    df = pd.DataFrame(data)
    # add response header infos
    df['unsplash_request_id'] = response.headers['x-request-id']
    df['unsplash_requested_at'] = response.headers['date']
    logger.info(
        f"Dataframe contains {df.shape[0]} rows and {df.shape[1]} columns"
    )

    return df


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
def extract_gcs_unsplash_napi_photos(params: dict):

    logger = get_run_logger()

    useragent_string = create_random_ua_string(
        min_version=120.0,
        platforms='desktop'
    )
    logger.info(f"Will be using '{useragent_string}' to make next requests")
    headers = {"User-Agent": useragent_string}  # Overwrite Useragent

    base_url = "https://unsplash.com/napi"
    endpoint = "/photos"

    response = request_unsplash_napi_photos(
        base_url, endpoint, params, headers
    )

    last_page_number = identify_last_api_page(response)

    df = store_unsplash_napi_photos_as_dataframe(
        response
    )

    upload_blob_from_dataframe(
        bucket_name='test__unsplash_napi__photos',
        dataframe=df,
        destination_file_name=response.headers['x-request-id'],
        gcp_credential_block_name='gcp--srv-user-etl',
        serialization_format='parquet'
    )

if __name__ == '__main__':
    for x in range(1,3):
        print(f"Requesting API page number '{x}'")
        params = {
            "page": x,
            "per_page": 100, # limit seems to be 10
            "order_by": "oldest",
        }
        extract_gcs_unsplash_napi_photos(params)



