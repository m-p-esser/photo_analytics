"""Prefect Flow to clean data from /photos endpoint stored in Bigquery"""

# Packages
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_query
from google.cloud import bigquery
# Custom modules


@task(log_prints=True)
def clean_unsplash_photos_in_bigquery(
    dataset: str,
    table: str,
    gcp_credential_block_name: str,
    location: str,
    job_config: dict
):
    gcp_credentials = GcpCredentials.load(gcp_credential_block_name)
    # client = gcp_credentials.get_bigquery_client()

    # deduplicate and only store the latest entry for a photo based on time of request
    query = f'''
        WITH t AS (
            SELECT *, 
            PARSE_TIMESTAMP('%a, %d %b %Y %H:%M:%S %Z',unsplash_requested_at) AS unsplash_requested_at_timestamp 
            FROM `{dataset}.raw__photos`
        )
        SELECT * EXCEPT(unsplash_requested_at)
        FROM t
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY unsplash_requested_at_timestamp DESC) = 1
    '''
    result = bigquery_query(
        query=query, 
        gcp_credentials=gcp_credentials, 
        dataset=dataset, 
        table=table, 
        location=location,
        job_config=job_config
    )
    return result


@flow(log_prints=True)
def clean_bq_unsplash_napi_photos():
    clean_unsplash_photos_in_bigquery(
        dataset='unsplash__test',
        table='cleaned__photos',
        gcp_credential_block_name='gcp--srv-user-etl',
        location='europe-west4',
        job_config={'write_disposition': 'WRITE_TRUNCATE'}
    )


if __name__ == '__main__':
    clean_bq_unsplash_napi_photos()