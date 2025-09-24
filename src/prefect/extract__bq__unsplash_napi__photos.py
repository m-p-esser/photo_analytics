"""Prefect Flow to store data from /photos endpoint to Bigquery"""

# Packages
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect_gcp import GcpCredentials
from google.cloud import bigquery
# Custom modules


@task(log_prints=True)
def store_unsplash_photos_in_bigquery(
    dataset: str,
    table: str,
    uri: str,
    gcp_credential_block_name: str,
    location: str,
    job_config: bigquery.LoadJobConfig
):
    gcp_credentials = GcpCredentials.load(gcp_credential_block_name)
    client = gcp_credentials.get_bigquery_client()

    load_job = client.load_table_from_uri(
        source_uris=uri, 
        destination=f"{dataset}.{table}",
        job_config=job_config,
        location=location
    )  # Make an API request.

    result = load_job.result()  # Waits for the job to complete.

    return result


@flow(log_prints=True)
def extract_bq_unsplash_napi_photos():
    store_unsplash_photos_in_bigquery(
        dataset='unsplash__test',
        table='raw__photos',
        uri='gs://test__unsplash_napi__photos/*.parquet',
        gcp_credential_block_name='gcp--srv-user-etl',
        location='europe-west4',
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
    )


if __name__ == '__main__':
    extract_bq_unsplash_napi_photos()
    