"""Collection of functions to interact with Google Cloud Storage"""

import pandas as pd
from prefect_gcp import GcpCredentials, GcsBucket

def upload_blob_from_dataframe(
    bucket_name: str,
    dataframe: pd.DataFrame,
    destination_file_name: str,
    gcp_credential_block_name: str,
    serialization_format: str
):
    """Uploads a file to the bucket."""

    gcp_credentials = GcpCredentials.load(gcp_credential_block_name)
    gcs_bucket = GcsBucket(
        bucket=bucket_name,
        gcp_credentials=gcp_credentials
    )
    gcs_bucket.upload_from_dataframe(
        df=dataframe, 
        to_path=destination_file_name,
        serialization_format=serialization_format
    )
    
    return gcs_bucket
