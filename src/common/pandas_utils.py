import pandas as pd
from typing import Any
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

def download_s3_to_dataframe(bucket: str, key: str) -> pd.DataFrame:
    """
    Reads an S3 file into a pandas DataFrame. Supports xlsx, csv, json, and parquet formats.
    """
    s3_client = boto3.client('s3')
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        raise Exception(f"Failed to get file from S3: {e}")

    file_extension = key.split('.')[-1].lower()

    if file_extension == 'csv':
        return pd.read_csv(obj['Body'])
    elif file_extension == 'json':
        return pd.read_json(obj['Body'])
    elif file_extension == 'xlsx':
        return pd.read_excel(obj['Body'])
    elif file_extension == 'parquet':
        return pd.read_parquet(obj['Body'])
    else:
        raise Exception(f"Unsupported file format: {file_extension}")


def upload_dataframe_s3(df: pd.DataFrame, bucket: str, key: str, file_type: str) -> None:
    """
    Uploads a pandas DataFrame to S3 as the specified file type.
    """
    buffer = None
    s3_client = boto3.client('s3')
    try:
        if file_type == 'csv':
            buffer = df.to_csv(index=False)
        elif file_type == 'json':
            buffer = df.to_json()
        elif file_type == 'xlsx':
            buffer = df.to_excel(index=False, engine='xlsxwriter')
        elif file_type == 'parquet':
            buffer = df.to_parquet()
        else:
            raise Exception(f"Unsupported file type: {file_type}")

        response = s3_client.put_object(Bucket=bucket, Key=key, Body=buffer)
        print(f"Successfully uploaded:\n{response}")
    except (ClientError, NoCredentialsError) as e:
        raise Exception(f"Failed to upload DataFrame to S3: {e}")