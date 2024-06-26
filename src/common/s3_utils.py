import re
import boto3
import pandas as pd
from typing import Any
from datetime import datetime
from botocore.exceptions import NoCredentialsError, ClientError

class S3Utility:
    @staticmethod
    def get_latest_file_key(bucket: str, prefix: str) -> str:
        """
        Retrieves the most recent file (based on Last Modified date) from an S3 bucket for a given prefix.
        """
        s3_client = boto3.client('s3')
        try:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            files = response.get('Contents', [])
        except ClientError as e:
            raise Exception(f"Failed to list files in S3: {e}")

        if not files:
            raise Exception("No files found under the specified bucket and prefix.")

        latest_file = max(files, key=lambda x: x['LastModified'])
        return latest_file['Key']
    
    @staticmethod
    def download_etag(bucket: str, key: str) -> str:
        s3_client = boto3.client('s3')
        try:
            s3_object = s3_client.head_object(Bucket=bucket, Key=key)
            s3_etag = s3_object['ETag'].strip('"')  # Remove double quotes from ETag
            return s3_etag
        except s3_client.exceptions.NoSuchKey:
            print("The object does not exist in S3.")
            return None 
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    @staticmethod
    def upload_local_file_to_s3(local_file_path: str, bucket: str, key: str) -> None:
        """
        Uploads a file from the local file system to an S3 bucket.
        """
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(local_file_path, bucket, key)
            print(f"Successfully uploaded:\n{response}")
        except (ClientError, NoCredentialsError) as e:
            raise Exception(f"Failed to upload file to S3: {e}")

    @staticmethod
    def download_s3_file_to_local(local_file_path: str, bucket: str, key: str) -> None:
        """
        Downloads a file from an S3 bucket to the local file system.
        """
        s3_client = boto3.client('s3')
        try:
            s3_client.download_file(bucket, key, local_file_path)
        except ClientError as e:
            raise Exception(f"Failed to download file from S3: {e}")

    @staticmethod
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

    @staticmethod
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
    
    @staticmethod
    def upload_obj_s3(bucket: str, key: str, obj: str) -> None:
        """
        Uploads an object to S3.
        """
        s3_client = boto3.client('s3')
        try:
            response = s3_client.put_object(Bucket=bucket, Key=key, Body=obj)
            print(f"Successfully uploaded:\n{response}")
        except (ClientError, NoCredentialsError) as e:
            raise Exception(f"Failed to upload object to S3: {e}")
        
    @staticmethod
    def replace_timestamp_in_filename(filename: str) -> str:
        """
        Replaces an old timestamp in the file name with the current timestamp.
        The timestamp format is assumed to be YYYYMMDD.

        :param filename: The original file name with the old timestamp.
        :return: The new file name with the current timestamp.
        """
        current_timestamp = datetime.now().strftime("%Y%m%d")
        pattern = r'(\d{8})(?=\.\w+$)'  # Looks for 8 digits before the file extension
        # Replace the old timestamp with the current timestamp
        new_filename = re.sub(pattern, current_timestamp, filename)

        return new_filename

            
