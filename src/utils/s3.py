import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from botocore.exceptions import ClientError, BotoCoreError

################################################################################
# 
################################################################################
def get_most_recently_modified_file(bucket, prefix):
    """
    Find the most recently modified file within an S3 bucket with the given prefix.

    Parameters:
    bucket (str): The name of the S3 bucket.
    prefix (str): The prefix used to filter the S3 keys.

    Returns:
    str: The key of the most recently modified file, or None if no files are found.
    """
    s3_client = boto3.client('s3')

    try:
        # Initialize variables
        latest_modification = None
        latest_filename = None

        # List objects within the bucket with the specified prefix
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        # Go through the returned objects and track the most recent
        for obj in response.get('Contents', []):
            if latest_modification is None or obj['LastModified'] > latest_modification:
                latest_modification = obj['LastModified']
                latest_filename = obj['Key']

        return latest_filename
    except NoCredentialsError:
        print("Credentials not available")
        return None
    except PartialCredentialsError:
        print("Incomplete credentials")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

################################################################################
# 
################################################################################
def upload_local_file_s3(local_file: str, s3_bucket: str, s3_key: str) -> bool:
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    file_without_extension = local_file.split('.')[0].replace('tmp/', '')
    full_key = f'{s3_key}{file_without_extension}_{timestamp}.xlsx'
    try:
        s3_client = boto3.client('s3')
        with open(local_file, 'rb') as file:
            
            response = s3_client.put_object(Bucket=s3_bucket, Key=full_key, Body=file)
        print(f"Successfully uploaded: {response}")
        return True
    except ClientError as e:
        print(f"Client error occurred:\n{e}")
        return False
    except BotoCoreError as e:
        print(f"BotoCore error occurred:\n{e}")
        return False
    except Exception as e:
        print(f"Unexpected error occurred:\n{e}")
        return False
    