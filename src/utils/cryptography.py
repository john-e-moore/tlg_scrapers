import boto3
import hashlib

################################################################################
# 
################################################################################
def calculate_md5(filepath: str) -> str:
    """Calculate the MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

################################################################################
# 
################################################################################
def check_file_change(local_file: str, bucket_name: str, object_key: str) -> bool:
    """Check if a local file is different from the version stored in S3."""
    s3_client = boto3.client('s3')

    # Calculate the MD5 hash of the local file
    local_file_md5 = calculate_md5(local_file)

    # Get the ETag of the S3 object
    try:
        s3_object = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        s3_etag = s3_object['ETag'].strip('"')  # Remove double quotes from ETag

        if local_file_md5 == s3_etag:
            print("The local file is identical to the S3 object.")
            return False
        else:
            print("The local file is different from the S3 object.")
            return True
    except s3_client.exceptions.NoSuchKey:
        print("The object does not exist in S3.")
        return True  # If there is no such object, consider it a change
    except Exception as e:
        print(f"An error occurred: {e}")
        return True  # Treat errors as a change for safety