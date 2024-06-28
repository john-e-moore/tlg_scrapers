import hashlib

def compute_md5(obj: str) -> str:
    """Calculate the MD5 hash of a string object."""
    hash_md5 = hashlib.md5()
    hash_md5.update(obj)
    return hash_md5.hexdigest()

def has_data_changed(s3, s3_bucket, s3_key, data):
    """
    Compares MD5 hash of 'data' to the etag of the S3 file, which is also 
    an MD5 hash.
    
    :params:
    s3: S3Utility object from common.s3_utils
    s3_bucket: S3 bucket
    s3_key: S3 key
    data: any data object; in this case the .xlsx response from a GET request.

    :returns:
    True if data has changed, False if not.
    """
    existing_data_md5 = s3.download_etag(s3_bucket, s3_key)
    new_data_md5 = compute_md5(data)

    if existing_data_md5 != new_data_md5:
        return True
    else:
        return False