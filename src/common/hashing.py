import hashlib

def compute_md5(obj: str) -> str:
    """Calculate the MD5 hash of a string object."""
    hash_md5 = hashlib.md5()
    hash_md5.update(obj)
    return hash_md5.hexdigest()