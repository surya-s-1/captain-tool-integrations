import os
from google.cloud import storage

BUCKET_NAME = os.getenv('BUCKET_NAME')

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name=BUCKET_NAME)


def upload_file_to_gcs(object, content_type, upload_path):
    blob = bucket.blob(upload_path)
    blob.upload_from_file(file_obj=object, content_type=content_type)
    return f'gs://{BUCKET_NAME}/{upload_path}'


# --- New function for downloading files ---
def get_file_from_gcs(gs_url: str):
    """
    Downloads a file from a GCS URL and returns its content as bytes.
    """
    if not gs_url.startswith("gs://"):
        raise ValueError("Invalid GCS URL format. Must start with 'gs://'")

    # Extract bucket name and blob path from the URL
    path_parts = gs_url[5:].split("/", 1)
    bucket_name = path_parts[0]
    blob_path = path_parts[1]

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return blob.download_as_bytes()
