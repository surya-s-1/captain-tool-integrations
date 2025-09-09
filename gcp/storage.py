import os
from google.cloud import storage

BUCKET_NAME = os.getenv('BUCKET_NAME')

bucket = storage.Client().bucket(bucket_name=BUCKET_NAME)


def upload_file_to_gcs(object, content_type, upload_path):
    blob = bucket.blob(upload_path)
    blob.upload_from_file(file_obj=object, content_type=content_type)
    return f'gs://{BUCKET_NAME}/{upload_path}'
