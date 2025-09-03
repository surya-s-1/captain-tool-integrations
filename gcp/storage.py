import os
from google.cloud import storage

BUCKET_NAME = os.getenv('BUCKET_NAME')

bucket = storage.Client().bucket(bucket_name=BUCKET_NAME)


def upload_file_to_gcs(object, content_type, project_id, version, file_name):
    upload_path = f'projects/{project_id}/v_{version}/{file_name}'
    blob = bucket.blob(upload_path)
    blob.upload_from_file(file_obj=object, content_type=content_type)
    return f'gs://{BUCKET_NAME}/{upload_path}'
