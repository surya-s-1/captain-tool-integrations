from google.cloud import secretmanager
import logging
import os

GOOGLE_CLOUD_PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

class SecretManager:
    '''
    A client for interacting with Google Cloud Secret Manager.
    This module is tool-agnostic and uses the GOOGLE_CLOUD_PROJECT
    environment variable for initialization.
    '''

    def __init__(self):
        if not GOOGLE_CLOUD_PROJECT:
            raise ValueError('GOOGLE_CLOUD_PROJECT environment variable not set.')

        self.client = secretmanager.SecretManagerServiceClient()

    def store_secret(self, secret_name, payload):
        '''
        Stores a payload as a new version of a secret.
        The secret is named dynamically based on the secret_name.
        '''
        parent = f'projects/{GOOGLE_CLOUD_PROJECT}'
        secret_path = f'{parent}/secrets/{secret_name}'

        # Check if the secret exists, and create it if it doesn't.
        try:
            self.client.get_secret(request={'name': secret_path})
        except Exception:
            logger.info(f'Secret \'{secret_name}\' not found. Creating a new one...')
            self.client.create_secret(
                request={
                    'parent': parent,
                    'secret_id': secret_name,
                    'secret': {'replication': {'automatic': {}}},
                }
            )

        # Add the new payload as a new version.
        self.client.add_secret_version(
            request={
                'parent': secret_path,
                'payload': {'data': payload.encode('UTF-8')},
            }
        )
        
        return secret_path

    def get_secret(self, secret_path):
        '''
        Retrieves the latest version of a secret given its full path.
        '''
        name = f'{secret_path}/versions/latest'
        response = self.client.access_secret_version(request={'name': name})
        return response.payload.data.decode('UTF-8')
