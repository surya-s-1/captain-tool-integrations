import os
import copy
import datetime
from logging import Logger
from google.cloud import firestore

GOOGLE_CLOUD_PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')
FIRESTORE_DATABASE = os.getenv('FIRESTORE_DATABASE')
REQUIREMENTS_COLLECTION = 'requirements'
TESTCASES_COLLECTION = 'testcases'

logger = Logger(__name__)


class FirestoreDB:
    '''
    A client for interacting with Google Cloud Firestore.
    This module is tool-agnostic and uses the GOOGLE_CLOUD_PROJECT
    environment variable for initialization.
    '''

    def __init__(self):
        self.db = firestore.Client(
            project=GOOGLE_CLOUD_PROJECT, database=FIRESTORE_DATABASE
        )

    def get_connection_status(self, tool_name, uid):
        doc_ref = self.db.collection('secrets', 'tools', tool_name).document(uid)
        doc = doc_ref.get()
        return doc.exists

    def save_auth_state(self, tool_name, uid, state):
        '''
        Saves the state parameter for a user in a tool-specific collection.
        This is for validating the OAuth callback.
        '''
        doc_ref = self.db.collection('tools', tool_name, 'auth_states').document(uid)
        doc_ref.set({'state': state})

    def get_auth_state(self, tool_name, uid):
        '''
        Retrieves and deletes the state parameter for a user.
        '''
        doc_ref = self.db.collection('tools', tool_name, 'auth_states').document(uid)
        doc = doc_ref.get()
        if doc.exists:
            state = doc.to_dict().get('state')
            doc_ref.delete()
            return state
        return None

    def save_secret_path(self, tool_name, uid, secret_path):
        '''
        Saves the path to the Secret Manager secret for a user.
        This uses the user's requested structure: a collection with uid and secret_path fields.
        '''
        doc_ref = self.db.collection('secrets', 'tools', tool_name).document(uid)
        doc_ref.set({'uid': uid, 'secret_path': secret_path})

    def get_secret_path(self, tool_name, uid):
        '''
        Retrieves the Secret Manager path for a user by querying on the uid.
        This is a less direct lookup than by document ID.
        '''
        collection_ref = self.db.collection('secrets', 'tools', tool_name)
        query = collection_ref.where('uid', '==', uid).limit(1)
        docs = query.get()
        for doc in docs:
            return doc.to_dict()
        return None

    def create_project(
        self, uid, tool_name, site_domain, site_id, project_key, project_name
    ):
        project_doc_ref = self.db.collection('projects').document()
        project_doc_ref.set(
            {
                'tool': tool_name,
                'toolSiteDomain': site_domain,
                'toolSiteId': site_id,
                'toolProjectName': project_name,
                'toolProjectKey': project_key,
                'project_id': project_doc_ref.id,
                'created_by': uid,
                'uids': [uid],
                'created_at': firestore.SERVER_TIMESTAMP,
            }
        )

        versions_doc_ref = project_doc_ref.collection('versions').document('1')
        versions_doc_ref.set(
            {
                'version': versions_doc_ref.id,
                'project_name': project_name,
                'project_id': project_doc_ref.id,
                'status': 'CREATED',
                'created_by': uid,
                'created_at': firestore.SERVER_TIMESTAMP,
            }
        )

        project_doc_ref.update({'latest_version': versions_doc_ref.id})

    def find_project_id_by_details(self, tool_name, site_domain, site_id, project_key):
        collection_ref = self.db.collection('projects')
        query = (
            collection_ref.where('tool', '==', tool_name)
            .where('toolSiteDomain', '==', site_domain)
            .where('toolSiteId', '==', site_id)
            .where('toolProjectKey', '==', project_key)
            .limit(1)
        )

        docs = query.get()

        if docs:
            return docs[0].id

        return None

    def get_connected_projects(self, uid):
        collection_ref = self.db.collection('projects')
        query = collection_ref.where('uids', 'array_contains', uid)

        docs = query.get()

        return [doc.to_dict() for doc in docs]

    def get_project_details(self, project_id):
        '''
        Fetches a project document to retrieve key details like the Jira project key.
        '''
        doc_ref = self.db.collection('projects').document(project_id)
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        return None

    def get_version_details(self, project_id, version_id):
        '''
        Fetches a specific version document.
        '''
        doc_ref = self.db.document(f'projects/{project_id}/versions/{version_id}')
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        return None

    def get_requirements(self, project_id, version_id):
        '''
        Fetches all requirements for a given project and version.
        '''
        collection_ref = (
            self.db.collection('projects')
            .document(project_id)
            .collection('versions')
            .document(version_id)
            .collection('requirements')
        )
        return [doc.to_dict() for doc in collection_ref.get()]

    def get_testcases(self, project_id, version_id):
        '''
        Fetches all test cases for a given project and version.
        '''
        collection_ref = (
            self.db.collection('projects')
            .document(project_id)
            .collection('versions')
            .document(version_id)
            .collection('testcases')
        )
        return [doc.to_dict() for doc in collection_ref.get()]

    def get_testcase_details(self, project_id, version_id, testcase_id):
        '''
        Fetches a specific test case document.
        '''
        doc_ref = (
            self.db.collection('projects')
            .document(project_id)
            .collection('versions')
            .document(version_id)
            .collection('testcases')
            .document(testcase_id)
        )
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        return None

    def update_project_users(self, project_id, uid):
        version_ref = self.db.collection('projects').document(project_id)
        version_ref.update({'uids': firestore.ArrayUnion([uid])})

    def update_project_details(self, project_id, update_details):
        project_ref = self.db.collection('projects').document(project_id)
        project_ref.update(update_details)

    def update_version(self, project_id, version, update_details):
        version_ref = (
            self.db.collection('projects')
            .document(project_id)
            .collection('versions')
            .document(version)
        )
        version_ref.update(update_details)

    def update_requirement(self, project_id, version, req_id, update_details):
        '''
        Updates a specific test case document with its new tool details.
        '''
        doc_ref = self.db.document(
            'projects', project_id, 'versions', version, 'requirements', req_id
        )
        doc_ref.update(update_details)

    def update_testcase(self, project_id, version_id, testcase_id, update_details):
        '''
        Updates a specific test case document with its new tool details.
        '''
        doc_ref = self.db.document(
            'projects', project_id, 'versions', version_id, 'testcases', testcase_id
        )
        doc_ref.update(update_details)

    # --- New Methods for Async Job Management ---
    def create_download_job(
        self, uid: str, project_id: str, version: str, testcase_id: str
    ):
        '''
        Creates a new document in the 'jobs' collection to track a download task.
        '''
        job_ref = self.db.collection('jobs').document()
        job_ref.set(
            {
                'project_id': project_id,
                'version': version,
                'testcase_id': testcase_id,
                'uid': uid,
                'job_id': job_ref.id,
                'created_at': firestore.SERVER_TIMESTAMP,
                'status': 'pending',
                'created_at': firestore.SERVER_TIMESTAMP,
            }
        )

        return job_ref.id

    def create_download_all_job(self, uid: str, project_id: str, version: str):
        '''
        Creates a new document in the 'jobs' collection to track a download task.
        '''
        job_ref = self.db.collection('jobs').document()
        job_ref.set(
            {
                'project_id': project_id,
                'version': version,
                'testcase_id': 'all',
                'uid': uid,
                'job_id': job_ref.id,
                'created_at': firestore.SERVER_TIMESTAMP,
                'status': 'pending',
                'created_at': firestore.SERVER_TIMESTAMP,
            }
        )

        return job_ref.id

    def get_download_job(self, job_id: str):
        '''
        Retrieves a download job document.
        '''
        job_ref = self.db.collection('jobs').document(job_id)
        job_doc = job_ref.get()
        return job_doc.to_dict() if job_doc.exists else None

    def update_download_job_status(
        self,
        job_id: str,
        status: str,
        file_name: str = None,
        result_url: str = None,
        error: str = None,
    ):
        '''
        Updates the status and optional results or errors for a download job.
        '''
        job_ref = self.db.collection('jobs').document(job_id)
        update_data = {'status': status}
        if result_url:
            update_data['result_url'] = result_url
            update_data['completed_at'] = firestore.SERVER_TIMESTAMP
        if file_name:
            update_data['file_name'] = file_name
        if error:
            update_data['error'] = error
        job_ref.update(update_data)

    def create_new_project_version(self, project_id: str, uid: str):
        try:
            project_data = self.db.document(f'projects/{project_id}').get()

            prev_version_id: str = project_data.get('latest_version')
            prev_version_id_num = int(prev_version_id)
            new_version_id_num = prev_version_id_num + 1

            new_version_ref = self.db.collection(
                f'projects/{project_id}/versions'
            ).document(f'{new_version_id_num}')

            new_version_ref.set(
                {
                    'version': new_version_ref.id,
                    'status': 'CREATED',
                    'project_id': project_id,
                    'project_name': project_data.get('toolProjectName'),
                    'created_by': uid,
                    'created_at': firestore.SERVER_TIMESTAMP,
                }
            )

            self.db.document(f'projects/{project_id}').update(
                {'latest_version': new_version_ref.id}
            )

            return prev_version_id, new_version_ref.id

        except Exception as e:
            logger.exception(f'Error creating new project version: {e}')
            raise

    def process_document_data(self, prev_version: str, doc_data: dict) -> dict:
        existing_history = []
        prev_history = doc_data.pop('history', [])

        if isinstance(prev_history, list):
            existing_history.extend(prev_history)

        current_state_entry = {
            'version': prev_version,
            'fields': copy.deepcopy(doc_data),
            'copied_at': datetime.datetime.now(datetime.timezone.utc),
        }

        new_history = [current_state_entry] + existing_history

        doc_data['history'] = new_history

        return doc_data

    def copy_requirements_and_testcases_with_history(
        self, project_id: str, prev_version: str, new_version: str
    ):
        source_doc_path = f'projects/{project_id}/versions/{prev_version}'
        target_doc_path = f'projects/{project_id}/versions/{new_version}'

        logger.info(f'Source: {source_doc_path}/{REQUIREMENTS_COLLECTION}')
        logger.info(f'Target: {target_doc_path}/{REQUIREMENTS_COLLECTION}')

        try:
            source_subcollection_ref = self.db.collection(
                f'{source_doc_path}/{REQUIREMENTS_COLLECTION}'
            )
            target_subcollection_ref = self.db.collection(
                f'{target_doc_path}/{REQUIREMENTS_COLLECTION}'
            )

            docs_to_copy = source_subcollection_ref.stream()

            batch = self.db.batch()
            copy_count = 0
            batch_size = 0

            for doc in docs_to_copy:
                doc_id = doc.id
                doc_data = doc.to_dict()

                processed_data = self.process_document_data(prev_version, doc_data)

                target_doc_ref = target_subcollection_ref.document(doc_id)
                batch.set(target_doc_ref, processed_data)

                copy_count += 1
                batch_size += 1

                if batch_size >= 499:
                    logger.info(f'Committing batch of {batch_size} documents...')
                    batch.commit()
                    batch = self.db.batch()  # Start a new batch
                    batch_size = 0

            if batch_size > 0:
                logger.info(f'Committing final batch of {batch_size} documents...')
                batch.commit()

            logger.info(f'Successfully copied {copy_count} documents.')

        except Exception as e:
            logger.exception(f'A critical error occurred: {e}')
            raise

        logger.info(f'Source: {source_doc_path}/{TESTCASES_COLLECTION}')
        logger.info(f'Target: {target_doc_path}/{TESTCASES_COLLECTION}')

        try:
            source_subcollection_ref = self.db.collection(
                f'{source_doc_path}/{TESTCASES_COLLECTION}'
            )
            target_subcollection_ref = self.db.collection(
                f'{target_doc_path}/{TESTCASES_COLLECTION}'
            )

            docs_to_copy = source_subcollection_ref.stream()

            batch = self.db.batch()
            copy_count = 0
            batch_size = 0

            for doc in docs_to_copy:
                doc_id = doc.id
                doc_data = doc.to_dict()

                processed_data = self.process_document_data(prev_version, doc_data)

                target_doc_ref = target_subcollection_ref.document(doc_id)
                batch.set(target_doc_ref, processed_data)

                copy_count += 1
                batch_size += 1

                if batch_size >= 499:
                    logger.info(f'Committing batch of {batch_size} documents...')
                    batch.commit()
                    batch = self.db.batch()  # Start a new batch
                    batch_size = 0

            if batch_size > 0:
                logger.info(f'Committing final batch of {batch_size} documents...')
                batch.commit()

            logger.info(f'Successfully copied {copy_count} documents.')

        except Exception as e:
            logger.exception(f'A critical error occurred: {e}')
            raise

    def delete_version(self, project_id: str, version: str):
        version_ref = self.db.collection(f'projects/{project_id}/versions').document(
            version
        )
        version_ref.delete()
        return None
