from google.cloud import firestore
import os

GOOGLE_CLOUD_PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')
FIRESTORE_DATABASE = os.getenv('FIRESTORE_DATABASE')


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

    def create_project(self, tool_name, site_domain, site_id, project_key, project_name):
        project_doc_ref = self.db.collection('projects').document()
        project_doc_ref.set(
            {
                'tool': tool_name,
                'toolSiteDomain': site_domain,
                'toolSiteId': site_id,
                'toolProjectName': project_name,
                'toolProjectKey': project_key,
                'project_id': project_doc_ref.id,
                'created_at': firestore.SERVER_TIMESTAMP,
            }
        )

        versions_doc_ref = project_doc_ref.collection('versions').document()
        versions_doc_ref.set(
            {
                'version': versions_doc_ref.id,
                'project_name': project_name,
                'project_id': project_doc_ref.id,
                'status': 'CREATED',
                'created_at': firestore.SERVER_TIMESTAMP,
            }
        )

        project_doc_ref.update({'latest_version': versions_doc_ref.id})

    def get_project_details(self, project_id):
        '''
        Fetches a project document to retrieve key details like the Jira project key.
        '''
        doc_ref = self.db.collection('projects').document(project_id)
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        return None

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

    def update_version(self, project_id, version, update_details):
        version_ref = (
            self.db.collection('projects')
            .document(project_id)
            .collection('versions')
            .document(version)
        )
        version_ref.update(update_details)

    def update_requirement(self, project_id, version_id, req_id, update_details):
        '''
        Updates a specific test case document with its new tool details.
        '''
        doc_ref = (
            self.db.collection('projects')
            .document(project_id)
            .collection('versions')
            .document(version_id)
            .collection('requirements')
            .document(req_id)
        )
        doc_ref.update(update_details)

    def update_testcase(
        self, project_id, version_id, testcase_id, update_details
    ):
        '''
        Updates a specific test case document with its new tool details.
        '''
        doc_ref = (
            self.db.collection('projects')
            .document(project_id)
            .collection('versions')
            .document(version_id)
            .collection('testcases')
            .document(testcase_id)
        )
        doc_ref.update(update_details)
