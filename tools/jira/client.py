import time
import requests
import json
import os
import logging
from urllib.parse import urlencode

from gcp.firestore import FirestoreDB
from gcp.secret_manager import SecretManager

logger = logging.getLogger(__name__)

JIRA_CLIENT_ID = os.getenv('JIRA_CLIENT_ID')
JIRA_CLIENT_SECRET = os.getenv('JIRA_CLIENT_SECRET')
JIRA_REDIRECT_URI = os.getenv('JIRA_REDIRECT_URI')


db = FirestoreDB()
sm = SecretManager()


class JiraClient:
    '''
    A generic, reusable Jira API client.
    Handles OAuth, access token refresh, and generic CRUD for Jira issues.
    '''

    def __init__(self):
        self.client_id = JIRA_CLIENT_ID
        self.client_secret = JIRA_CLIENT_SECRET
        self.redirect_uri = JIRA_REDIRECT_URI
        self.base_auth_url = 'https://auth.atlassian.com'
        self.base_api_url = 'https://api.atlassian.com'

    # ---------- AUTH FLOW ----------

    def get_authorization_url(self, state):
        scopes = 'read:jira-work read:jira-user write:jira-work offline_access'
        params = {
            'audience': 'api.atlassian.com',
            'client_id': self.client_id,
            'scope': scopes,
            'redirect_uri': self.redirect_uri,
            'state': state,
            'response_type': 'code',
            'prompt': 'consent',
        }
        return f'{self.base_auth_url}/authorize?{urlencode(params)}'

    def get_access_tokens(self, auth_code):
        url = f'{self.base_auth_url}/oauth/token'
        headers = {'Content-Type': 'application/json'}
        data = {
            'grant_type': 'authorization_code',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'code': auth_code,
            'redirect_uri': self.redirect_uri,
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        return response.json()

    def refresh_access_token(self, refresh_token):
        url = f'{self.base_auth_url}/oauth/token'
        headers = {'Content-Type': 'application/json'}
        data = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': refresh_token,
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        return response.json()

    def get_usage_access_token(self, uid, new_set=False):
        secret_path_doc = db.get_secret_path(tool_name='jira', uid=uid)
        if not secret_path_doc:
            logger.warning(f'No Jira secret path found for UID {uid}.')
            return None

        secret_path = secret_path_doc['secret_path']
        tokens_json = sm.get_secret(secret_path)
        tokens = json.loads(tokens_json)

        if not new_set:
            return tokens['access_token']

        refresh_token = tokens['refresh_token']
        new_tokens = self.refresh_access_token(refresh_token)
        secret_name = secret_path.split('/')[-1]
        sm.store_secret(secret_name, json.dumps(new_tokens))
        updated_tokens = json.loads(sm.get_secret(secret_path))
        return updated_tokens['access_token']

    def get_cloud_ids(self, uid):
        '''
        Fetches the user's cloud ID from the Atlassian platform.
        '''
        url = f'{self.base_api_url}/oauth/token/accessible-resources'
        access_token = self.get_usage_access_token(uid=uid)

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 401:
            logger.info('Access token expired. Refreshing...')
            access_token = self.get_usage_access_token(uid=uid, new_set=True)
            headers['Authorization'] = access_token
            response = requests.get(url, headers=headers)

        response.raise_for_status()

        return response.json()

    def get_projects(self, uid, cloud_id, cloud_url):
        access_token = self.get_usage_access_token(uid=uid)

        url = f'{self.base_api_url}/ex/jira/{cloud_id}/rest/api/3/project/search'
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 401:
            logger.info('Access token expired. Refreshing...')
            access_token = self.get_usage_access_token(uid=uid, new_set=True)
            headers['Authorization'] = access_token
            response = requests.get(url, headers=headers)

        response.raise_for_status()

        current_projects = response.json().get('values', [])

        for project in current_projects:
            project['siteId'] = cloud_id
            project['siteDomain'] = cloud_url

        return current_projects

    # ---------- GENERIC ISSUE OPERATIONS ----------

    def create_issue(self, uid, cloud_id, issue):
        return self._post(uid, cloud_id, '/rest/api/3/issue', issue)

    def create_bulk_issues(self, uid, cloud_id, issues):
        payload = {'issueUpdates': issues}
        return self._post(uid, cloud_id, '/rest/api/3/issue/bulk', payload)

    def update_issue(self, uid, cloud_id, issue_key, update_fields):
        return self._put(
            uid, cloud_id, f'/rest/api/3/issue/{issue_key}', {'fields': update_fields}
        )

    def search_issues(self, uid, cloud_domain, cloud_id, jql, max_results=100):
        issues = []

        payload = {
            'jql': jql,
            'maxResults': max_results,
            'fields': ['key', 'labels']
        }

        response = self._post(uid, cloud_id, '/rest/api/3/search/jql', payload)

        for issue in response.get('issues', []):
            issue_key = issue.get('key')
            labels = issue.get('fields', {}).get('labels', [])
            issues.append(
                {
                    'key': issue_key,
                    'url': f'{cloud_domain}/browse/{issue_key}',
                    'labels': labels,
                }
            )

        return issues

    # ---------- PRIVATE HELPERS ----------

    def _post(self, uid, cloud_id, endpoint, payload):
        access_token = self.get_usage_access_token(uid)

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

        url = f'{self.base_api_url}/ex/jira/{cloud_id}{endpoint}'

        response = requests.post(url, headers=headers, data=json.dumps(payload))

        if response.status_code == 401:
            logger.info('Access token expired. Refreshing...')
            access_token = self.get_usage_access_token(uid, new_set=True)
            headers['Authorization'] = f'Bearer {access_token}'
            response = requests.post(url, headers=headers, data=json.dumps(payload))
        
        if response.status_code == 400 and response.json():
            logger.error(response.json())

        response.raise_for_status()

        return response.json() if response.content else {}

    def _put(self, uid, cloud_id, endpoint, payload, new_set=False):
        access_token = self.get_usage_access_token(uid, new_set=new_set)

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

        url = f'{self.base_api_url}/ex/jira/{cloud_id}{endpoint}'

        for attempt in range(5):
            response = requests.put(url, headers=headers, data=json.dumps(payload))

            if response.status_code == 401:
                logger.info('Access token expired, refreshing...')
                return self._put(uid, cloud_id, endpoint, payload, new_set=True)

            elif response.status_code == 204:
                return True

            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 5))
                time.sleep(retry_after)

            else:
                response.raise_for_status()

        return False
