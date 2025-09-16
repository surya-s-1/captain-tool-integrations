import requests
import json
import os
from urllib.parse import urlencode

import logging

JIRA_CLIENT_ID = os.getenv('JIRA_CLIENT_ID')
JIRA_CLIENT_SECRET = os.getenv('JIRA_CLIENT_SECRET')
JIRA_REDIRECT_URI = os.getenv('JIRA_REDIRECT_URI')

from gcp.firestore import FirestoreDB
from gcp.secret_manager import SecretManager

logger = logging.getLogger(__name__)

db = FirestoreDB()
sm = SecretManager()


class JiraClient:
    '''
    Handles all interactions with the Jira REST API, including OAuth flows.
    '''

    def __init__(self):
        self.client_id = JIRA_CLIENT_ID
        self.client_secret = JIRA_CLIENT_SECRET
        self.redirect_uri = JIRA_REDIRECT_URI
        self.base_auth_url = 'https://auth.atlassian.com'
        self.base_api_url = 'https://api.atlassian.com'

    def get_authorization_url(self, state):
        '''
        Generates the URL to redirect the user for Jira authorization.
        '''
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
        '''
        Exchanges the authorization code for access and refresh tokens.
        '''
        try:
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
        except requests.exceptions.RequestException as e:
            logger.exception('Error exchanging authorization code for tokens.')
            raise

    def refresh_access_token(self, refresh_token):
        '''
        Uses the refresh token to get a new access token.
        '''
        try:
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

        except requests.exceptions.RequestException as e:
            logger.exception('Error refreshing access token.')
            raise

    def get_cloud_ids(self, access_token):
        '''
        Fetches the user's cloud ID from the Atlassian platform.
        '''
        try:
            url = f'{self.base_api_url}/oauth/token/accessible-resources'
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json',
            }
            response = requests.get(url, headers=headers)
            return response
        except requests.exceptions.RequestException as e:
            logger.exception('Error fetching cloud IDs.')
            raise

    def get_projects(self, access_token, cloud_ids):
        '''
        Fetches the list of all projects visible to the user.
        '''
        projects = []
        for cloud_id in cloud_ids:
            url = f'{self.base_api_url}/ex/jira/{cloud_id.get('id')}/rest/api/3/project/search'
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json',
            }
            response = requests.get(url, headers=headers)

            response.raise_for_status()

            current_projects = response.json().get('values', [])

            for project in current_projects:
                project['siteId'] = cloud_id.get('id')
                project['siteDomain'] = cloud_id.get('url')

            projects.extend(current_projects)

        return projects

    def create_bulk_requirements(self, uid, cloud_id, project_key, requirements):
        access_token = self.get_usage_access_token(uid)
        if not access_token:
            raise Exception('Access token not found')

        url = f'{self.base_api_url}/ex/jira/{cloud_id}/rest/api/3/issue/bulk'
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

        issue_updates = []
        for requirement in requirements:
            if (
                not requirement.get('requirement_id')
                or not requirement.get('requirement_title')
                or not requirement.get('requirement')
            ):
                continue

            description_text = requirement.get('requirement', '')

            issue_payload = {
                'fields': {
                    'project': {'key': project_key},
                    'summary': requirement.get('requirement_title'),
                    'description': {
                        'type': 'doc',
                        'version': 1,
                        'content': [
                            {
                                'type': 'paragraph',
                                'content': [
                                    {
                                        'text': description_text,
                                        'type': 'text',
                                    }
                                ],
                            }
                        ],
                    },
                    'issuetype': {'name': 'Task'},
                    'priority': {'name': requirement.get('priority', 'Medium')},
                    'labels': [
                        'AI_Generated',
                        'Created_by_Captain',
                        'Requirement',
                        requirement.get('requirement_id'),
                    ],
                }
            }
            issue_updates.append(issue_payload)

        payload = {'issueUpdates': issue_updates}

        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 401:
            print('Access token expired, attempting to refresh...')

            access_token = self.get_usage_access_token(uid, new_set=True)
            headers['Authorization'] = f'Bearer {access_token}'

            response = requests.post(url, headers=headers, data=json.dumps(payload))
            response.raise_for_status()

        return response.json()

    def create_bulk_testcases(
        self, uid, cloud_id, project_key, requirement_key_mapping, testcases
    ):
        '''
        Constructs a bulk payload and creates multiple testcases as tasks in Jira.
        '''
        access_token = self.get_usage_access_token(uid)
        if not access_token:
            raise Exception('Access token not found')

        url = f'{self.base_api_url}/ex/jira/{cloud_id}/rest/api/3/issue/bulk'
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

        issue_updates = []
        for testcase in testcases:
            if (
                not testcase.get('title')
                or not testcase.get('description')
                or not testcase.get('testcase_id')
            ):
                continue

            acceptance_criteria = testcase.get('acceptance_criteria', '')

            # Build acceptance criteria into the description (if custom field not used)
            description_text = testcase.get('description', '')
            if acceptance_criteria:
                description_text += f'\n\n*Acceptance Criteria:*\n{acceptance_criteria}'

            parent_story_key = requirement_key_mapping.get(testcase.get('requirement_id'))

            issue_payload = {
                'fields': {
                    'project': {'key': project_key},
                    'summary': testcase.get('title'),
                    'description': {
                        'type': 'doc',
                        'version': 1,
                        'content': [
                            {
                                'type': 'paragraph',
                                'content': [
                                    {
                                        'text': description_text,
                                        'type': 'text',
                                    }
                                ],
                            }
                        ],
                    },
                    'issuetype': {'name': 'Task'},
                    'priority': {'name': testcase.get('priority', 'Medium')},
                    'labels': [
                        'AI_Generated',
                        'Created_by_Captain',
                        'Testcase',
                        testcase.get('testcase_id'),
                    ],
                }
            }

            if parent_story_key:
                issue_payload['fields']['parent']['key'] = parent_story_key

            issue_updates.append(issue_payload)

        payload = {'issueUpdates': issue_updates}

        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 401:
            print('Access token expired, attempting to refresh...')

            access_token = self.get_usage_access_token(uid, new_set=True)
            headers['Authorization'] = f'Bearer {access_token}'

            response = requests.post(url, headers=headers, data=json.dumps(payload))
            response.raise_for_status()

        return response.json()

    def search_issues_by_label(
        self, uid, cloud_domain, cloud_id, label, max_results_per_page=100
    ):
        '''
        Searches for all Jira issues with a specific label (across multiple pages).
        Returns a list of issues with their key, summary, and a direct issue URL.
        '''
        issues = []

        access_token = self.get_usage_access_token(uid)
        if not access_token:
            raise Exception('Access token not found')

        start_at = 0
        total = 1
        fetched = 0

        while fetched < total:
            url = f'{self.base_api_url}/ex/jira/{cloud_id}/rest/api/3/search'

            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }
            payload = {
                'jql': f'labels = \'{label}\'',
                'maxResults': max_results_per_page,
                'startAt': start_at,
                'fields': ['key', 'labels'],
            }

            response = requests.post(url, headers=headers, data=json.dumps(payload))

            if response.status_code == 401:
                print('Access token expired, attempting to refresh...')

                access_token = self.get_usage_access_token(uid, new_set=True)
                headers['Authorization'] = f'Bearer {access_token}'

                response = requests.post(url, headers=headers, data=json.dumps(payload))
                response.raise_for_status()

            query_result = response.json()

            current_issues = query_result.get('issues', [])

            for issue in current_issues:
                issue_key = issue.get('key')
                labels = issue.get('fields', {}).get('labels', [])

                issue_url = f'{cloud_domain}/browse/{issue_key}'

                issues.append({'key': issue_key, 'url': issue_url, 'labels': labels})

            total = query_result.get('total', 0)
            fetched += len(current_issues)
            start_at += len(current_issues)

        return issues

    def get_usage_access_token(self, uid, new_set=False):
        try:
            secret_path_doc = db.get_secret_path(tool_name='jira', uid=uid)
            if not secret_path_doc:
                logger.warning(f'No secret path found for Jira user {uid}.')
                return None

            secret_path = secret_path_doc.get('secret_path')
            tokens_json = sm.get_secret(secret_path)
            tokens = json.loads(tokens_json)

            if not new_set:
                return tokens['access_token']

            refresh_token = tokens['refresh_token']
            new_tokens = self.refresh_access_token(refresh_token)
            secret_name = secret_path.split('/')[-1]
            sm.store_secret(secret_name, json.dumps(new_tokens))
            tokens_json = sm.get_secret(secret_path)
            tokens = json.loads(tokens_json)
            return tokens['access_token']
        except Exception as e:
            logger.exception(
                f'Error getting or refreshing Jira access token for user {uid}.'
            )
            raise
