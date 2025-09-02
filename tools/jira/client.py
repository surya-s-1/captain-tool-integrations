import requests
import json
import os
from urllib.parse import urlencode

JIRA_CLIENT_ID = os.getenv('JIRA_CLIENT_ID')
JIRA_CLIENT_SECRET = os.getenv('JIRA_CLIENT_SECRET')
JIRA_REDIRECT_URI = os.getenv('JIRA_REDIRECT_URI')

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
        scopes = 'read:jira-work read:jira-user write:jira-work'
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
        '''
        Uses the refresh token to get a new access token.
        '''
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

    def get_projects(self, access_token):
        '''
        Fetches the list of all projects visible to the user.
        '''
        url = f'{self.base_api_url}/ex/jira/rest/api/3/project'
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
        }
        response = requests.get(url, headers=headers)
        return response
