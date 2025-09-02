import os
import json
from typing import Dict
from fastapi import APIRouter, HTTPException, Request, Depends, status
from fastapi.responses import RedirectResponse

from auth import get_current_user

from gcp.firestore import FirestoreDB
from gcp.secret_manager import SecretManager

from tools.jira.client import JiraClient

FRONTEND_REDIRECT_URL = os.getenv('FRONTEND_REDIRECT_URL')

db = FirestoreDB()
sm = SecretManager()

jira_client = JiraClient()

router = APIRouter(tags=['Jira Integration'])


@router.get('/status')
async def jira_status(user: Dict = Depends(get_current_user)):
    try:
        uid = user.get('uid', None)
        jira_connected = db.get_connection_status('jira', uid)
        return {'connected': jira_connected}

    except Exception as e:
        print(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail='Failed to retrieve Jira connection status.',
        )


@router.post('/connect')
async def jira_connect(user: Dict = Depends(get_current_user)):
    '''
    Initiates the Jira OAuth 2.0 (3LO) authorization flow.
    Returns a redirect URL for the user to authorize the application.
    '''
    try:
        uid = user.get('uid', None)

        if not uid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='User not authenticated.',
            )

        state = f'user_uid_{uid}'

        auth_url = jira_client.get_authorization_url(state)

        # Save the auth state using the new generic method
        db.save_auth_state(tool_name='jira', uid=uid, state=state)

        return {'redirect_url': auth_url}

    except Exception as e:
        print(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@router.get('/auth/callback')
async def jira_auth_callback(request: Request):
    '''
    Receives the authorization code from Jira, exchanges it for tokens,
    and stores them securely.
    '''
    auth_code = request.query_params.get('code')
    state = request.query_params.get('state')

    if not auth_code or not state:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Missing \'code\' or \'state\' parameter.',
        )

    # Extract the uid from the state parameter
    try:
        uid = state.split('_')[-1]
    except IndexError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Invalid state parameter format.',
        )

    # Validate the state parameter using the new generic method
    expected_state = db.get_auth_state(tool_name='jira', uid=uid)

    if state != expected_state:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid state parameter.'
        )

    try:
        tokens = jira_client.get_access_tokens(auth_code)

        # Store the secrets using the generic Secret Manager method
        secret_name = f'jira-tokens-{uid}'
        secret_path = sm.store_secret(secret_name, json.dumps(tokens))

        # Save the secret path using the generic Firestore method
        db.save_secret_path(tool_name='jira', uid=uid, secret_path=secret_path)

        return RedirectResponse(url=f'{FRONTEND_REDIRECT_URL}')

    except Exception as e:
        print(f'Error during Jira OAuth callback: {e}')

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail='Failed to authenticate with Jira.',
        )


@router.get('/projects/list')
async def get_jira_projects(user: Dict = Depends(get_current_user)):
    '''
    Fetches the list of Jira projects for a connected user.
    '''
    uid = user.get('uid', None)

    if not uid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail='User not authenticated.'
        )

    try:
        # Get the secret path using the generic Firestore method
        secret_path_doc = db.get_secret_path(tool_name='jira', uid=uid)
        if not secret_path_doc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail='User not connected to Jira.',
            )

        secret_path = secret_path_doc.get('secret_path')

        # Get the tokens using the generic Secret Manager method
        tokens_json = sm.get_secret(secret_path)
        tokens = json.loads(tokens_json)

        access_token = tokens['access_token']
        refresh_token = tokens['refresh_token']

        cloud_ids = jira_client.get_cloud_ids(tokens['access_token'])

        if cloud_ids.status_code == 401:
            print('Access token expired, attempting to refresh...')
            new_tokens = jira_client.refresh_access_token(refresh_token)

            secret_name = secret_path.split('/')[-1]
            sm.store_secret(secret_name, json.dumps(new_tokens))

        tokens_json = sm.get_secret(secret_path)
        tokens = json.loads(tokens_json)

        access_token = tokens['access_token']
        refresh_token = tokens['refresh_token']

        cloud_ids = jira_client.get_cloud_ids(tokens['access_token'])
        cloud_ids = [
            {
                'id': r['id'], 
                'name': r['name'], 
                'url': r['url']
            }
            for r in cloud_ids.json()
        ]

        projects = jira_client.get_projects(access_token, cloud_ids)

        return projects

    except Exception as e:
        print(f'Error fetching Jira projects: {e}')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail='Failed to retrieve Jira projects.',
        )
