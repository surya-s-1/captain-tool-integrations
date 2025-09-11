import os
import io
import uuid
import json
import requests
from typing import Dict, List
from fastapi import (
    APIRouter,
    HTTPException,
    Depends,
    status,
    UploadFile,
    File,
    BackgroundTasks,
    Body,
)
from starlette.responses import StreamingResponse

from auth import get_current_user
from tools.jira.client import JiraClient

from projects.models import ConnectProjectRequest
from projects.functions import create_on_jira, background_zip_task

from gcp.firestore import FirestoreDB
from gcp.secret_manager import SecretManager
from gcp.storage import upload_file_to_gcs, get_file_from_gcs  # New import for downloading files

from google.cloud.workflows.executions_v1beta import (
    Execution,
    ExecutionsClient,
    CreateExecutionRequest,
)
import google.auth.transport.requests as auth_requests
import google.oauth2.id_token as oauth2_id_token

REQUIREMENTS_WORFLOW = os.getenv('REQUIREMENTS_WORFLOW')
TESTCASE_CREATION_URL = os.getenv('TESTCASE_CREATION_URL')
DATASET_TASKS_DISPATHER_URL = os.getenv('DATASET_TASKS_DISPATHER_URL')

import logging

logger = logging.getLogger(__name__)

db = FirestoreDB()
sm = SecretManager()
jira_client = JiraClient()
router = APIRouter(tags=['Project Actions'])

workflow_client = ExecutionsClient()


@router.post('/connect')
def access_project(
    user: Dict = Depends(get_current_user), request: ConnectProjectRequest = None
):
    if (
        not request
        or not request.tool
        or not request.siteId
        or not request.projectKey
        or not request.projectName
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Tool, siteId, projectKey and projectName are required.',
        )

    uid = user.get('uid', '')

    try:
        project_id = db.find_project_id_by_details(
            tool_name=request.tool,
            site_domain=request.siteDomain,
            site_id=request.siteId,
            project_key=request.projectKey,
        )

        if not project_id:
            db.create_project(
                tool_name=request.tool,
                site_id=request.siteId,
                site_domain=request.siteDomain,
                project_key=request.projectKey,
                project_name=request.projectName,
            )

            project_id = db.find_project_id_by_details(
                tool_name=request.tool,
                site_domain=request.siteDomain,
                site_id=request.siteId,
                project_key=request.projectKey,
            )

        db.update_project_users(project_id=project_id, uid=uid)

        return f'Connected successfully.'

    except Exception as e:
        logger.exception('Failed to connect to project.')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to connect: {str(e)}',
        )


@router.post('/{project_id}/v/{version}/docs/upload')
def upload_docs(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
    files: List[UploadFile] = File([]),
):
    if not project_id or not version or not files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID, version and files are required.',
        )

    if len(files) > 5:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Only upto 5 files are allowed.',
        )

    try:
        uploaded_files = []

        for idx, file in enumerate(files):
            file_path = upload_file_to_gcs(
                object=file.file,
                content_type=file.content_type,
                upload_path=f'projects/{project_id}/v_{version}/uploads/{idx}_{file.filename}',
            )

            uploaded_files.append(
                {'url': file_path, 'name': file.filename, 'type': file.content_type}
            )

        if not uploaded_files:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='No files uploaded.',
            )

        db.update_version(
            project_id,
            version,
            {'files': uploaded_files, 'files_uploaded_by': user.get('uid', '')},
        )

        message_data = {
            'project_id': project_id,
            'version': version,
            'files': [f.get('url') for f in uploaded_files],
        }

        execution = Execution(argument=json.dumps(message_data))

        request = CreateExecutionRequest(
            parent=REQUIREMENTS_WORFLOW, execution=execution
        )

        response = workflow_client.create_execution(request=request)

        print(f'Workflow execution started successfully. Execution ID: {response.name}')

        return f'Files uploaded successfully.'

    except Exception as e:
        logger.exception('Failed to upload files.')

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to upload files: {str(e)}',
        )


@router.delete('/{project_id}/v/{version}/r/{req_id}/delete')
def delete_req(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
    req_id: str = None,
):
    if not project_id or not version or not req_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID, version and req_id are required.',
        )

    try:
        db.update_requirement(
            project_id,
            version,
            req_id,
            {'deleted': True, 'deleted_by': user.get('uid', '')},
        )
        return f'Requirement {req_id} marked as deleted successfully.'

    except Exception as e:
        logger.exception('Failed to delete requirement.')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to mark requirement as deleted: {str(e)}',
        )


@router.delete('/{project_id}/v/{version}/t/{tc_id}/delete')
def delete_tc(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
    tc_id: str = None,
):
    if not project_id or not version or not tc_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID, version and tc_id are required.',
        )

    try:
        db.update_testcase(
            project_id,
            version,
            tc_id,
            {
                'deleted': True,
                'deleted_by': user.get('uid', ''),
            },
        )
        return f'Test case {tc_id} marked as deleted successfully.'

    except Exception as e:
        logger.exception('Failed to delete test case.')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to mark test case as deleted: {str(e)}',
        )


@router.post('/{project_id}/v/{version}/requirements/confirm')
def confirm_requirements(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
):
    if not project_id or not version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID, version and req_ids are required.',
        )

    try:
        db.update_version(
            project_id,
            version,
            {'requirements_confirmed_by': user.get('uid', '')},
        )

        request = auth_requests.Request()
        id_token = oauth2_id_token.fetch_id_token(request, TESTCASE_CREATION_URL)

        response = requests.post(
            TESTCASE_CREATION_URL,
            headers={'Authorization': f'Bearer {id_token}'},
            json={
                'project_id': project_id,
                'version': version,
            },
            timeout=600,
        )

        response.raise_for_status()

        logging.info(f'{TESTCASE_CREATION_URL} responded with {response.status_code}')

        return 'Requirements confirmed successfully.'
    except Exception as e:
        logger.exception('Failed to confirm requirements.')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to confirm requirements: {str(e)}',
        )


@router.post('/{project_id}/v/{version}/testcases/confirm')
def create_testcases_on_jira(
    background_tasks: BackgroundTasks,
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
):
    if not project_id or not version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID and version are required.',
        )

    uid = user.get('uid', None)

    background_tasks.add_task(create_on_jira, uid, project_id, version)

    return 'OK'


@router.post('/{project_id}/v/{version}/datasets/create')
def create_datasets_for_testcases(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
):
    if not project_id or not version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID and version are required.',
        )

    uid = user.get('uid', None)

    try:
        request = auth_requests.Request()
        id_token = oauth2_id_token.fetch_id_token(request, DATASET_TASKS_DISPATHER_URL)

        logging.info(f'Making request to {DATASET_TASKS_DISPATHER_URL}')

        response = requests.post(
            DATASET_TASKS_DISPATHER_URL,
            headers={'Authorization': f'Bearer {id_token}'},
            json={
                'project_id': project_id,
                'version': version,
            },
            timeout=900,
        )

        response.raise_for_status()

        logging.info(
            f'{DATASET_TASKS_DISPATHER_URL} responded with {response.status_code}'
        )

    except Exception as e:
        logger.exception('Error when making API call to create datasets')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to create datasets: {str(e)}',
        )

    return 'OK'


@router.post('/v1/download/async', status_code=status.HTTP_202_ACCEPTED)
async def submit_download_job(
    background_tasks: BackgroundTasks,
    user: Dict = Depends(get_current_user),
    project_id: str = Body(..., embed=True),
    version: str = Body(..., embed=True),
    testcase_id: str = Body(..., embed=True),
):
    '''
    Starts an asynchronous job to download and zip datasets.
    '''
    try:
        uid = user.get('uid', None)
        job_id = db.create_download_job(uid, project_id, version, testcase_id)

        if not job_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail='Failed to create download job.',
            )

        background_tasks.add_task(
            background_zip_task, job_id, project_id, version, testcase_id
        )

        return {'message': 'Download job started successfully', 'job_id': job_id}
    
    except Exception as e:
        logger.exception(f'Error in submit_download_job: {e}')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail='Failed to start download job.',
        )


@router.get('/v1/download/status/{job_id}')
async def get_download_status(job_id: str):
    '''
    Checks the status of a download job. Returns the zip file if the job is completed.
    '''
    job_data = db.get_download_job(job_id)
    if not job_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail='Job not found.'
        )

    status_str = job_data.get('status')

    if status_str == 'completed':
        zip_url = job_data.get('result_url')
        file_name = job_data.get('file_name')

        if not zip_url:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail='Job completed but no download URL found.',
            )

        try:
            zip_content = get_file_from_gcs(zip_url)

            headers = {
                'Content-Disposition': f'attachment; filename={file_name}',
                'Content-Type': 'application/zip',
            }

            return StreamingResponse(io.BytesIO(zip_content), headers=headers)
        
        except Exception as e:
            logger.error(f'Failed to retrieve zip file for job {job_id}: {e}')

            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail='Failed to retrieve zipped file.',
            )

    elif status_str == 'failed':
        error = job_data.get('error', 'An unknown error occurred.')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Job failed: {error}',
        )

    return {'status': status_str}
