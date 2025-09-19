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

from projects.models import ConnectProjectRequest, UpdateTestCaseRequest
from projects.background_functions import (
    background_creation_on_tool,
    background_sync_tool_testcases,
    background_zip_task,
    background_zip_all_task,
)

from gcp.firestore import FirestoreDB
from gcp.secret_manager import SecretManager
from gcp.storage import (
    upload_file_to_gcs,
    get_file_from_gcs,
)  # New import for downloading files

from google.cloud.workflows.executions_v1beta import (
    Execution,
    ExecutionsClient,
    CreateExecutionRequest,
)
import google.auth.transport.requests as auth_requests
import google.oauth2.id_token as oauth2_id_token

REQUIREMENTS_WORFLOW = os.getenv('REQUIREMENTS_WORFLOW')
TESTCASE_CREATION_URL = os.getenv('TESTCASE_CREATION_URL')
TESTCASE_ENHANCER_URL = os.getenv('TESTCASE_ENHANCER_URL')
DATASET_TASKS_DISPATHER_URL = os.getenv('DATASET_TASKS_DISPATHER_URL')

import logging

logger = logging.getLogger(__name__)

db = FirestoreDB()
sm = SecretManager()
jira_client = JiraClient()
router = APIRouter(tags=['Project Actions'])

workflow_client = ExecutionsClient()


@router.post(
    '/connect',
    description='Connects a user to a Jira project by either creating a new project entry or updating an existing one.',
)
def connect_project_to_application(
    user: Dict = Depends(get_current_user), request: ConnectProjectRequest = None
):
    '''
    Connects a user to a Jira project by either creating a new project entry or updating an existing one.
    '''
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
            tool_name=request.tool.lower(),
            site_domain=request.siteDomain,
            site_id=request.siteId,
            project_key=request.projectKey,
        )

        if not project_id:
            db.create_project(
                tool_name=request.tool.lower(),
                site_id=request.siteId,
                site_domain=request.siteDomain,
                project_key=request.projectKey,
                project_name=request.projectName,
            )

            project_id = db.find_project_id_by_details(
                tool_name=request.tool.lower(),
                site_domain=request.siteDomain,
                site_id=request.siteId,
                project_key=request.projectKey,
            )

        db.update_project_users(project_id=project_id, uid=uid)

        return f'Connected successfully.'

    except HTTPException as e:
        raise e

    except Exception as e:
        logger.exception('Failed to connect to project.')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to connect: {str(e)}',
        )


@router.get(
    '/connected',
    description='Gets the projects that a user is connected to and has access to.',
)
def get_connected_projects(user: Dict = Depends(get_current_user)):
    uid = user.get('uid', '')

    connected = []
    for project in db.get_connected_projects(uid):
        project.pop('uids', None)
        connected.append(project)

    return connected


@router.get(
    '/{project_id}/details',
    description='Gets the details of a project by project id including its lates version, tool (ex. Jira, Azure DevOps), siteId(Unique project id in the rool), siteDomain(Projects domain in the tool), etc.',
)
def get_project_details(user: Dict = Depends(get_current_user), project_id: str = None):
    if not project_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID is required.',
        )

    try:
        project_data = db.get_project_details(project_id)

        project_data.pop('uids', None)

        return project_data

    except Exception as e:
        logger.exception('Failed to get project details.')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to get project details: {str(e)}',
        )


@router.post(
    '/{project_id}/v/{version}/docs/upload',
    description='Uploads documentation files for a specific project version.',
)
def upload_documentation_for_project_latest_version(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
    files: List[UploadFile] = File([]),
):
    '''
    Uploads documentation files for a specific project version.
    '''
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

    except HTTPException as e:
        raise e

    except Exception as e:
        logger.exception('Failed to upload files.')

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to upload files: {str(e)}',
        )


@router.delete(
    '/{project_id}/v/{version}/r/{req_id}',
    description='Marks a specific requirement as deleted for a given project version.',
)
def mark_requirement_deleted(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
    req_id: str = None,
):
    '''
    Marks a specific requirement as deleted for a given project version.
    '''
    if not project_id or not version or not req_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID, version and req_id are required.',
        )

    try:
        version_details = db.get_version_details(project_id, version)

        if version_details.get('status', '') != 'CONFIRM_REQ_EXTRACT':
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail='You will be allowed to delete requirements only in confirm requirements state.',
            )

        db.update_requirement(
            project_id,
            version,
            req_id,
            {'deleted': True, 'deleted_by': user.get('uid', '')},
        )
        return f'Requirement {req_id} marked as deleted successfully.'

    except HTTPException as e:
        raise e

    except Exception as e:
        logger.exception('Failed to delete requirement.')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to mark requirement as deleted: {str(e)}',
        )


@router.delete(
    '/{project_id}/v/{version}/t/{tc_id}',
    description='Marks a specific test case as deleted for a given project version.',
)
def mark_testcase_deleted(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
    tc_id: str = None,
):
    '''
    Marks a specific test case as deleted for a given project version.
    '''
    if not project_id or not version or not tc_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID, version and tc_id are required.',
        )

    version_details = db.get_version_details(project_id, version)

    if version_details.get('status', '') != 'CONFIRM_TESTCASES':
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='You will be allowed to delete testcases only in confirm testcases state.',
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

    except HTTPException as e:
        raise e

    except Exception as e:
        logger.exception('Failed to delete test case.')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to mark test case as deleted: {str(e)}',
        )


@router.post(
    '/{project_id}/v/{version}/requirements/confirm',
    description='Confirms the requirements for a project version and triggers the test case creation workflow.',
)
def confirm_requirements_and_trigger_testcase_creation(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
):
    '''
    Confirms the requirements for a project version and triggers the test case creation workflow.
    '''
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


@router.post(
    '/{project_id}/v/{version}/t/{testcase_id}/update',
    description='Updates a specific test case.',
)
def update_testcase(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
    testcase_id: str = None,
    request: UpdateTestCaseRequest = None,
):
    '''
    Updates a specific test case.
    '''
    if not project_id or not version or not testcase_id or not request.prompt:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID, version, testcase_id and prompt are required.',
        )

    uid = user.get('uid', None)

    auth_request = auth_requests.Request()
    id_token = oauth2_id_token.fetch_id_token(auth_request, TESTCASE_ENHANCER_URL)

    try:
        response = requests.post(
            url=TESTCASE_ENHANCER_URL,
            headers={'Authorization': f'Bearer {id_token}'},
            json={
                'uid': uid,
                'project_id': project_id,
                'version': version,
                'testcase_id': testcase_id,
                'prompt': request.prompt,
            },
            timeout=120,
        )

        logger.info(f'{TESTCASE_ENHANCER_URL} responded with {response.status_code}')

        return response.json()
    except Exception as e:
        logger.exception('Failed to update testcase.')

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to update testcase: {str(e)}',
        )


@router.post(
    '/{project_id}/v/{version}/testcases/confirm',
    description='Confirms test cases and initiates their creation in Jira as a background task.',
)
def confirm_create_testcases_on_jira(
    background_tasks: BackgroundTasks,
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
):
    '''
    Confirms test cases and initiates their creation in Jira as a background task.
    '''
    if not project_id or not version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID and version are required.',
        )

    uid = user.get('uid', None)

    background_tasks.add_task(background_creation_on_tool, uid, project_id, version)

    return 'OK'


@router.post(
    '/{project_id}/v/{version}/testcases/sync',
    description='Syncs the testcases with the app.',
)
def confirm_create_testcases_on_jira(
    background_tasks: BackgroundTasks,
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
):
    '''
    Syncs the testcases with the app.
    '''
    if not project_id or not version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID and version are required.',
        )

    uid = user.get('uid', None)

    background_tasks.add_task(background_sync_tool_testcases, uid, project_id, version)

    return 'OK'


@router.post(
    '/{project_id}/v/{version}/datasets/create',
    description='Triggers the creation of datasets for test cases by calling an external workflow.',
)
def create_datasets_for_testcases(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
):
    '''
    Triggers the creation of datasets for test cases by calling an external workflow.
    '''
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


@router.post(
    '/download/one',
    status_code=status.HTTP_202_ACCEPTED,
    description='Starts an asynchronous job to download and zip a specific testcase\'s dataset.',
)
async def initiate_download_dataset_job_for_one_testcase(
    background_tasks: BackgroundTasks,
    user: Dict = Depends(get_current_user),
    project_id: str = Body(..., embed=True),
    version: str = Body(..., embed=True),
    testcase_id: str = Body(..., embed=True),
):
    '''
    Starts an asynchronous job to download and zip a specific testcase's dataset.
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

    except HTTPException as e:
        raise e

    except Exception as e:
        logger.exception(f'Error in submit_download_job: {e}')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail='Failed to start download job.',
        )


@router.post(
    '/download/all',
    status_code=status.HTTP_202_ACCEPTED,
    description='Starts an asynchronous job to download and zip datasets.',
)
async def initiate_download_dataset_job_for_all_testcases(
    background_tasks: BackgroundTasks,
    user: Dict = Depends(get_current_user),
    project_id: str = Body(..., embed=True),
    version: str = Body(..., embed=True),
):
    '''
    Starts an asynchronous job to download and zip datasets.
    '''
    try:
        uid = user.get('uid', None)
        job_id = db.create_download_all_job(uid, project_id, version)

        if not job_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail='Failed to create download job.',
            )

        background_tasks.add_task(background_zip_all_task, job_id, project_id, version)

        return {'message': 'Download job started successfully', 'job_id': job_id}

    except HTTPException as e:
        raise e

    except Exception as e:
        logger.exception(f'Error in submit_download_job: {e}')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail='Failed to start download job.',
        )


@router.get(
    '/download/status/{job_id}',
    description='Checks the status of a download job. Returns the zip file if the job is completed.',
)
async def get_download_job_status(
    user: Dict = Depends(get_current_user), job_id: str = None
):
    '''
    Checks the status of a download job. Returns the zip file if the job is completed.
    '''
    if not job_id:
        return HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail='Job ID is required.'
        )

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
                'Content-Disposition': f'attachment; filename="{file_name}"',
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


@router.get(
    '/{project_id}/v/{version}/requirements/list',
    description='Fetches requirements for a given project version, optionally filtered by source or regulation.',
)
async def get_requirements_filtered(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
    source_filename: str = None,
    regulation: str = None,
):
    '''
    Fetches requirements for a given project version, optionally filtered by source or regulation.
    '''
    if not project_id or not version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID and version are required.',
        )

    try:
        requirements = db.get_requirements(project_id, version)

        if source_filename:
            requirements = [
                req
                for req in requirements
                if any(
                    s.get('filename') == source_filename for s in req.get('sources', [])
                )
            ]

        if regulation:
            requirements = [
                req
                for req in requirements
                if any(
                    r.get('regulation') == regulation
                    for r in req.get('regulations', [])
                )
            ]

        return requirements

    except Exception as e:
        logger.exception(f'Failed to retrieve requirements: {e}')

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to retrieve requirements: {str(e)}',
        )


@router.get(
    '/{project_id}/v/{version}/testcases/list',
    description='Fetches testcases for a given project version, optionally filtered by source or regulation.',
)
async def get_testcases_filtered(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
    requirement_id: str = None,
):
    '''
    Fetches requirements for a given project version, optionally filtered by source or regulation.
    '''
    if not project_id or not version or not requirement_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Project ID and version are required.',
        )

    try:
        testcases = db.get_testcases(project_id, version)

        testcases = [
            tc for tc in testcases if tc.get('requirement_id') == requirement_id
        ]

        return testcases

    except Exception as e:
        logger.exception(f'Failed to retrieve requirements: {e}')

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to retrieve requirements: {str(e)}',
        )
