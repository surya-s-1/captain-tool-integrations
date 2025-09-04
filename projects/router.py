import os
import requests
from typing import Dict, List
from fastapi import APIRouter, HTTPException, Depends, status, UploadFile, File, Form

import google.auth.transport.requests as auth_requests
import google.oauth2.id_token as oauth2_id_token

from projects.models import ConnectProjectRequest
from auth import get_current_user
from gcp.firestore import FirestoreDB
from gcp.storage import upload_file_to_gcs

TEXT_EXTRACTION_URL = os.getenv('TEXT_EXTRACTION_URL')
TEST_CREATION_URL = os.getenv('TEST_CREATION_URL')

db = FirestoreDB()
router = APIRouter(tags=['Project Actions'])


@router.post('/connect')
def connect_project(
    user: Dict = Depends(get_current_user), request: ConnectProjectRequest = None
):
    if not request.tool or not request.projectKey or not request.projectName:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Tool, projectKey and projectName are required.',
        )

    tool_name = request.tool
    project_key = request.projectKey
    project_name = request.projectName

    try:
        db.create_project(
            tool_name=tool_name, project_key=project_key, project_name=project_name
        )

        return f'{tool_name}\'s {project_name} connected successfully.'

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to connect project: {str(e)}',
        )


@router.post('/{project_id}/v/{version}/docs/upload')
def upload_docs(
    user: Dict = Depends(get_current_user),
    project_id: str = None,
    version: str = None,
    manual_verification: str = Form('true'),
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
                project_id=project_id,
                version=version,
                file_name=f'{idx}_{file.filename}',
            )

            uploaded_files.append(
                {'url': file_path, 'name': file.filename, 'type': file.content_type}
            )

        db.update_version(project_id, version, manual_verification == 'true', uploaded_files)

        # request = auth_requests.Request()
        # id_token = oauth2_id_token.fetch_id_token(request, TEXT_EXTRACTION_URL)

        message_data = {
            'project_id': project_id,
            'version': version,
            'files': [f.get('url') for f in uploaded_files],
        }

        response = requests.post(
            TEXT_EXTRACTION_URL,
            # headers={'Authorization': f'Bearer {id_token}'},
            json=message_data,
            timeout=30,
        )

        print(f'{TEXT_EXTRACTION_URL} responded with {response.status_code}')

        return f'Files uploaded successfully.'
    except Exception as e:
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
            detail='Project ID, version and req_id are required.'
        )

    try:
        db.mark_requirement_deleted(project_id, version, req_id)
        return f'Requirement {req_id} marked as deleted successfully.'
    except Exception as e:
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
        db.mark_testcase_deleted(project_id, version, tc_id)
        return f'Test case {tc_id} marked as deleted successfully.'
    except Exception as e:
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
        requests.post(
            TEST_CREATION_URL,
            json={
                'project_id': project_id,
                'version': version,
            },
            timeout=30,
        )

        return 'Requirements confirmed successfully.'
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to confirm requirements: {str(e)}',
        )


# @router.post('/{project_id}/v/{version}/testcases/confirm')
# def confirm_test_cases(
#     user: Dict = Depends(get_current_user),
#     project_id: str = None,
#     version: str = None,
# ):
#     if not project_id or not version:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail='Project ID, version and tc_ids are required.',
#         )

#     try:
#         requests.post(
#             TEST_CREATION_URL,
#             json={
#                 'project_id': project_id,
#                 'version': version,
#             },
#             timeout=30,
#         )

#         return 'Test cases confirmed successfully.'
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f'Failed to confirm test cases: {str(e)}',
#         )