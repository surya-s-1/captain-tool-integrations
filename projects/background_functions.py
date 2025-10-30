import io
import os
import time
import zipfile
import logging
import requests
from fastapi import HTTPException, status
import google.auth.transport.requests as auth_requests
import google.oauth2.id_token as oauth2_id_token

from gcp.firestore import FirestoreDB
from gcp.secret_manager import SecretManager
from gcp.storage import upload_file_to_gcs, get_file_from_gcs

from projects.utilities import map_testcase_to_jira_payload
from tools.jira.client import JiraClient

REQUIREMENTS_CHANGE_ANALYSIS_IMPLICIT_ENDPOINT = os.getenv(
    'REQUIREMENTS_CHANGE_ANALYSIS_IMPLICIT_ENDPOINT'
)
REQUIREMENTS_IMPLICIT_ENDPOINT = os.getenv('REQUIREMENTS_IMPLICIT_ENDPOINT')

db = FirestoreDB()
sm = SecretManager()
jira_client = JiraClient()

logger = logging.getLogger(__name__)


def background_issue_creation_on_alm(uid, project_id, version):
    '''
    Incremental batch creation & syncing with Jira.
    '''
    try:
        db.update_version(
            project_id,
            version,
            {
                'status': 'START_ALM_ISSUE_CREATION',
                'testcases_confirmed_by': uid,
            },
        )

        project_details = db.get_project_details(project_id)

        if not project_details or not all(
            [
                project_details.get(k, None)
                for k in ['toolSiteId', 'toolSiteDomain', 'toolProjectKey']
            ]
        ):
            raise Exception(
                f'Required project details not found:: toolSiteId: {project_details.get("toolSiteId", None)}, toolSiteDomain: {project_details.get("toolSiteDomain", None)}, toolProjectKey: {project_details.get("toolProjectKey", None)}'
            )

        alm_site_id = project_details['toolSiteId']
        alm_project_key = project_details['toolProjectKey']

        testcases = db.get_testcases(project_id, version)

        new_testcases = [
            tc
            for tc in testcases
            if not tc.get('deleted', False)
            and tc.get('change_analysis_status') == 'NEW'
            and tc.get('toolCreated') != 'SUCCESS'
        ]

        db.update_version(
            project_id,
            version,
            {'status': 'CREATE_ALM_NEW_ISSUES'},
        )

        batch_size = 40

        for i in range(0, len(new_testcases), batch_size):
            batch = new_testcases[i : i + batch_size]

            issue_payloads = [
                map_testcase_to_jira_payload(tc, alm_project_key) for tc in batch
            ]

            try:
                jira_client.create_bulk_issues(uid, alm_site_id, issue_payloads)

                sync_testcases_on_alm(
                    uid=uid,
                    project_id=project_id,
                    version=version,
                    project_details=project_details,
                    testcase_ids=[tc['testcase_id'] for tc in batch],
                )

            except Exception as e:
                logger.exception(f'Error creating batch of test cases: {e}')

        dep_testcases = [
            tc
            for tc in testcases
            if not tc.get('deleted', False)
            and tc.get('change_analysis_status') == 'DEPRECATED'
        ]

        db.update_version(project_id, version, {'status': 'UPDATE_ALM_DEP_ISSUES'})

        for tc in dep_testcases:
            try:
                jira_client.update_issue(
                    uid,
                    alm_site_id,
                    tc.get('toolIssueKey'),
                    {'summary': tc.get('title')},
                )

                time.sleep(0.5)
            except Exception as e:
                logger.exception(f'Error updating deprecated testcase: {e}')

        db.update_version(
            project_id, version, {'status': 'COMPLETE_ALM_ISSUE_CREATION'}
        )

    except Exception as e:
        logger.exception(f'Error syncing test cases to Jira: {e}')

        db.update_version(project_id, version, {'status': 'ERR_ALM_ISSUE_CREATION'})


def sync_testcases_on_alm(uid, project_id, version, project_details, testcase_ids):
    '''
    Syncs Firestore testcases with Jira issues incrementally or fully.
    '''
    try:
        tool_site_id = project_details['toolSiteId']
        domain = project_details['toolSiteDomain']

        jira_issues = jira_client.search_issues(
            uid, domain, tool_site_id, 'labels = \'Created_by_Captain\''
        )

        for tc_id in testcase_ids:
            match = next(
                (issue for issue in jira_issues if tc_id in issue.get('labels', [])),
                None,
            )

            if match:
                db.update_testcase(
                    project_id,
                    version,
                    tc_id,
                    {
                        'toolIssueKey': match['key'],
                        'toolIssueLink': match['url'],
                        'toolCreated': 'SUCCESS',
                    },
                )
            else:
                db.update_testcase(
                    project_id, version, tc_id, {'toolCreated': 'FAILED'}
                )

    except Exception as e:
        logger.exception(f'Error syncing test cases to Jira: {e}')


def background_creation_specific_testcase_on_tool(uid, project_id, version, tc_id):
    try:
        project_details = db.get_project_details(project_id)

        if (
            not project_details
            or not project_details.get('toolSiteId')
            or not project_details.get('toolSiteDomain')
            or not project_details.get('toolProjectKey')
        ):
            return

        cloud_id = project_details.get('toolSiteId')
        cloud_domain = project_details.get('toolSiteDomain')
        cloud_project_key = project_details.get('toolProjectKey')

        testcase = db.get_testcase_details(project_id, version, tc_id)
        tc_id = testcase.get('testcase_id')

        jira_client.create_issue(
            uid, cloud_id, map_testcase_to_jira_payload(testcase, cloud_project_key)
        )

        jira_issues = jira_client.search_issues(
            uid, cloud_domain, cloud_id, f'labels = \'{tc_id}\''
        )

        match = next(
            (issue for issue in jira_issues if tc_id in issue.get('labels', [])),
            None,
        )

        if match:
            db.update_testcase(
                project_id,
                version,
                tc_id,
                {
                    'toolIssueKey': match['key'],
                    'toolIssueLink': match['url'],
                    'toolCreated': 'SUCCESS',
                },
            )
        else:
            db.update_testcase(project_id, version, tc_id, {'toolCreated': 'FAILED'})

    except Exception as e:
        logger.exception(f'Error creating test case: {e}')

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to create test case: {str(e)}',
        )


def background_document_zip_task(
    job_id: str, project_id: str, version: str, doc_name: str
):
    '''
    Background task to download GCS files and zip them.
    The result is stored as a blob in the job document in Firestore.
    '''
    try:
        db.update_download_job_status(job_id, 'in_progress')

        version_data = db.get_version_details(project_id, version)

        if not version_data or not version_data.get('files'):
            db.update_download_job_status(job_id, 'failed', error='No documents found')
            return

        files = version_data.get('files', [])
        docs = [item.get('url', '') for item in files if item.get('name') == doc_name]

        if not docs:
            db.update_download_job_status(
                job_id, 'failed', error='Specified document found'
            )
            return

        zip_buffer = io.BytesIO()

        zip_name = f'{doc_name}-{version}-{project_id}'

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for url in docs:
                if not url.startswith('gs://'):
                    continue

                try:
                    file_content = get_file_from_gcs(url)
                    content_type = url.split('.')[-1]

                    zip_file.writestr(f'{zip_name}.{content_type}', file_content)
                except Exception as e:
                    # Log the error and continue to the next file
                    logger.warning(f'Failed to download {url}: {e}')

        zip_buffer.seek(0)

        upload_path = f'jobs/{job_id}/archive.zip'

        zip_url = upload_file_to_gcs(zip_buffer, 'application/zip', upload_path)

        db.update_download_job_status(
            job_id, 'completed', file_name=f'{zip_name}.zip', result_url=zip_url
        )

    except Exception as e:
        logger.error(f'Error in background zip task for job {job_id}: {e}')

        db.update_download_job_status(job_id, 'failed', error=str(e))


def background_testcase_zip_task(
    job_id: str, project_id: str, version: str, testcase_id: str
):
    '''
    Background task to download GCS files and zip them.
    The result is stored as a blob in the job document in Firestore.
    '''
    try:
        db.update_download_job_status(job_id, 'in_progress')

        testcase_data = db.get_testcase_details(project_id, version, testcase_id)

        if not testcase_data or not testcase_data.get('datasets'):
            db.update_download_job_status(job_id, 'failed', error='No datasets found')
            return

        datasets = testcase_data.get('datasets')
        zip_buffer = io.BytesIO()

        zip_name = f'{testcase_id}-{version}-{project_id}'

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for url in datasets:
                if not url.startswith('gs://'):
                    continue

                try:
                    file_content = get_file_from_gcs(url)
                    content_type = url.split('.')[-1]

                    zip_file.writestr(f'{zip_name}.{content_type}', file_content)
                except Exception as e:
                    # Log the error and continue to the next file
                    logger.warning(f'Failed to download {url}: {e}')

        zip_buffer.seek(0)

        upload_path = f'jobs/{job_id}/archive.zip'

        zip_url = upload_file_to_gcs(zip_buffer, 'application/zip', upload_path)

        db.update_download_job_status(
            job_id, 'completed', file_name=f'{zip_name}.zip', result_url=zip_url
        )

    except Exception as e:
        logger.error(f'Error in background zip task for job {job_id}: {e}')
        db.update_download_job_status(job_id, 'failed', error=str(e))


def background_zip_all_task(job_id: str, project_id: str, version: str):
    try:
        db.update_download_job_status(job_id, 'in_progress')

        testcases = db.get_testcases(project_id, version)

        datasets = [
            {'testcase_id': tc.get('testcase_id'), 'urls': tc.get('datasets')}
            for tc in testcases
            if tc.get('datasets')
        ]

        zip_buffer = io.BytesIO()

        zip_name = f'{version}-{project_id}'

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for data in datasets:
                for url in data.get('urls'):
                    if not url.startswith('gs://'):
                        continue

                    try:
                        tc_id = data.get('testcase_id')
                        file_content = get_file_from_gcs(url)
                        content_type = url.split('.')[-1]

                        zip_file.writestr(
                            f'{tc_id}-{zip_name}.{content_type}', file_content
                        )
                    except Exception as e:
                        # Log the error and continue to the next file
                        logger.warning(f'Failed to download {url}: {e}')

        zip_buffer.seek(0)

        upload_path = f'jobs/{job_id}/archive.zip'

        zip_url = upload_file_to_gcs(zip_buffer, 'application/zip', upload_path)

        db.update_download_job_status(
            job_id, 'completed', file_name=f'{zip_name}.zip', result_url=zip_url
        )
    except Exception as e:
        logger.exception(f'Error in background zip task for job {job_id}: {e}')
        db.update_download_job_status(job_id, 'failed', error=str(e))


def background_invoke_change_analysis_implicit_processing(project_id, version):
    try:
        request = auth_requests.Request()
        id_token = oauth2_id_token.fetch_id_token(
            request, REQUIREMENTS_CHANGE_ANALYSIS_IMPLICIT_ENDPOINT
        )

        response = requests.post(
            REQUIREMENTS_CHANGE_ANALYSIS_IMPLICIT_ENDPOINT,
            headers={'Authorization': f'Bearer {id_token}'},
            json={
                'project_id': project_id,
                'version': version,
            },
            timeout=600,
        )

        response.raise_for_status()

        logging.info(
            f'{REQUIREMENTS_CHANGE_ANALYSIS_IMPLICIT_ENDPOINT} responded with {response.status_code}'
        )

    except Exception as e:
        logger.exception(
            f'Error when invoking implicit req processor for change analysis: {e}'
        )

        db.update_version(
            project_id=project_id,
            version=version,
            update_details={'status': 'ERR_CHANGE_ANALYSIS_IMPLICIT'},
        )


def background_invoke_implicit_processing(project_id, version):
    try:
        request = auth_requests.Request()
        id_token = oauth2_id_token.fetch_id_token(
            request, REQUIREMENTS_IMPLICIT_ENDPOINT
        )

        response = requests.post(
            REQUIREMENTS_IMPLICIT_ENDPOINT,
            headers={'Authorization': f'Bearer {id_token}'},
            json={
                'project_id': project_id,
                'version': version,
            },
            timeout=2400,
        )

        response.raise_for_status()

        logging.info(
            f'{REQUIREMENTS_IMPLICIT_ENDPOINT} responded with {response.status_code}'
        )

    except Exception as e:
        logger.exception(f'Error when invoking implicit req processor: {e}')

        db.update_version(
            project_id=project_id,
            version=version,
            update_details={'status': 'ERR_IMP_REQ_EXTRACT'},
        )
