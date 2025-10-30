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

from tools.jira.client import JiraClient

REQUIREMENTS_CHANGE_ANALYSIS_IMPLICIT_ENDPOINT = os.getenv(
    'REQUIREMENTS_CHANGE_ANALYSIS_IMPLICIT_ENDPOINT'
)
REQUIREMENTS_IMPLICIT_ENDPOINT = os.getenv('REQUIREMENTS_IMPLICIT_ENDPOINT')

db = FirestoreDB()
sm = SecretManager()
jira_client = JiraClient()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def get_jira_requirement_payload(requirement, project_key):
    '''
    Maps internal requirement structure to a Jira issue payload.
    '''

    req_id = requirement.get('requirement_id')
    req_text = requirement.get('requirement', '')

    return {
        'fields': {
            'project': {'key': project_key},
            'summary': (
                f'[{req_id}] {req_text[:200]}...'
                if len(req_text) > 200
                else f'[{req_id}] {req_text}...'
            ),
            'description': {
                'type': 'doc',
                'version': 1,
                'content': [
                    {
                        'type': 'paragraph',
                        'content': [{'text': req_text, 'type': 'text'}],
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


def get_jira_testcase_payload(testcase, req_issue_key, project_key):
    '''
    Maps internal testcase structure to a Jira issue payload.
    '''
    title = testcase.get('title')
    description_text = testcase.get('description', '')
    acceptance = testcase.get('acceptance_criteria')
    if acceptance:
        description_text += f'\n\n*Acceptance Criteria:*\n{acceptance}'

    return {
        'fields': {
            'project': {'key': project_key},
            'summary': title[:200] + '...' if len(title) > 200 else title,
            'description': {
                'type': 'doc',
                'version': 1,
                'content': [
                    {
                        'type': 'paragraph',
                        'content': [{'text': description_text or '', 'type': 'text'}],
                    }
                ],
            },
            'issuetype': {'name': 'Subtask'},
            'priority': {'name': testcase.get('priority', 'Medium')},
            'parent': {'key': req_issue_key},
            'labels': [
                'AI_Generated',
                'Created_by_Captain',
                'Testcase',
                testcase.get('testcase_id'),
            ],
        }
    }


def sync_entities_on_alm(
    uid, project_id, version, project_details, entity_name, entity_ids
):
    '''
    Syncs Firestore testcases with Jira issues incrementally or fully.
    '''
    try:
        cloud_id = project_details['toolSiteId']
        cloud_domain = project_details['toolSiteDomain']

        if len(entity_ids) == 0:
            return

        if len(entity_ids) == 1:
            jira_issues = jira_client.search_issues(
                uid, cloud_domain, cloud_id, f'labels = \'{entity_ids[0]}\''
            )
        else:
            jira_issues = jira_client.search_issues(
                uid, cloud_domain, cloud_id, 'labels = \'Created_by_Captain\''
            )

        results = {}

        if entity_name == 'testcases':
            for tc_id in entity_ids:
                match = next(
                    (
                        issue
                        for issue in jira_issues
                        if tc_id in issue.get('labels', [])
                    ),
                    None,
                )

                if match:
                    results[tc_id] = match['key']

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

            return results

        if entity_name == 'requirements':
            for req_id in entity_ids:
                match = next(
                    (
                        issue
                        for issue in jira_issues
                        if req_id in issue.get('labels', [])
                    ),
                    None,
                )

                if match:
                    results[req_id] = match['key']

                    db.update_requirement(
                        project_id,
                        version,
                        req_id,
                        {
                            'toolIssueKey': match['key'],
                            'toolIssueLink': match['url'],
                            'toolCreated': 'SUCCESS',
                        },
                    )
                else:
                    db.update_requirement(
                        project_id, version, req_id, {'toolCreated': 'FAILED'}
                    )

            return results

    except Exception as e:
        logger.exception(f'Error syncing {entity_name} to Jira: {e}')


def get_req_issue_key(project_id, version, req_keys, testcase):
    req_id = testcase.get('requirement_id')

    if req_id in req_keys:
        return req_keys[req_id]

    req_details = db.get_requirement_details(
        project_id=project_id, version_id=version, requirement_id=testcase.get('requirement_id')
    )

    if not req_details:
        return None

    return req_details.get('toolIssueKey', None)


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

        if (
            not project_details
            or not project_details.get('toolSiteId')
            or not project_details.get('toolSiteDomain')
            or not project_details.get('toolProjectKey')
        ):
            raise Exception(
                f'''Required project details not found:
                toolSiteId: {project_details.get("toolSiteId", None)},
                toolSiteDomain: {project_details.get("toolSiteDomain", None)},
                toolProjectKey: {project_details.get("toolProjectKey", None)}'''
            )

        cloud_id = project_details['toolSiteId']
        alm_project_key = project_details['toolProjectKey']

        db.update_version(
            project_id,
            version,
            {'status': 'CREATE_ALM_NEW_REQUIREMENTS'},
        )

        requirements = db.get_requirements(project_id, version)

        new_requirements = [
            req
            for req in requirements
            if req.get('change_analysis_status') == 'NEW'
            and req.get('toolCreated', '') != 'SUCCESS'
        ]

        batch_size = 40
        req_keys = {}

        for i in range(0, len(new_requirements), batch_size):
            batch = new_requirements[i : i + batch_size]

            issue_payloads = [
                get_jira_requirement_payload(req, alm_project_key) for req in batch
            ]

            try:
                jira_client.create_bulk_issues(uid, cloud_id, issue_payloads)

                req_keys = sync_entities_on_alm(
                    uid=uid,
                    project_id=project_id,
                    version=version,
                    entity_name='requirements',
                    project_details=project_details,
                    entity_ids=[req['requirement_id'] for req in batch],
                )

            except Exception as e:
                logger.exception(f'Error creating batch of requirements: {e}')

        db.update_version(
            project_id,
            version,
            {'status': 'CREATE_ALM_NEW_TESTCASES'},
        )

        testcases = db.get_testcases(project_id, version)

        new_testcases = [
            tc
            for tc in testcases
            if tc.get('change_analysis_status') == 'NEW'
            and tc.get('toolCreated') != 'SUCCESS'
        ]

        for i in range(0, len(new_testcases), batch_size):
            batch = new_testcases[i : i + batch_size]

            issue_payloads = [
                get_jira_testcase_payload(
                    tc,
                    get_req_issue_key(project_id, version, req_keys, tc),
                    alm_project_key,
                )
                for tc in batch
            ]

            try:
                jira_client.create_bulk_issues(uid, cloud_id, issue_payloads)

                sync_entities_on_alm(
                    uid=uid,
                    project_id=project_id,
                    version=version,
                    entity_name='testcases',
                    project_details=project_details,
                    entity_ids=[tc['testcase_id'] for tc in batch],
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
                    cloud_id,
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


def create_one_testcase_on_alm(uid, project_id, version, tc_id):
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
        cloud_project_key = project_details.get('toolProjectKey')

        testcase = db.get_testcase_details(project_id, version, tc_id)
        if not testcase:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f'Test case {tc_id} not found.',
            )

        tc_id = testcase.get('testcase_id')

        jira_client.create_issue(
            uid,
            cloud_id,
            get_jira_testcase_payload(
                testcase,
                get_req_issue_key(project_id, version, {}, testcase),
                cloud_project_key,
            ),
        )

        sync_entities_on_alm(
            uid=uid,
            project_id=project_id,
            version=version,
            project_details=project_details,
            entity_name='testcases',
            entity_ids=[tc_id],
        )

    except HTTPException as e:
        logger.exception(f'Error creating test case: {e}')
        raise e

    except Exception as e:
        logger.exception(f'Error creating test case: {e}')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to create test case: {str(e)}',
        )


def create_one_requirement_on_alm(uid, project_id, version, req_id):
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
        cloud_project_key = project_details.get('toolProjectKey')

        requirement = db.get_requirement_details(project_id, version, req_id)

        if not requirement:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f'Requirement {req_id} not found.',
            )

        req_id = requirement.get('requirement_id')

        jira_client.create_issue(
            uid,
            cloud_id,
            get_jira_requirement_payload(
                requirement,
                cloud_project_key,
            ),
        )

        sync_entities_on_alm(
            uid=uid,
            project_id=project_id,
            version=version,
            project_details=project_details,
            entity_name='requirements',
            entity_ids=[req_id],
        )

    except HTTPException as e:
        logger.exception(f'Error creating requirement: {e}')
        raise e

    except Exception as e:
        logger.exception(f'Error creating requirement: {e}')
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to create requirement: {str(e)}',
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
