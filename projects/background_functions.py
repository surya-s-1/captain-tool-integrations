import os
import io
import zipfile
import logging

from gcp.firestore import FirestoreDB
from gcp.secret_manager import SecretManager
from gcp.storage import upload_file_to_gcs, get_file_from_gcs

from tools.jira.client import JiraClient

db = FirestoreDB()
sm = SecretManager()
jira_client = JiraClient()

logger = logging.getLogger(__name__)


def background_creation_on_tool(uid, project_id, version):
    try:
        db.update_version(
            project_id=project_id,
            version=version,
            update_details={
                'status': 'START_TC_CREATION_ON_TOOL',
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
            db.update_version(
                project_id=project_id,
                version=version,
                update_details={'status': 'ERR_TC_CREATION_ON_TOOL'},
            )
            return

        tool_site_id = project_details.get('toolSiteId')
        tool_site_domain = project_details.get('toolSiteDomain')
        tool_project_key = project_details.get('toolProjectKey')

        # # 1. Create in batches of 40
        batch_size = 40

        testcases = db.get_testcases(project_id, version)
        testcases = [tc for tc in testcases if not tc.get('deleted')]
        testcases = [tc for tc in testcases if tc.get('toolCreated') != 'SUCCESS']

        for i in range(0, len(testcases), batch_size):
            batch = testcases[i : i + batch_size]

            try:
                jira_client.create_bulk_testcases(
                    uid, tool_site_id, tool_project_key, batch
                )

            except Exception as e:
                logger.exception(f'Error creating batch of test cases: {e}')

        db.update_version(
            project_id=project_id,
            version=version,
            update_details={'status': 'COMPLETE_TC_CREATION_ON_TOOL'},
        )

        db.update_version(
            project_id=project_id,
            version=version,
            update_details={'status': 'START_TC_SYNC_WITH_TOOL'},
        )

        # 2. Get all issues from Jira with the label Created_by_Captain
        try:
            jira_issues = jira_client.search_issues_by_label(
                uid, tool_site_domain, tool_site_id, 'Created_by_Captain'
            )

        except Exception as e:
            logger.exception(f'Error getting issues from Jira: {e}')
            db.update_version(
                project_id=project_id,
                version=version,
                update_details={'status': 'ERR_TC_SYNC_WITH_TOOL'},
            )
            return

        for testcase in testcases:
            testcase_id = testcase.get('testcase_id')

            if not testcase_id:
                continue

            found_match = False

            # 3. Find matching issue and update as SUCCESS
            for issue in jira_issues:
                labels = issue.get('labels', [])

                if testcase_id in labels:
                    tool_key = issue.get('key', '')
                    tool_link = issue.get('url', '')

                    db.update_testcase(
                        project_id,
                        version,
                        testcase_id,
                        {'toolIssueKey': tool_key, 'toolIssueLink': tool_link, 'toolCreated': 'SUCCESS'},
                    )

                    found_match = True
                    break

            if not found_match:
                db.update_testcase(
                    project_id, version, testcase_id, {'toolCreated': 'FAILED'}
                )

        db.update_version(
            project_id, 
            version, 
            {'status': 'COMPLETE_TC_SYNC_WITH_TOOL'}
        )

    except Exception as e:
        logger.exception(f'Error syncing test cases to Jira: {e}')


def background_zip_task(job_id: str, project_id: str, version: str, testcase_id: str):
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
