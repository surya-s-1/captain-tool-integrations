import logging

from gcp.firestore import FirestoreDB
from gcp.secret_manager import SecretManager
from tools.jira.client import JiraClient

db = FirestoreDB()
sm = SecretManager()
jira_client = JiraClient()

logger = logging.getLogger(__name__)

def create_on_jira(uid, project_id, version):
    try:
        db.update_version(
            project_id=project_id,
            version=version,
            update_details={'status': 'START_JIRA_CREATION', 'testcases_confirmed_by': uid}
        )

        testcases = db.get_testcases(project_id, version)
        testcases = [tc for tc in testcases if not tc.get('deleted')]
        testcases = [tc for tc in testcases if tc.get('created') != 'SUCCESS']

        if not testcases:
            logger.info('No test cases found to sync.')
            return 'No test cases found to sync.'

        project_details = db.get_project_details(project_id)

        if (
            not project_details
            or not project_details.get('toolSiteId')
            or not project_details.get('toolProjectKey')
        ):
            db.update_version(
                project_id=project_id,
                version=version,
                update_details={'status': 'ERR_JIRA_CREATION'},
            )
            return

        cloud_id = project_details.get('toolSiteId')
        cloud_domain = project_details.get('toolSiteDomain')
        project_key = project_details.get('toolProjectKey')

        # # 1. Create in batches of 30
        batch_size = 40

        for i in range(0, len(testcases), batch_size):
            batch = testcases[i : i + batch_size]

            try:
                jira_client.create_bulk_issues(
                    uid, cloud_id, project_key, batch
                )

            except Exception as e:
                logger.exception(f'Error creating batch of test cases: {e}')

        db.update_version(
            project_id=project_id,
            version=version,
            update_details={'status': 'COMPLETE_JIRA_CREATION'},
        )

        db.update_version(
            project_id=project_id,
            version=version,
            update_details={'status': 'START_JIRA_SYNC'},
        )

        # 2. Get all issues from Jira with the label Created_by_Captain
        try:
            jira_issues = jira_client.search_issues_by_label(
                uid, cloud_domain, cloud_id, 'Created_by_Captain'
            )

        except Exception as e:
            logger.exception(f'Error getting issues from Jira: {e}')
            db.update_version(
                project_id=project_id,
                version=version,
                update_details={'status': 'ERR_JIRA_SYNC'},
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

                print('testcase_id in labels', testcase_id in labels, testcase_id, labels)

                if testcase_id in labels:
                    jira_link = issue.get('url', '')

                    db.update_testcase(
                        project_id,
                        version,
                        testcase_id,
                        {'toolIssueLink': jira_link, 'created': 'SUCCESS'},
                    )

                    found_match = True
                    break

            if not found_match:
                db.update_testcase(
                    project_id, version, testcase_id, {'created': 'FAILED'}
                )

        db.update_version(project_id, version, {'status': 'COMPLETE_JIRA_SYNC'})

    except Exception as e:
        logger.exception(f'Error syncing test cases to Jira: {e}')