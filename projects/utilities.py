def map_testcase_to_jira_payload(testcase, project_key):
    '''
    Maps internal testcase structure to a Jira issue payload.
    '''
    description_text = testcase.get('description', '')
    acceptance = testcase.get('acceptance_criteria')
    if acceptance:
        description_text += f'\n\n*Acceptance Criteria:*\n{acceptance}'

    return {
        'fields': {
            'project': {'key': project_key},
            'summary': testcase.get('title'),
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
            'issuetype': {'name': 'Task'},
            'priority': {'name': testcase.get('priority', 'Medium')},
            'labels': [
                'AI_Generated',
                'Created_by_Captain',
                'Testcase',
                testcase.get('testcase_id')
            ],
        }
    }
