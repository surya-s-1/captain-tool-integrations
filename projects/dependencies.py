from fastapi import HTTPException, status

from gcp.firestore import FirestoreDB

db = FirestoreDB()


async def check_if_latest_project_version(
    project_id: str,
    version: str,
):
    try:
        project_details = db.get_project_details(project_id)

        if not project_details:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Project with ID '{project_id}' not found.",
            )

        latest_version = project_details.get('latest_version')

        if version != latest_version:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail='Actions allowed only to the latest version.',
            )

        return True

    except HTTPException as e:
        raise e

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
