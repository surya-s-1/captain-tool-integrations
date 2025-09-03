from typing import Dict
from fastapi import APIRouter, HTTPException, Depends, status

from projects.models import ConnectProjectRequest

from gcp.firestore import FirestoreDB
db = FirestoreDB()

from auth import get_current_user

router = APIRouter(tags=['Project Actions'])

@router.post('/connect')
def connect_project(
    user: Dict = Depends(get_current_user),
    request: ConnectProjectRequest = None
):
    if not request.tool or not request.projectKey or not request.projectName:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Tool, projectKey and projectName are required.')
    
    tool_name = request.tool
    project_key = request.projectKey
    project_name = request.projectName

    try:
        db.create_project(tool_name=tool_name, project_key=project_key, project_name=project_name)
        
        return f'{tool_name}\'s {project_name} connected successfully.'
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f'Failed to connect project: {str(e)}'
        )
