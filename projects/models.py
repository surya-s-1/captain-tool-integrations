from pydantic import BaseModel

class ConnectProjectRequest(BaseModel):
    tool: str
    projectKey: str
    projectName: str