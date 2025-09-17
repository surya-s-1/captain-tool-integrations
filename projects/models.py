from pydantic import BaseModel

class ConnectProjectRequest(BaseModel):
    tool: str
    siteId: str
    siteDomain: str
    projectKey: str
    projectName: str

class UpdateTestCaseRequest(BaseModel):
    prompt: str