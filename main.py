from dotenv import load_dotenv
load_dotenv()

import os
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from tools.jira.router import router as JiraRouter

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
ALLOW_DOMAINS = os.getenv('ALLOW_DOMAINS', '')

ORIGINS = [domain.strip() for domain in ALLOW_DOMAINS.split(',') if domain.strip()]

app = FastAPI(
    title='Captain Tools API',
    description='A service for creating test cases by integrating with various tools like Jira.',
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router=JiraRouter, prefix='/tools/jira')

@app.get('/health')
def health_check():
    return 'OK'

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8001)
