import os
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from firebase_admin import initialize_app, auth
from typing import Dict


http_bearer = HTTPBearer()

initialize_app(options={'projectId': os.getenv('GOOGLE_CLOUD_PROJECT')})


def get_current_user(cred: HTTPAuthorizationCredentials = Depends(http_bearer)) -> Dict:
    if not cred:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Bearer authentication required',
            headers={'WWW-Authenticate': 'Bearer'},
        )

    id_token = cred.credentials

    try:
        decoded_token = auth.verify_id_token(id_token)

    except auth.ExpiredIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail='ID token has expired'
        )

    except auth.InvalidIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail='Invalid ID token'
        )

    return decoded_token
