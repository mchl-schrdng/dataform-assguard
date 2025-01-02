import json
import logging
from typing import Tuple
from google.auth.transport.requests import Request
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

def authenticate(service_account_json: str) -> Tuple[str, service_account.Credentials]:
    try:
        logger.info("Starting authentication process.")
        info = json.loads(service_account_json)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        creds.refresh(Request())
        token = creds.token
        logger.info("Authentication successful.")
        return token, creds
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        raise