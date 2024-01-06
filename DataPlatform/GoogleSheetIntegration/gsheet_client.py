import os.path
import json
import logging
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Set up logging with timestamps
log_file_path = '/app/logs/googlesheets/consumer_logs.log'
logging.basicConfig(filename=log_file_path, level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) 

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def read_config():
    config_file_path = "/app/config_files/config.json"
    with open(config_file_path, "r") as config_file:
        return json.load(config_file)

def get_spreadsheet_client():
    config = read_config()
    creds = None
    
    if os.path.exists("/app/token.json"):
        creds = Credentials.from_authorized_user_file(config["googleSheet"]["tokenFile"], SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                config["googleSheet"]["credentials.json"], SCOPES
            )
            creds = flow.run_local_server(port=0)

        token_path = "/app/token.json"
        with open(token_path, "w") as token:
            token.write(creds.to_json())

    try:
        service = build("sheets", "v4", credentials=creds)
        return service.spreadsheets()
    except Exception as e:
        logger.error(f"Error initializing spreadsheet client: {e}")
        raise


