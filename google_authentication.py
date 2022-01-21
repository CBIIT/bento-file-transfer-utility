from oauth2client import file, client, tools
from oauth2client.service_account import ServiceAccountCredentials
import json

SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly"
]


def authenticate_service_account():
    return ServiceAccountCredentials.from_json_keyfile_name('auth/client_secrets.json', SCOPES)


def authenticate_user_account():
    store = file.Storage('auth/storage.json').get()
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('auth/client_secrets.json', SCOPES)
        creds = tools.run_flow(flow, store)
    return creds


def authenticate_api_key():
    key = json.load(open('auth/api_key.json'))['key']
    return key
