from oauth2client import file, client, tools
from oauth2client.service_account import ServiceAccountCredentials
import json

# Google API scopes
SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly"
]


def authenticate_service_account():
    """
    Create a service account credentials object from the 'auth/client_secrets.json'
    :return: The generated credentials object
    """
    return ServiceAccountCredentials.from_json_keyfile_name('auth/client_secrets.json', SCOPES)


def authenticate_user_account():
    """
    Create an OAuth2.0 credentials object from the 'auth/client_secrets.json'
    :return: The generated credentials object
    """
    # Checks to see if there is a saved authentication token
    store = file.Storage('auth/storage.json').get()
    creds = store.get()
    # Prompt user for login to generate an authentication token based off of the 'auth/client_secrets.json' object
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('auth/client_secrets.json', SCOPES)
        creds = tools.run_flow(flow, store)
    return creds


def authenticate_api_key():
    """
    Loads an API key from the 'auth/api_key.json' file
    :return: Returns an API key
    """
    key = json.load(open('auth/api_key.json'))['key']
    return key
