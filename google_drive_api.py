from googleapiclient import discovery
from httplib2 import Http

GOOGLE_FILE_NAME = "name"
GOOGLE_FILE_ID = "id"
GOOGLE_FILE_MD5 = "md5Checksum"
GOOGLE_FILE_SIZE = "size"
GOOGLE_FILE_MIMETYPE = "mimeType"


class API:

    def __init__(self, credentials):
        self.connection = discovery.build('drive', 'v3', http=credentials.authorize(Http()))

    def get_folder_by_id(self, resource_id):
        return self.connection.files().get(fileId=resource_id).execute()

    def get_file_by_id(self, resource_id):
        return self.connection.files().get(
            fileId=resource_id,
            fields=','.join([GOOGLE_FILE_NAME, GOOGLE_FILE_ID, GOOGLE_FILE_MD5, GOOGLE_FILE_SIZE])
        ).execute()

    def get_children_by_id(self, resource_id):
        return self.connection.files().list(q="'{}' in parents".format(resource_id)).execute()['files']

