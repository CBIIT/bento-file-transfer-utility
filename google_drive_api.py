from googleapiclient import discovery
from httplib2 import Http

# Google Drive object metadata attribute keys
GOOGLE_FILE_NAME = "name"
GOOGLE_FILE_ID = "id"
GOOGLE_FILE_MD5 = "md5Checksum"
GOOGLE_FILE_SIZE = "size"
GOOGLE_FILE_MIMETYPE = "mimeType"


class API:

    def __init__(self, credentials):
        """
        Establish a Google Drive API connection using the provided credentials
        :param credentials: Credentials used to connect to the Google Drive API
        """
        self.connection = discovery.build('drive', 'v3', http=credentials.authorize(Http()))

    def get_folder_by_id(self, resource_id):
        """
        Returns the default Google Drive Object metadata for the provided Google ID's associated object
        :param resource_id: Google ID associated with the desired metadata
        :return: Metadata map for the provided Google ID's associated object
        """
        return self.connection.files().get(fileId=resource_id).execute()

    def get_file_by_id(self, resource_id):
        """
        Returns file specific metadata for the provided Google ID's associated object
        :param resource_id: Google ID associated with the desired metadata
        :return: Metadata map for the provided Google ID's associated object
        """
        return self.connection.files().get(
            fileId=resource_id,
            fields=','.join([GOOGLE_FILE_NAME, GOOGLE_FILE_ID, GOOGLE_FILE_MD5, GOOGLE_FILE_SIZE])
        ).execute()

    def get_children_by_id(self, resource_id):
        """
        Returns an array of the default metadata for all children of the the provided Google ID's associated object
        :param resource_id: Google ID associated with the desired children's parent object
        :return: Metadata map array for the provided Google ID's associated object's children
        """
        return self.connection.files().list(q="'{}' in parents".format(resource_id)).execute()['files']

