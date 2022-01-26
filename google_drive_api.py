import datetime
import logging
from dateutil import tz

from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from httplib2 import Http

# Google Drive object metadata attribute keys
GOOGLE_FILE_NAME = "name"
GOOGLE_FILE_ID = "id"
GOOGLE_FILE_MD5 = "md5Checksum"
GOOGLE_FILE_SIZE = "size"
GOOGLE_FILE_MIMETYPE = "mimeType"
GOOGLE_FILE_LAST_MODIFIED = "modifiedTime"
GOOGLE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


def google_time_string_to_datetime(input_time):
    """
    Convert a UTC time string from Google into a local timezone datetime object
    :param input_time: The Google time string
    :return: The generated Datetime object
    """
    # Remove fractions of seconds
    time_string = input_time.split('.')[0]
    # Parse time string into datetime object
    time = datetime.datetime.strptime(time_string, GOOGLE_TIME_FORMAT)
    # Set datetime timezone property as UTC
    time = time.replace(tzinfo=tz.tzutc())
    # Convert time from UTC to local
    time = time.astimezone(tz.tzlocal())
    return time


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
            fields=','.join(
                [
                    GOOGLE_FILE_NAME,
                    GOOGLE_FILE_ID,
                    GOOGLE_FILE_MD5,
                    GOOGLE_FILE_SIZE,
                    GOOGLE_FILE_LAST_MODIFIED
                ]
            )
        ).execute()

    def get_children_by_id(self, resource_id):
        """
        Returns an array of the default metadata for all children of the the provided Google ID's associated object
        :param resource_id: Google ID associated with the desired children's parent object
        :return: Metadata map array for the provided Google ID's associated object's children
        """
        return self.connection.files().list(q="'{}' in parents".format(resource_id)).execute()['files']

    def download_file(self, path, google_id):
        """
        Downloads a file with the specified Google ID from Google Drive and saves it at the specified path
        :param path: The path to which the file is saved
        :param google_id: The Google ID of the target file
        """
        # Create the download request
        request = self.connection.files().get_media(fileId=google_id)
        # Create the initial last progress update time
        last_update = datetime.datetime.now()
        # Download the file
        with open(path, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            # Print out the download progress if it has not been printed in the last 30 seconds
            while done is False:
                status, done = downloader.next_chunk()
                while (last_update - datetime.datetime.now()) > datetime.timedelta(seconds=30):
                    last_update = datetime.datetime.now()
                    logging.info("Download %d%%." % int(status.progress() * 100))
