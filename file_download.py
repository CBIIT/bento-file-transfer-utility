import hashlib
import os
from os.path import exists
from folder_inventory import *
from google_authentication import authenticate_service_account
from google_drive_api import *

# Block size used for file reading during MD5 Checksum verification
BLOCK_SIZE = 65536


def download_file(metadata, inputs, api):
    """
    Uses the provided Google API connection to download the specified file
    :param metadata: Target file metadata
    :param inputs: User inputs object
    :param api: Google Drive API connection
    """
    # Generate file output path
    if FILE_PATH in metadata:
        path = os.path.join(inputs.output_dir, metadata[FILE_PATH])
    else:
        path = os.path.join(inputs.output_dir, metadata[GOOGLE_FILE_NAME])
    # Create output folder hierarchy if necessary
    folder_name = os.path.dirname(path)
    if folder_name and not os.path.exists(folder_name):
        os.makedirs(folder_name)
    # Get file name
    file_name = os.path.basename(path)
    # If the file does not already exist or the utility is in overwrite mode
    if not exists(path) or inputs.mode == OVERWRITE_MODE:
        # Attempt to download the file and verify the MD5 checksum
        try:
            logging.info("Downloading File: {}".format(file_name))
            api.download_file(path, metadata[GOOGLE_FILE_ID])
            verify_file(path, metadata)
        # If an exception occurs during the file download, print an error message and update the file status
        except Exception as ex:
            logging.error("An error occurred while downloading file: {}".format(file_name))
            logging.error(ex)
            metadata[FILE_STATUS] = "Download Error"
    # Else if the utility is in verify mode
    elif inputs.mode == VERIFY_MODE:
        # Verify the MD5 checksum of the target file that already exists in the output directory
        verify_file(path, metadata)
    # Else update the status to record that the file already existed and the MD5 checksum verification was skipped
    else:
        metadata[FILE_STATUS] = "File Verification Skipped"


def verify_file(path, metadata):
    """

    :param path:
    :param metadata:
    :return:
    """
    try:
        file_name = os.path.basename(path)
        logging.info("Verifying File: {}".format(file_name))
        if verify_md5(path, metadata[GOOGLE_FILE_MD5]):
            logging.info("{} MD5 Checksum verified".format(file_name))
            metadata[FILE_STATUS] = "Checksum Verified"
        else:
            logging.warning("{} MD5 Checksum Mismatch".format(file_name))
            metadata[FILE_STATUS] = "Checksum Mismatched"
    # If an exception occurs during download verification, print an error message and update the file status
    except Exception as ex:
        logging.error("An error occurred while verifying checksum of file: {}".format(file_name))
        logging.error(ex)
        metadata[FILE_STATUS] = "Checksum Verification Error"


def verify_md5(file_path, md5):
    """
    Verify a file at the specified file path using the provided MD5 Checksum
    :param file_path: The file path of the file to verify
    :param md5: The MD5 Checksum used to verify
    :return: The Boolean result of the verification
    """
    # Generate an MD5 checksum and then verify that it matches the provided checksum
    md5hash = hashlib.md5()
    with open(file_path, 'rb') as file:
        data = file.read(BLOCK_SIZE)
        while data:
            md5hash.update(data)
            data = file.read(BLOCK_SIZE)
    calculated_md5 = md5hash.hexdigest()
    return md5 == calculated_md5


def main(inputs):
    # Create service account credentials
    credentials = authenticate_service_account()
    # Create Google Drive API connection using service account credentials
    api = API(credentials)
    # Create an array to store the downloaded files metadata
    downloaded_files = []
    # Loop through each provided Google ID
    logging.info("Beginning transfer")
    for file_id in inputs.google_id:
        metadata = api.get_file_by_id(file_id)
        download_file(metadata, inputs, api)
        # Append the file metadata to the download files array
        downloaded_files.append(metadata)
    # Generate and inventory report from the downloaded files array
    generate_inventory_report(downloaded_files, inputs.output_dir)
    # Print that the transfer has completed
    logging.info("Transfer Completed")


if __name__ == '__main__':
    # Configure logger
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
    # Parse and verify command line arguments
    args = parse_arguments()
    if verify_args(args):
        main(args)