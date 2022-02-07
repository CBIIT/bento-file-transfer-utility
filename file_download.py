import logging
import os

from folder_download import verify_md5
from folder_inventory import parse_arguments, verify_args, FILE_STATUS, generate_inventory_report
from google_authentication import authenticate_service_account
from google_drive_api import *


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
        file_name = metadata[GOOGLE_FILE_NAME]
        path = os.path.join(inputs.output_dir, file_name)
        try:
            logging.info("Downloading File: {}".format(file_name))
            api.download_file(path, metadata[GOOGLE_FILE_ID])
            # Verify the file download and store the verification result in the metadata status attribute
            try:
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
                # If an exception occurs during the file download, print an error message and update the file status
        except Exception as ex:
            logging.error("An error occurred while downloading file: {}".format(file_name))
            logging.error(ex)
            metadata[FILE_STATUS] = "Download Error"
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