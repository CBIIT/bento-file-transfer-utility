import csv
import argparse
import logging
import os.path

from googleapiclient.errors import HttpError
from google_authentication import authenticate_service_account
from datetime import datetime
from google_drive_api import *

FILE_PATH = "path"
FOLDER_TYPE = "application/vnd.google-apps.folder"


def get_folder_contents(api, folder_id):
    """
    Traverses the specified folder and all sub-folders and collects metadata for all encountered files
    :param api: The Google Drive API connection object
    :param folder_id: The Google Drive id of the folder to inventory
    :return: A list of metadata maps for all files contained in the specified folder and its sub-folders
    """
    # Verify the specified folder exists and query its metadata
    root_folder = None
    try:
        root_folder = api.get_folder_by_id(folder_id)
        logging.info("Opening folder {}".format(root_folder[GOOGLE_FILE_NAME]))
    except HttpError as error:
        logging.error("Unable to access folder with ID: {}".format(folder_id))
        logging.error(error)
    # Traverse specified folder and sub-folders
    inventory = None
    if root_folder:
        inventory = []
        root_folder[FILE_PATH] = root_folder[GOOGLE_FILE_NAME]
        folder_queue = [root_folder]
        # Loop while there are still un-traversed folders
        while len(folder_queue) != 0:
            current_folder = folder_queue.pop()
            try:
                # Query folder contents
                folder_children = api.get_children_by_id(current_folder[GOOGLE_FILE_ID])
                logging.info("Opening folder {}".format(current_folder[GOOGLE_FILE_NAME]))
                # Loop through folder contents
                for child in folder_children:
                    path = "{}/{}".format(current_folder[FILE_PATH], child[GOOGLE_FILE_NAME])
                    # Check if current object is a sub-folder or a file
                    if child[GOOGLE_FILE_MIMETYPE] == FOLDER_TYPE:
                        # Add sub-folder to the folder queue to be traversed later
                        child[FILE_PATH] = path
                        folder_queue.append(child)
                        logging.info("Found sub-folder {}".format(child[GOOGLE_FILE_NAME]))
                    else:
                        # Query file specific attributes and then save to the metadata to the inventory array
                        file = api.get_file_by_id(child[GOOGLE_FILE_ID])
                        file[FILE_PATH] = path
                        inventory.append(file)
                        logging.info("Found file {}".format(child[GOOGLE_FILE_NAME]))
            except HttpError as error:
                logging.error("Unable to access entity with ID: {} in path: {}"
                              .format(current_folder[GOOGLE_FILE_ID], current_folder[FILE_PATH]))
                logging.error(error)
    # Return the file metadata inventory
    return inventory


def generate_inventory_report(folder_inventory, output_dir):
    """
    Writes an inventory report using the supplied file metadata array
    :param folder_inventory: An array of file metadata
    :param output_dir: The output directory for the report
    """
    # Use the path attribute from a file in the folder inventory to get the root folder name
    root_name = (folder_inventory[0][FILE_PATH]).split('/')[0]
    # Create the folder inventory report name string
    file_name = "{}/{}.{}.csv".format(output_dir, root_name, datetime.now().strftime("%Y-%m-%d %H-%M-%S"))
    logging.info("Writing report {}".format(file_name))
    # Create an ordered list of the attributes to include in the inventory report
    attributes = [GOOGLE_FILE_NAME, FILE_PATH, GOOGLE_FILE_SIZE, GOOGLE_FILE_MD5, GOOGLE_FILE_ID]
    # Create a map of the attributes to display names for the report
    attribute_name_map = {
        GOOGLE_FILE_NAME: "File Name",
        FILE_PATH: "File Path",
        GOOGLE_FILE_SIZE: "Size",
        GOOGLE_FILE_MD5: "MD5 Checksum",
        GOOGLE_FILE_ID: "Google ID"
    }
    # Write folder inventory to CSV
    with open(file_name, 'w') as csvfile:
        csvwriter = csv.DictWriter(csvfile, lineterminator='\n', fieldnames=attributes)
        csvwriter.writerow(attribute_name_map)
        csvwriter.writerows(folder_inventory)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output-dir", help="Output directory")
    parser.add_argument("-i", "--google-id", help="Google Drive folder ID", action='append')
    return parser.parse_args()


def verify_args(inputs):
    """
    Verifies that the required inputs are present and that all inputs are valid
    :param inputs: The command line arguments
    :return: Boolean result of the inputs verification
    """
    # Check if at least one Google ID was provided as input and if a URL was provided then extract the Google ID
    ids = inputs.google_id
    if not ids:
        logging.error("At lease one folder id must be specified with the '-i' or '--folder-id' flag")
        return False
    else:
        for i, current_id in enumerate(ids):
            ids[i] = current_id.split('/')[-1]
    # If an output directory is specified then verify that the directory exists
    output_dir = inputs.output_dir
    if output_dir:
        if not os.path.isdir(output_dir):
            logging.error("The specified output directory does not exist")
            return False
    # Returns true after all verifications are completed
    return True


def main(inputs):
    # Create service account credentials
    credentials = authenticate_service_account()
    # Create Google Drive API connection using service account credentials
    api = API(credentials)
    # Loop through each provided Google ID
    for folder_id in inputs.google_id:
        # Get file metadata array using the current Google ID
        inventory = get_folder_contents(api, folder_id)
        # Generate a file inventory report using the file metadata array
        if inventory:
            generate_inventory_report(inventory, inputs.output_dir)


if __name__ == '__main__':
    # Configure logger
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
    # Parse and verify command line arguments
    args = parse_arguments()
    if verify_args(args):
        main(args)
