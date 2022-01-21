import csv
import argparse
import logging
import os.path

from googleapiclient.errors import HttpError
from google_authentication import authenticate_service_account
from datetime import datetime
from google_drive_api import *

# TODO Convert print statements to logging
# TODO Code Google IDs as command line inputs

FILE_PATH = "path"
FOLDER_TYPE = "application/vnd.google-apps.folder"


def get_folder_contents(api, folder_id):
    root_folder = None
    try:
        root_folder = api.get_folder_by_id(folder_id)
        logging.info("Opening folder {}".format(root_folder[GOOGLE_FILE_NAME]))
    except HttpError as error:
        logging.error("Unable to access folder with ID: {}".format(folder_id))
        logging.error(error)

    file_manifest = None
    if root_folder:
        file_manifest = []
        root_folder[FILE_PATH] = root_folder[GOOGLE_FILE_NAME]
        folder_queue = [root_folder]
        while len(folder_queue) != 0:
            current_folder = folder_queue.pop()
            try:
                folder_children = api.get_children_by_id(current_folder[GOOGLE_FILE_ID])
                logging.info("Opening folder {}".format(current_folder[GOOGLE_FILE_NAME]))
                for child in folder_children:
                    path = "{}/{}".format(current_folder[FILE_PATH], child[GOOGLE_FILE_NAME])
                    if child[GOOGLE_FILE_MIMETYPE] == FOLDER_TYPE:
                        child[FILE_PATH] = path
                        folder_queue.append(child)
                        logging.info("Found sub-folder {}".format(child[GOOGLE_FILE_NAME]))
                    else:
                        file = api.get_file_by_id(child[GOOGLE_FILE_ID])
                        file[FILE_PATH] = path
                        file_manifest.append(file)
                        logging.info("Found file {}".format(child[GOOGLE_FILE_NAME]))
            except HttpError as error:
                logging.error("Unable to access entity with ID: {} in path: {}"
                      .format(current_folder[GOOGLE_FILE_ID], current_folder[FILE_PATH]))
                logging.error(error)
    return file_manifest


# TODO Use file output path from command line input
def generate_manifest_report(file_manifest, output_dir):
    # Use the path attribute from a file in the file manifest to get the root folder name
    root_name = (file_manifest[0][FILE_PATH]).split('/')[0]
    # Create the file manifest report name string
    file_name = "{}/{}.{}.csv".format(output_dir,root_name, datetime.now().strftime("%Y-%m-%d %H-%M-%S"))
    logging.info("Writing report {}".format(file_name))
    # Create an ordered list of the attributes to include in the manifest report
    attributes = [GOOGLE_FILE_NAME, FILE_PATH, GOOGLE_FILE_SIZE, GOOGLE_FILE_MD5, GOOGLE_FILE_ID]
    # Create a map of the attributes to display names for the report
    attribute_name_map = {
        GOOGLE_FILE_NAME: "File Name",
        FILE_PATH: "File Path",
        GOOGLE_FILE_SIZE: "Size",
        GOOGLE_FILE_MD5: "MD5 Checksum",
        GOOGLE_FILE_ID: "Google ID"
    }
    # Write file manifest to CSV
    with open(file_name, 'w') as csvfile:
        csvwriter = csv.DictWriter(csvfile, lineterminator='\n', fieldnames=attributes)
        csvwriter.writerow(attribute_name_map)
        csvwriter.writerows(file_manifest)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output-dir", help="Output directory")
    parser.add_argument("-i", "--folder-id", help="Google Drive folder ID", action='append')
    return parser.parse_args()


def verify_args(args):
    ids = args.folder_id
    if not ids:
        logging.error("At lease one folder id must be specified with the '-i' or '--folder-id' flag")
        return False
    output_dir = args.output_dir
    if output_dir:
        if not os.path.isdir(output_dir):
            logging.error("The specified output directory does not exist")
            return False
    return True


def main(args):
    # Create service account credentials
    credentials = authenticate_service_account()
    # Create Google Drive API connection using credentials
    api = API(credentials)
    # Read folder contents and generate file manifest report for each input folder id
    for folder_id in args.folder_id:
        file_manifest = get_folder_contents(api, folder_id)
        if file_manifest:
            generate_manifest_report(file_manifest, args.output_dir)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
    args = parse_arguments()
    if verify_args(args):
        main(args)
