import argparse
import csv
import logging
import os

from aws_transfer_verification import checksum_verification as verify
from datetime import datetime
from aws_transfer_verification.session import Session
from aws_transfer_verification.session_serialization import deserialize_session, serialize_session

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def main(inputs):
    # Initialize session
    session = init_session(inputs)
    session = queue_files(inputs, session)
    session = verify_files(session)
    generate_output(inputs, session)


def init_session(inputs):
    """
    Create a new session or load one from file
    :param inputs: The user command line inputs object
    :return: A session object
    """
    # If a session file was specified, then deserialize it and return it
    # Else create and return a new session object
    if inputs.session:
        return deserialize_session(inputs.session)
    else:
        return Session(inputs.local_root_folder, inputs.s3_root_folder, inputs.bucket)


def queue_files(inputs, session):
    """
    Add the files contained in the file manifests to the session queue
    :param inputs: The command line inputs object
    :param session: The session object to which the files are added
    :return: The updated session object
    """
    # If file manifest(s) were specified in the inputs
    if inputs.file_manifest:
        # For each file manifest, add all the contained files to the session queue
        for file in inputs.file_manifest:
            with open(file) as csv_file:
                reader = csv.DictReader(csv_file)
                # Verify that each row has all the required information
                for row in reader:
                    file_path = row['File Path']
                    file_size = row['Size']
                    file_md5 = row['MD5 Checksum']
                    if file_path and file_size and file_md5:
                        session.queue_file(file_path, file_size, file_md5)
                    else:
                        raise Exception(
                            "The file manifest format is incorrect, it must be a .csv file, it must contain the "
                            "columns 'File Path', 'Size', and 'MD5 Checksum', and each row must have values for each"
                            "of the required columns")
    return session


def verify_files(session):
    """

    :param bucket:
    :param session:
    :return:
    """

    session.log_verification_start_time()
    while session.has_next_file():
        file_start = datetime.now()
        file_data = session.get_next_file()
        if file_data:
            logging.info("Verifying {}".format(file_data.path))
            try:
                file_data = verify.aws_verify(file_data, session.local_root_folder, session.s3_bucket,
                                                             session.s3_root_folder)
                if not file_data.verified:
                    session.fail_file(file_data, file_start)
                    continue
                else:
                    session.complete_file(file_data, file_start)
                    logging.info("Estimated time remaining {} hours".format(session.estimate_remaining_time()))
                    serialize_session(session)
            except Exception as ex:
                file_data.verified = False
                file_data.comment = str(ex)
                session.fail_file(file_data, file_start)
        else:
            raise Exception(
                "Critical error: unable to get file data from queue, please contact developer and provide the"
                " session file")
    session.log_verification_last_time()
    return session


def generate_output(inputs, session):
    dir_path = inputs.output_dir
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    report_name = "{}/{}_verification.csv".format(dir_path, datetime.now().strftime("%Y-%m-%dT%H-%M"))
    logging.info("Writing report {}".format(report_name))
    with open(report_name, 'w') as csvfile:
        csvwriter = csv.writer(csvfile, lineterminator='\n')
        csvwriter.writerow(["File Path", "Size", "Reference MD5 Checksum", "S3 Etag", "Calculated Etag", "Etag Format", "Verified",
                            "Comment"])
        for key in session.failed:
            file_data = session.failed[key]
            csvwriter.writerow([file_data.path, file_data.size, file_data.md5, file_data.etag, file_data.calculated_etag, file_data.etag_format,
                                file_data.verified, file_data.comment])
        for key in session.completed:
            file_data = session.completed[key]
            csvwriter.writerow([file_data.path, file_data.size, file_data.md5, file_data.etag, file_data.calculated_etag, file_data.etag_format,
                                file_data.verified, "Verified"])
    csvfile.close()


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s3r", "--s3-root-folder", help="The root folder of the s3 files")
    parser.add_argument("-lr", "--local-root-folder", help="The root folder of the local files")
    parser.add_argument("-b", "--bucket", required=True, help="The AWS S3 Bucket")
    parser.add_argument("-o", "--output-dir", required=True, help="Output directory for the verification log")
    parser.add_argument("-s", "--session",
                        help="Location the transfer session file to use to resume an interrupted transfer")
    parser.add_argument("-i", "--file-manifest", help="File Manifest CSV", action='append')
    return parser.parse_args()


def verify_args(inputs):
    """
    Verifies the following:
        - output directory is specified
        - a resume transfer file or file manifest is specified
        - all specified files and directories exist
        - all specified files are the expected file type
    :param inputs: The command line arguments to be verified
    :return: boolean verification result
    """
    if inputs.local_root_folder and not os.path.isdir(inputs.local_root_folder):
        logging.error("The specified local root folder must be a directory and must exist")
        return False
    elif not (inputs.file_manifest or inputs.session):
        logging.error("Either a file manifest file or a transfer session file must be specified")
        return False
    elif inputs.session and not os.path.isfile(inputs.session):
        logging.error("The specified transfer session file cannot be found")
        return False
    if inputs.file_manifest and len(inputs.file_manifest) > 0:
        for file in inputs.file_manifest:
            if not os.path.isfile(file):
                logging.error("The specified file manifest {} cannot be found".format(file))
                return False
    return True


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', datefmt=DATE_FORMAT, level=logging.INFO)
    args = parse_arguments()
    if verify_args(args):
        main(args)
    else:
        logging.error("Verification failed, please check error log for details")
