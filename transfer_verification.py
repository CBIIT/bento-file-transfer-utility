import argparse
import csv
import logging
import os
import shutil

from aws_transfer_verification import checksum_verification as verify
from datetime import datetime

from aws_transfer_verification.checksum_verification import NOT_RECALCULATED
from aws_transfer_verification.session import Session
from aws_transfer_verification.session_serialization import deserialize_session, serialize_session

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
FILE_NAME = "File Name"
FILE_PATH = "File Path"
SIZE = "Size"
REF_MD5 = "Reference MD5 Checksum"
REF_ETAG = "Reference Etag"
S3_ETAG = "S3 Etag"
CALC_ETAG = "Calculated Etag"
ETAG_FORMAT = "Etag Format"
VERIFIED = "Verified"
COMMENT = "Comment"


def main(inputs):
    session = init_session(inputs)
    session = queue_files(inputs, session)
    session = verify_files(session)
    generate_output(inputs, session)
    shutil.rmtree('tmp/')


def init_session(inputs):
    if inputs.session:
        return deserialize_session(inputs.session)
    else:
        return Session(inputs.local_root_folder, inputs.s3_root_folder, inputs.bucket)


def queue_files(inputs, session):
    # If file manifest(s) were specified in the inputs
    if inputs.file_manifest:
        # For each file manifest, add all the contained files to the session queue
        for file in inputs.file_manifest:
            with open(file) as csv_file:
                reader = csv.DictReader(csv_file)
                # Verify that each row has all the required information
                try:
                    for row in reader:
                        file_path = row[FILE_PATH]
                        file_size = row[SIZE]
                        if REF_MD5 in row and len(row[REF_MD5]) > 0:
                            file_ref_md5 = row[REF_MD5]
                        else:
                            file_ref_md5 = None
                        if REF_ETAG in row and len(row[REF_ETAG]) > 0:
                            file_ref_etag = row[REF_ETAG]
                        elif CALC_ETAG in row and not row[CALC_ETAG] == NOT_RECALCULATED:
                            file_ref_etag = row[CALC_ETAG]
                        else:
                            file_ref_etag = None
                        session.queue_file(file_path, file_size, file_ref_md5, file_ref_etag)
                except KeyError:
                    raise Exception("The file manifest format is incorrect, it must be a .csv file, and must contain "
                                    "the following columns with values for each row: {}".format(", ".join([FILE_PATH,
                                                                                                           SIZE])))
    return session


def verify_files(session):
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
    data = []
    for key in session.get_completed_and_failed():
        file_data = session.get_completed_and_failed()[key]
        path = file_data.path
        data.append({
            FILE_NAME: os.path.basename(path),
            FILE_PATH: path,
            SIZE: file_data.size,
            REF_MD5: file_data.ref_md5,
            REF_ETAG: file_data.ref_etag,
            S3_ETAG: file_data.s3_etag,
            CALC_ETAG: file_data.calc_etag,
            ETAG_FORMAT: file_data.etag_format,
            VERIFIED: file_data.verified,
            COMMENT: file_data.comment
        })
    with open(report_name, 'w') as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=data[0].keys(), lineterminator='\n')
        csv_writer.writeheader()
        csv_writer.writerows(data)
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
