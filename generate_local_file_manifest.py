import argparse
import csv
import glob
import logging
import os

from aws_transfer_verification import checksum_verification as verify

FILE_NAME = "File Name"
FILE_PATH = "File Path"
SIZE = "Size"
MD5 = "MD5 Checksum"
COLUMNS = [FILE_NAME, FILE_PATH, SIZE, MD5]


def main(inputs):
    root_dir_path = inputs.root_dir
    output_file_path = os.path.join(inputs.output_dir, "file-manifest.{}.csv".format(os.path.basename(root_dir_path)))
    logging.info("Collection File List")
    files = glob.glob(os.path.join(root_dir_path, '**'), recursive=True)
    file_list = []
    logging.info("Getting File Meta-Data")
    for file_path in files:
        try:
            if os.path.isfile(file_path):
                name = os.path.basename(file_path)
                size = os.path.getsize(file_path)
                md5 = verify.calculate_md5(file_path)
                file_path = os.path.relpath(file_path, root_dir_path).replace("\\", "/")
                file_list.append({
                    FILE_NAME: name,
                    SIZE: size,
                    FILE_PATH: file_path,
                    MD5: md5
                })
        except Exception:
            logging.error("Error getting metadata for file: {}".format(file_path))
    logging.info("Writing output file")
    with open(output_file_path, 'w', newline="\n") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(file_list)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output-dir")
    parser.add_argument("-i", "-r", "--root-dir")
    return parser.parse_args()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
    args = parse_arguments()
    main(args)
