import unittest
from argparse import Namespace

import transfer_verification
from aws_transfer_verification import aws_client
from transfer_verification import *
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

def get_local_inputs():
    inputs = Namespace()
    inputs.s3_root_folder = "gmb/"
    inputs.local_root_folder = "C:\\Users\\muelleras\\Desktop"
    inputs.bucket = "bento-metadata"
    inputs.output_dir = "C:\\Users\\muelleras\\Desktop"
    inputs.file_manifest = "C:\\Users\\muelleras\\Desktop\\file-manifest.mock-data-2021-09-27.csv"
    return inputs


class TestClass(unittest.TestCase):
    def local_test(self):
        inputs = get_local_inputs()
        session = init_session(inputs)
        session = queue_files(inputs, session)
        session = verify_files(session)
        generate_output(inputs, session)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', datefmt=DATE_FORMAT, level=logging.INFO)
    unittest.main()
