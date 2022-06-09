import unittest

from aws_transfer_verification import aws_client
from transfer_verification import *
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


class TestClass(unittest.TestCase):

    def test_get_etag(self):
        bucket = "bento-gmb-files"
        file_key = "Final/GMB/CCB010015.pdf"
        self.assertTrue(aws_client.get_file_etag(bucket, file_key) == "bbfe8c89bb6f8c294817b5d0cc54d45b")
        with self.assertRaises(Exception):
            bucket = "fake"
            aws_client.get_file_etag(bucket, file_key)
        with self.assertRaises(Exception):
            bucket = "bento-gmb-files"
            file_key = "fake"
            aws_client.get_file_etag(bucket, file_key)

    def test_verify_etag(self):
        test_data = [
            {
                'bucket': "bento-gmb-files",
                'file_key': "Final/GMB/wgEncodeUwRepliSeqBg02esG1bAlnRep1.bam.bai",
                'etag': "630dac10a11afbad7000dee6fc970848"
            },
            {
                'bucket': "bento-test-cloudfront",
                'file_key': "NA20811.10.bam",
                'etag': "e1512622cc3b25153a3280788733057c-118"
            }
        ]
        for entry in test_data:
            etag = aws_client.get_file_etag(entry['bucket'], entry['file_key'])
            logging.info("Calculated etag: "+etag)
            self.assertTrue(etag == entry['etag'])


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', datefmt=DATE_FORMAT, level=logging.INFO)
    unittest.main()
