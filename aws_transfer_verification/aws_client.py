import logging
import boto3


def get_file_etag(bucket, file_key):
    """
    Get the Etag for the given bucket and file key
    :param bucket: The S3 bucket
    :param file_key: The file key
    :return: The file's Etag
    """
    s3 = boto3.resource('s3')
    try:
        response = s3.meta.client.head_object(Bucket=bucket, Key=file_key)
        etag = response['ResponseMetadata']['HTTPHeaders']['etag']
        return etag.strip('\"')
    except Exception as ex:
        logging.error(ex)
        raise ex
