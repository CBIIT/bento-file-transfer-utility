import hashlib
import os

from aws_transfer_verification import aws_client

BLOCK_SIZE_64KB = 65536
BLOCK_SIZE_1MB = 1048576
BLOCK_SIZE_8MB = 8388608
BLOCK_SIZE_15MB = 15728640


def aws_verify(file_data, local_root_folder, bucket, s3_root_folder):
    """
    Retrieve file Etag from AWS, then detect Etag format and calculate it locally to verify
    :param s3_root_folder:
    :param file_data: File data of the file to verify
    :param local_root_folder: The root folder of the files to verify
    :param bucket: The AWS bucket containing the file
    :return: Boolean result of the Etag verification
    """
    s3_path = file_data.path
    local_path = file_data.path
    if s3_root_folder:
        s3_path = os.path.join(s3_root_folder, s3_path)
    if local_root_folder:
        local_path = os.path.join(local_root_folder, local_path)
    etag = aws_client.get_file_etag(bucket, s3_path)
    if "-" in etag:
        file_data.verified = verify_etag(local_path, etag)
        file_data.etag_format = "Double Layered MD5 Checksum"
    else:
        file_data.verified = verify_md5(local_path, etag)
        file_data.etag_format = "MD5 Checksum"
    if not file_data.verified:
        file_data.comment = "The calculated Etag did not match the reference Etag"


def verify_md5(local_path, md5):
    """
    Calculate the MD5 checksum of the specified file and verify that it matches the provided MD5 checksum
    :param local_path: The path of the file to verify
    :param md5: The verification MD5 checksum
    :return: Boolean result of the MD5 verification
    """
    return md5 == calculate_md5(local_path)


def verify_etag(local_path, etag):
    """
    Calculate the Etag of the specified file and verify that it matches the provided Etag
    :param local_path: The path of the file to verify
    :param etag: The verification Etag
    :return: Boolean result of the Etag verification
    """
    num_parts = int(etag.split('-')[1])
    return etag == _calculate_etag(local_path, num_parts)


def calculate_md5(local_path):
    md5hash = hashlib.md5()
    with open(local_path, 'rb') as file:
        data = file.read(BLOCK_SIZE_64KB)
        while data:
            md5hash.update(data)
            data = file.read(BLOCK_SIZE_64KB)
    return md5hash.hexdigest()


def _calculate_etag(file_data, num_parts):
    part_size = _determine_part_size(file_data.size, num_parts)
    md5hash = hashlib.md5()
    md5_digests = []
    with open(file_data.path, 'rb') as f:
        for chunk in iter(lambda: f.read(part_size), b''):
            md5_digests.append(md5hash(chunk).digest())
    return md5hash(b''.join(md5_digests)).hexdigest() + '-' + str(len(md5_digests))


def _determine_part_size(size, num_parts):
    part_sizes = [
        BLOCK_SIZE_8MB,
        BLOCK_SIZE_15MB,
        _factor_of_1MB(size, num_parts)
    ]
    for part_size in part_sizes:
        if num_parts * part_size >= size > (num_parts - 1) * part_size:
            return part_size
    raise Exception("Could not determine part size")


def _factor_of_1MB(filesize, num_parts):
    x = filesize / int(num_parts)
    y = x % BLOCK_SIZE_1MB
    return int(x + BLOCK_SIZE_1MB - y)
