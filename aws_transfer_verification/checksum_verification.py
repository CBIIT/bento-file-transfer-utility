from hashlib import md5

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
    if s3_root_folder:
        s3_path = "/".join([s3_root_folder, s3_path])
    etag = aws_client.get_file_etag(bucket, s3_path)
    file_data.etag = etag
    if "-" in etag:
        file_data.verified = verify_etag(file_data, etag)
        file_data.etag_format = "Double Layered MD5 Checksum"
    else:
        file_data.verified = verify_md5(file_data, etag)
        file_data.etag_format = "MD5 Checksum"
    if not file_data.verified:
        file_data.comment = "The calculated Etag did not match the reference Etag"
    return file_data


def verify_md5(file_data, md5):
    return md5 == calculate_md5(file_data)


def verify_etag(file_data, etag):
    num_parts = int(etag.split('-')[1])
    return etag == _calculate_etag(file_data, num_parts)


def calculate_md5(file_data):
    md5hash = md5()
    with open(file_data.get_local_path(), 'rb') as file:
        data = file.read(BLOCK_SIZE_64KB)
        while data:
            md5hash.update(data)
            data = file.read(BLOCK_SIZE_64KB)
    return md5hash.hexdigest()


def _calculate_etag(file_data, num_parts):
    part_size = _determine_part_size(file_data.size, num_parts)
    md5_digests = []
    with open(file_data.get_local_path(), 'rb') as f:
        for chunk in iter(lambda: f.read(part_size), b''):
            md5_digests.append(md5(chunk).digest())
    return md5(b''.join(md5_digests)).hexdigest() + '-' + str(len(md5_digests))


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
