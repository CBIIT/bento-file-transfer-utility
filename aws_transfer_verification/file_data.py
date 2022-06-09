class FileData:
    """
    Contains meta-data for an individual file and verification status

    Attributes:
        path (str): The file's path
        size (int): The file's size in bytes
        md5 (str): The file's reference MD5 checksum
        etag (str): The file's AWS etag
        comment (str): A comment containing details of the file's verification results
        verified (bool): The file's verification status
    """
    def __init__(self, path, size, md5):
        self.path = path
        self.size = int(size)
        self.md5 = md5
        self.etag = None
        self.etag_format = None
        self.comment = None
        self.verified = False
