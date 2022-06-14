import os.path


class FileData:
    def __init__(self, path, size, ref_md5, ref_etag):
        self.path = path
        self.size = int(size)
        self.ref_md5 = ref_md5
        self.ref_etag = ref_etag
        self.s3_etag = None
        self.etag_format = None
        self.comment = None
        self.verified = False
        self.local_root = None
        self.calc_etag = None

    def get_local_path(self):
        if self.local_root:
            return os.path.join(self.local_root, self.path)
