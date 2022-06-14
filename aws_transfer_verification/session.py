import logging
from aws_transfer_verification.file_data import FileData
from datetime import datetime


class Session:
    def __init__(self, local_root, s3_root, bucket):
        self.local_root_folder = local_root
        self.s3_root_folder = s3_root
        self.s3_bucket = bucket
        self.completed = {}
        self.completed_data_size = 0
        self.queued = {}
        self.queued_data_size = 0
        self.failed = {}
        self.failed_data_size = 0
        self.verification_start = None
        self.verification_last = None
        self.verification_durations = []
        self.completed_duration = 0
        self.failure_duration = 0

    def get_completed_and_failed(self):
        return {**self.failed, **self.completed}

    def estimate_remaining_time(self):
        estimate = self.completed_duration / self.completed_data_size * self.queued_data_size
        return round(estimate / 3600, 2)

    def has_next_file(self):
        return len(self.queued) > 0

    def get_next_file(self):
        if self.has_next_file():
            return self.queued[list(self.queued.keys())[0]]
        else:
            return None

    def queue_file(self, path, file_size, ref_md5, ref_etag):
        file_data = FileData(path, file_size, ref_md5, ref_etag)
        if self.local_root_folder:
            file_data.local_root = self.local_root_folder
        self.queued[file_data.path] = file_data
        self.queued_data_size += file_data.size

    def complete_file(self, file_data, file_start_time):
        self.completed[file_data.path] = file_data
        self.completed_data_size += file_data.size
        self.queued.pop(file_data.path)
        self.queued_data_size -= file_data.size
        self.completed_duration += (datetime.now() - file_start_time).total_seconds()

    def fail_file(self, file_data, file_start_time):
        self.failed[file_data.path] = file_data
        self.failed_data_size += file_data.size
        self.queued.pop(file_data.path)
        self.queued_data_size -= file_data.size
        self.failure_duration += (datetime.now() - file_start_time).total_seconds()

    def log_previous_verification_time(self):
        if self.verification_start and self.verification_last:
            self.verification_durations.append(self.verification_last - self.verification_start)

    def log_verification_start_time(self):
        logging.info("Verification Started")
        self.verification_start = datetime.now()
        self.verification_last = None

    def log_verification_last_time(self):
        logging.info("Session Time Recorded")
        self.verification_last = datetime.now()
