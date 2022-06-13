import logging
from aws_transfer_verification.file_data import FileData
from datetime import datetime


class Session:
    """
    Contains verification session state data

    Attributes:
        root_folder (str): 
        completed (dict): Map containing file data objects, indexed by file path, of the files that have been verified
        completed_data_size (int): The total size (bytes) of the files in the completed map
        queued (dict): Map containing file data objects, indexed by file path, of the files that are queued for verification
        queued_data_size (int): The total size (bytes) of the files in the queued map
        failed (dict): Map containing file data objects, indexed by file path, of the files that have failed verification
        failed_data_size (int): The total size (bytes) of the files in the failed map
        verification_start (datetime): The start time of the verification session
        verification_last (datetime): The last recorded time of the verification session
        verification_durations (list): The previous run durations if the session has been stopped previously
        completed_duration (float): The total duration of the verifications (in seconds) in which the verification was successful
        failure_duration (float): The total duration of the verifications (in seconds) in which the verification failed
    """

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

    def estimate_remaining_time(self):
        """
        Estimates the remaining verification time using completed verification times and the amount of remaining data
        left to verify
        :return: The estimated remaining time in hours
        """
        estimate = self.completed_duration / self.completed_data_size * self.queued_data_size
        return round(estimate / 3600, 2)

    def has_next_file(self):
        """
        Checks if there is file data left in the queue
        :return: Boolean result of the check
        """
        return len(self.queued) > 0

    def get_next_file(self):
        """
        Retrieves the next file data object from the queue without removing it
        :return: The next file data object
        """
        if self.has_next_file():
            return self.queued[list(self.queued.keys())[0]]
        else:
            return None

    def queue_file(self, path, file_size, md5):
        """
        Creates a new file data object, adds it to the queue, updates the queue data size counter
        :param path: The file path
        :param file_size: The file's size in bytes
        :param md5: The file's MD5 hash
        :return: None
        """
        file_data = FileData(path, file_size, md5)
        if self.local_root_folder:
            file_data.local_root = self.local_root_folder
        self.queued[file_data.path] = file_data
        self.queued_data_size += file_data.size

    def complete_file(self, file_data, file_start_time):
        """
        Adds the file data to the completed map, removes it from the queued map, updates the data size counters
        :param file_data: The file data to store as complete
        :param file_start_time:
        :return: None
        """
        self.completed[file_data.path] = file_data
        self.completed_data_size += file_data.size
        self.queued.pop(file_data.path)
        self.queued_data_size -= file_data.size
        self.completed_duration += (datetime.now() - file_start_time).total_seconds()

    def fail_file(self, file_data, file_start_time):
        """
        Adds the file data to the failed map, removes it from the queued map, updates the data size counters,
        stores comment in the file datas
        :param file_data: The file data to store as complete
        :param file_start_time:
        :param comment: Comment as to why the file failed
        :return: None
        """
        self.failed[file_data.path] = file_data
        self.failed_data_size += file_data.size
        self.queued.pop(file_data.path)
        self.queued_data_size -= file_data.size
        self.failure_duration += (datetime.now() - file_start_time).total_seconds()

    def log_previous_verification_time(self):
        """
        Calculated the time between the verification start and the last recorded time and then stores it in the
        verification durations array
        :return: None
        """
        if self.verification_start and self.verification_last:
            self.verification_durations.append(self.verification_last - self.verification_start)

    def log_verification_start_time(self):
        """
        Clear previous verification start and stop times, record current time as verification start time
        :return: None
        """
        logging.info("Verification Started")
        self.verification_start = datetime.now()
        self.verification_last = None

    def log_verification_last_time(self):
        """
        If verification start time has been recorded, then record current time as verification stop time.
        Calculate and store verification duration
        :return: None
        """
        logging.info("Session Time Recorded")
        self.verification_last = datetime.now()
