from google_drive_transfer.google_drive_api import *

BYTES_IN_GB = 1073741824

class Metrics:

    def __init__(self, file_data):
        """
        Initialize a metrics object used to estimate the remaining download time based on previous download speeds
        :param file_data: The metadata array of the files to be downloaded
        """
        self.data_points = 0
        self.start_time = None
        self.total_size = 0
        for file in file_data:
            file_size = int(file[GOOGLE_FILE_SIZE]) / BYTES_IN_GB
            self.total_size += file_size
        self.remaining_size = self.total_size

    def log_start(self):
        """
        Store the current time as the start time of the download
        """
        self.start_time = datetime.datetime.now()

    def update_estimate(self, file_size):
        """
        Update the and return a remaining time estimate after successfully downloading a file
        :param file_size: The size of the file downloaded
        :return: An estimate of the remaining download time
        """
        # Update remaining size of data to be downloaded
        file_size = int(file_size) / BYTES_IN_GB
        self.remaining_size -= file_size
        # If there have been less than 3 downloads, return that there is not enough data yet for an estimate
        if self.data_points < 3:
            self.data_points += 1
            return "Not yet enough data for estimate"
        # Generate an estimate using running time, the amount of data downloaded, and the amount of data remaining
        else:
            elapsed_time = datetime.datetime.now() - self.start_time
            downloaded_size = self.total_size - self.remaining_size
            estimate = elapsed_time * self.remaining_size / downloaded_size
            return estimate

