import os
import pickle
from datetime import datetime

DATE_FORMAT = '%Y-%m-%dT%H-%M'


def serialize_session(session):
    """
    Serialize a session object to a file
    :param session: The session object to serialize
    :return: None
    """
    try:
        time = session.verification_start
        time = time.strftime(DATE_FORMAT)
    except Exception:
        time = datetime.now().strftime(DATE_FORMAT)
    dir_path = 'tmp'
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)
    file_path = os.path.join(dir_path, '{}.session.dump'.format(time))
    session_file = open(file_path, 'wb')
    pickle.dump(session, session_file, pickle.HIGHEST_PROTOCOL)
    session_file.close()


def deserialize_session(path):
    """
    Deserialize a session file at the specified path and log previous verification within the session object
    :param path: The file path of the serialized session
    :return: The deserialized session object
    """
    session_file = open(path, 'rb')
    session = pickle.load(session_file)
    session.log_previous_verification_time()
    return session
