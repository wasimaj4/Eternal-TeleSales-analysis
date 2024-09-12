import logging
from logging.handlers import TimedRotatingFileHandler
import os

def create_timed_rotating_log(path: str) -> logging.Logger:
    """
    Create and configure a logger with a TimedRotatingFileHandler.

    This function sets up a logger that writes to a file, rotating the log file
    at certain timed intervals. The log files are backed up and older logs are
    deleted based on the specified parameters.

    :param path: The path to the log file.
    :type path: str
    :return: A configured logger object.
    :rtype: logging.Logger

    :Example:

    >>> get_logger = create_timed_rotating_log("/path/to/log/file.log")
    >>> get_logger.info("This is a log message")
    """
    get_logger = logging.getLogger("Rotating Log")
    get_logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler(
        path,
        when="m",
        interval=1,
        backupCount=5
    )
    get_logger.addHandler(handler)

    return get_logger

current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the log file relative to the current file
log_file = os.path.join(current_dir, "..", "utils", "logs", "current_log.log")
logger: logging.Logger = create_timed_rotating_log(log_file)
