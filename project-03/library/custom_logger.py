import logging
import os
import datetime


def init_logger(name: str = __name__) -> logging.Logger:
    """Generate a logger object.

    Args:
        name (str): Name of the logger, default to the name of the file in which this function is called

    Returns:
        logging.logger
    """

    # Init logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    add_stream_handler(logger)

    return logger


def add_stream_handler(logger: logging.Logger) -> None:
    """Function to add a stream handler to the logger object, so that log message are printed to the console.

    Args:
        logger (logging.logger): a logger object
    """

    stream_handler = logging.StreamHandler()
    stream_format = logging.Formatter("%(name)s:%(levelname)s:%(message)s")
    stream_handler.setFormatter(stream_format)
    logger.addHandler(stream_handler)


def add_file_handler(logger: logging.Logger) -> None:
    """Function to add a file handler to the logger object, so that log message are written to a log file.

    Args:
        logger (logging.Logger): a logger object
    """
    log_path = "./logs"
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{now}.log"

    file_handler = logging.FileHandler(
        filename=os.path.join(log_path, log_filename))
    file_format = logging.Formatter(
        "%(asctime)s:%(name)s:%(levelname)s:%(message)s")
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
