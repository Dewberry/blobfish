"""General functions to set up formatting and enable custom logging"""
import logging


def log_setup(filename: str = None, level: int = logging.INFO) -> logging.Logger:
    """Sets up logs

    Args:
        filename (str, optional): Path to log file to use in storing logged messages. Defaults to None.
        level (int, optional): Minimum message level of logger. Defaults to logging.INFO.

    Returns:
        logging.Logger: Configured logger
    """
    logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter('{"time":"%(asctime)s", "level": "%(levelname)s", "message":%(message)s}')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    if filename:
        file_handler = logging.FileHandler(filename=filename)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    else:
        logger.addHandler(stream_handler)

    return logger
