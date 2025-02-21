import logging
import os

LOG_FORMAT = f'[%(asctime)s]: {os.getpid()} %(levelname)s %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
HANDLERS = [logging.StreamHandler()]
LOG_LEVEL = logging.DEBUG


def setup_logger() -> logging.Logger:

    logging.basicConfig(level=LOG_LEVEL,
                        format=LOG_FORMAT,
                        datefmt=DATE_FORMAT,
                        handlers=HANDLERS)

    return logging.getLogger()
