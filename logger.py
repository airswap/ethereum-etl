import os
import logging
from pythonjsonlogger import jsonlogger

LOGGER = logging.getLogger()
LOGGER.handlers = list()

LOGGER.setLevel(logging.INFO)
handler = logging.StreamHandler()

if os.environ.get("KUBERNETES"):
    format_str = '%(asctime)%(message)%(levelname)' \
                 '%(pathname)%(funcName)%(lineno)' \
                 '%(process)%(processName)%(threadName)%(thread)'
    handler.setFormatter(jsonlogger.JsonFormatter(format_str))

LOGGER.addHandler(handler)
