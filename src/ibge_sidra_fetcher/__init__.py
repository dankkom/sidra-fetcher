import logging

from .fetcher import SidraClient

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
