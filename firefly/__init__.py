import logging
from logging import NullHandler

# Set default logging handler to avoid "No handler found" warnings.
logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())

ENDPOINT = 'api.firefly.ai'

from firefly.client import Client
