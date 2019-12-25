import logging
from logging import NullHandler

# Set default logging handler to avoid "No handler found" warnings.
logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())

token = None
api_base = 'https://api.firefly.ai'
ENDPOINT = 'api.firefly.ai'

from firefly.client import Client
from firefly import enums

from firefly.auth import authenticate
from firefly.resources import *


