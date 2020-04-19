import logging
from logging import NullHandler

# Set default logging handler to avoid "No handler found" warnings.
logger = logging.getLogger(__name__)

token = None
api_base = 'https://api.firefly.ai'

from fireflyai import enums
from fireflyai.auth import authenticate
from fireflyai.resources import *


