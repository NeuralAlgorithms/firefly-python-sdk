import logging
from enum import Enum
from logging import NullHandler

# Set default logging handler to avoid "No handler found" warnings.
logger = logging.getLogger(__name__)
logger.addHandler(NullHandler())

ENDPOINT = 'api.firefly.ai'


class ProblemTypes(Enum):
    CLASSIFICATION = 'classification'
    REGRESSION = 'regression'
    # classification = 0
    # recommendation = 1
    # regression = 2
    # timeseries = 3
    # anomaly_detection = 4
    # multivariate_timeseries = 5
    # classification_timeseries = 6
    # anomaly_timeseries = 7
    # regression_timeseries = 8

from firefly.client import Client
