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
    ANOMALY_DETECTION = 'anomaly_detection'
    TIMESERIES_CALSSIFICATION = 'classification_timeseries'
    TIMESERIES_REGRESSION = 'regression_timeseries'
    TIMESERIES_ANOMALY_DETECTION = 'anomaly_timeseries'


from firefly.client import Client
