import abc
import logging

logger = logging.getLogger('clients')

class FireflyClientError(Exception, abc.ABC):
    MESSAGE = "Please contact support"

    def __init__(self, message=None):
        self.indicative = True
        super().__init__(message or self.MESSAGE)


class ErrorCodes:
    MAP = {
        0: FireflyClientError
    }

    @classmethod
    def get_code(cls, e):
        reverse_map = dict((v, k) for k, v in cls.MAP.items())
        return reverse_map.get(e.__class__, 0)

    @classmethod
    def get_code_by_class(cls, value):
        reverse_map = dict((v, k) for k, v in cls.DS_CODES.items())
        return reverse_map.get(value, 0)

    @classmethod
    def get_message(cls, code):
        return cls.MAP.get(code, cls.MAP[0]).MESSAGE


class ServiceException(Exception):
    pass
