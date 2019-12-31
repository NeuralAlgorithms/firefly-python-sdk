from typing import Dict


class FireflyError(Exception):
    MESSAGE = "Please contact support"

    def __init__(self, message: str = None, json_body: str = None, headers: Dict = None, code: int = None):
        super().__init__(message or self.MESSAGE)

        self._message = message
        self.json_body = json_body
        self.headers = headers or {}
        self.code = code
        self.request_id = self.headers.get('X-Request-ID', None)

    def __str__(self):
        msg = self._message or "<empty message>"
        if self.request_id is not None:
            return "Request {0}: {1}".format(self.request_id, msg)
        else:
            return msg


class InvalidRequestError(FireflyError):
    pass


class APIError(FireflyError):
    pass


class APIConnectionError(FireflyError):
    pass


class AuthenticationError(FireflyError):
    pass


class PermissionError(AuthenticationError):
    pass
