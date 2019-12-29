class FireflyResponse(object):
    def __init__(self, data: dict = None, headers: dict = None, status_code: int = None):
        self.headers = headers or {}
        self._data = data or {}
        self.status_code = status_code

    def __getitem__(self, key):
        return self._data.get(key, None)

    def __repr__(self):
        return str(self._data)

    def __contains__(self, item):
        return item in self._data

    def get(self, key, default=None):
        return self._data.get(key, default)

    def to_dict(self):
        return self._data

    @property
    def request_id(self):
        return self.headers.get("X-Request-ID", None)
