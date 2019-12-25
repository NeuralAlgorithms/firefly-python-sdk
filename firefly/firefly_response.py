class FireflyResponse(object):
    def __init__(self, data: dict = {}):
        self._data = data

    def __getitem__(self, key):
        return self._data.get(key, None)

    def __repr__(self):
        return str(self._data)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def to_dict(self):
        return self._data