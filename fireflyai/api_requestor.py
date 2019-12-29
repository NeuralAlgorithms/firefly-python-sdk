import os
from collections import OrderedDict

import requests
import uuid

import fireflyai
from fireflyai.errors import FireflyError, AuthenticationError, APIError
from fireflyai.firefly_response import FireflyResponse


class APIRequestor(object):
    def __init__(self, http_client=None):
        if http_client is None:
            self._http_client = requests
        else:
            self._http_client = http_client

    def parse_filter_parameters(self, filter):
        if filter:
            filters = []
            for field, values in filter.items():
                for value in values:
                    filters.append("{}:{}".format(field, value))
            return filters

    def parse_sort_parameters(self, sort):
        assert sort is None or isinstance(sort, OrderedDict)
        if sort:
            sorts = []
            for field, value in sort.items():
                sorts.append('{}:{}'.format(field, value))
            return sorts

    def request(self, method, url, headers=None, body=None, params=None, api_key=None):
        if api_key is None:
            token = self._get_token()
        else:
            token = api_key

        rheaders = self._build_headers()
        rheaders.update(**(headers or {}))

        params = {'jwt': token, **(params or {})}
        abs_url = "{base_url}/{url}".format(base_url=fireflyai.api_base, url=url)
        response = self._http_client.request(method=method, url=abs_url, headers=rheaders, json=body, params=params)
        return self._handle_response(response)

    def post(self, url, headers=None, body=None, params=None, api_key=None):
        return self.request("POST", url, headers, body, params, api_key)

    def get(self, url, headers=None, body=None, params=None, api_key=None):
        return self.request("GET", url, headers, body, params, api_key)

    def delete(self, url, headers=None, body=None, params=None, api_key=None):
        return self.request("DELETE", url, headers, body, params, api_key)

    def put(self, url, headers=None, body=None, params=None, api_key=None):
        return self.request("PUT", url, headers, body, params, api_key)

    def _build_headers(self):
        return {'X-Request-ID': str(uuid.uuid4())}

    def _handle_response(self, response):
        response_json = {}
        try:
            response_json = response.json()
        except ValueError:
            pass
        if response.status_code != 200:
            if response.status_code == 400 and response_json:
                # handled error. contains some description of the underlying error (but no traceback!)
                raise self._handled(response)
            else:  # unhandled error (500). contains only a description of the operation that failed (no traceback!)
                raise self._unhandled(response)
        else:
            if response_json:
                return self._handle_json(response)
            else:
                return FireflyResponse(headers=response.headers, status_code=response.status_code)

    def _handle_json(self, response):
        response_json = response.json()
        if 'result' not in response_json:
            result = FireflyResponse(data={'result': response_json}, headers=response.headers,
                                     status_code=response.status_code)
        else:
            response_type = type(response_json['result'])
            if response_type == dict:
                result = FireflyResponse(data=response_json.get('result', response_json), headers=response.headers,
                                         status_code=response.status_code)
            elif response_type == bool:
                result = FireflyResponse(data=response_json, headers=response.headers,
                                         status_code=response.status_code)
            elif response_type == int:
                result = FireflyResponse(data={'id': response_json['result']}, headers=response.headers,
                                         status_code=response.status_code)
            else:
                result = FireflyResponse(data=response_json, headers=response.headers,
                                         status_code=response.status_code)
        return result

    def _handled(self, response):
        raise FireflyError(response.json()['error'])

    def _unhandled(self, response):
        if response.status_code == 401:
            raise AuthenticationError("token expired.")  # todo retry after renew
        try:
            raise APIError(response.json().get('error') or response.json().get('message'))
        except ValueError:
            # logger.exception('reached endpoint but problem with api. status code {}'.format(response.status_code))
            raise APIError('API problem exception during request.')

    def _get_token(self):
        if fireflyai.token is None:
            fireflyai.token = os.getenv("FIREFLY_TOKEN", None)
            if fireflyai.token is None:
                raise FireflyError("No token found. Please use `fireflyai.authenticate()` to create a token,"
                                   "or use `FIREFLY_TOKEN` environment variable to manually use on."
                                   "If problem persists, please contact support.")
        return fireflyai.token
