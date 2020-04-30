import os
from collections import OrderedDict

import requests
import uuid

import fireflyai
from fireflyai.errors import AuthenticationError, APIError, InvalidRequestError, APIConnectionError, PermissionError
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
        if method not in ['GET', 'POST', 'PUT', 'DELETE']:
            raise APIConnectionError(
                "Unrecognized HTTP method {method}. This may indicate a bug in the Firefly "
                "internal bindings. Please contact support for assistance.".format(method=method)
            )

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
        if 200 <= response.status_code < 300:
            return self._handle_ok(response, response_json)
        else:
            if 400 <= response.status_code < 500 and response_json:
                raise self._handled(response)
            else:
                raise self._unhandled(response)

    def _handle_ok(self, response, response_json):
        if not response_json:
            return FireflyResponse(headers=response.headers, status_code=response.status_code)

        if 'result' not in response_json:
            response_json = {'result': response_json}

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
        response_json = response.json()
        if response.status_code in [400, 404]:
            err = InvalidRequestError(message=response_json['error'], headers=response.headers,
                                      code=response.status_code, json_body=response_json)
        elif response.status_code == 401:
            err = AuthenticationError(message="Invalid token.", headers=response.headers,
                                      code=response.status_code, json_body=response_json)
        elif response.status_code == 403:
            err = PermissionError(message=response_json['message'], headers=response.headers, code=response.status_code,
                                  json_body=response_json)
        else:
            err = APIError(message=response_json['message'], headers=response.headers, code=response.status_code,
                           json_body=response_json)
        return err

    def _unhandled(self, response):
        try:
            raise APIError(response.json().get('error') or response.json().get('message'))
        except ValueError:
            raise APIError('API problem exception during request.')

    def _get_token(self):
        if fireflyai.token is None:
            fireflyai.token = os.getenv("FIREFLY_TOKEN", None)
            if fireflyai.token is None:
                raise AuthenticationError("No token found. Please use `fireflyai.authenticate()` to create a token,"
                                          "or use `FIREFLY_TOKEN` environment variable to manually use one."
                                          "If problem persists, please contact support.")
        return fireflyai.token
