from collections import OrderedDict

import requests

import firefly
from firefly.errors import FireflyError, AuthenticationError, APIError
from firefly.firefly_response import FireflyResponse


class APIRequestor(object):
    BASE_URL = "api.firefly.ai"

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

        params = {'jwt': token, **(params or {})}
        abs_url = "{base_url}/{url}".format(base_url=firefly.api_base, url=url)
        response = self._http_client.request(method=method, url=abs_url, headers=headers, json=body, params=params)
        return self._handle_response(response)

    def post(self, url, headers=None, body=None, params=None, api_key=None):
        return self.request("POST", url, headers, body, params, api_key)

    def get(self, url, headers=None, body=None, params=None, api_key=None):
        return self.request("GET", url, headers, body, params, api_key)

    def delete(self, url, headers=None, body=None, params=None, api_key=None):
        return self.request("DELETE", url, headers, body, params, api_key)

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
                if 'result' in response_json and isinstance(response_json['result'], dict):
                    return FireflyResponse(data=response_json.get('result', response_json))
                elif 'result' in response_json and isinstance(response_json['result'], bool):
                    return FireflyResponse(data=response_json)
                elif 'result' in response_json and isinstance(response_json['result'], int):
                    return FireflyResponse(data={'id': response_json['result']})
                else:
                    return FireflyResponse(data={'result': response_json})
            else:
                return FireflyResponse()

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
        return firefly.token

    def _parse_filter_parameters(self, filter):
        if filter:
            filters = []
            for field, values in filter.items():
                for value in values:
                    filters.append("{}:{}".format(field, value))
            return filters

    def _parse_sort_parameters(self, sort):
        assert sort is None or isinstance(sort, OrderedDict)
        if sort:
            sorts = []
            for field, value in sort.items():
                sorts.append('{}:{}'.format(field, value))
            return sorts
