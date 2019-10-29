from collections import OrderedDict

import requests
from flask_log_request_id import current_request_id

from firefly import logger, ENDPOINT
from firefly.errors import FireflyClientError, ServiceException
from firefly.mixins.datasets_mixin import DatasetsMixin
from firefly.mixins.tasks_mixin import TasksMixin
from firefly.mixins.um_mixin import UMMixin


class _BaseClient:
    def __init__(self, endpoint, port, http=requests, timeout=None, use_https=True):
        self.query_prefix = ''
        self.endpoint = endpoint
        self.port = port
        self.http_client = http
        self.timeout = timeout
        self.protocol = 'https' if use_https else 'http'

    def set_endpoint(self, endpoint):
        self.endpoint = endpoint

    def set_port(self, port):
        self.port = port

    def ok(self, response):
        return response.json().get('result', response.json())

    def handled(self, response):
        raise FireflyClientError(response.json()['error'])

    def unhandled(self, response):
        try:
            raise ServiceException(response.json().get('error') or response.json().get('message'))
        except ValueError:
            logger.exception('reached endpoint but problem with api. status code {}'.format(response.status_code))
            raise FireflyClientError('API problem exception during request.')

    def __build_query(self, query, query_prefix=None):
        prefix_sep = '/'
        if query_prefix is None:
            prefix = self.query_prefix
        else:
            prefix = query_prefix
            if query_prefix == '':
                prefix_sep = ''
        full_query = '{0.protocol}://{0.endpoint}:{0.port}/{prefix}{sep}{query}'.format(self, prefix=prefix,
                                                                                        sep=prefix_sep,
                                                                                        query=query)
        if full_query.endswith('/'):
            full_query = full_query[:-1]
        return full_query

    def get(self, query, query_prefix=None, params=None):
        full_query = self.__build_query(query, query_prefix)
        headers = self.build_headers()
        try:
            response = self.http_client.get(full_query, params=params, timeout=self.timeout, headers=headers)
        except requests.RequestException as e:
            logger.exception('Failed query:{} with requestException {}'.format(full_query, e))
            raise FireflyClientError('Unable to reach Neural endpoint. Please check your connection.')
        return self._handle_response(response, full_query)

    def post(self, query, data=None, params=None, query_prefix=None):
        full_query = self.__build_query(query, query_prefix)
        headers = self.build_headers()
        try:
            response = self.http_client.post(full_query, json=data, params=params, timeout=self.timeout,
                                             headers=headers)
        except requests.RequestException as e:
            logger.exception('Failed query:{} with requestException {}'.format(full_query, e))
            raise FireflyClientError('Unable to reach Neural endpoint. Please check your connection.')
        return self._handle_response(response, full_query)

    def build_headers(self):
        return {'X-Request-ID': current_request_id()}

    def put(self, query, data=None, params=None, query_prefix=None):
        full_query = self.__build_query(query, query_prefix)
        try:
            response = self.http_client.put(full_query, json=data, params=params, timeout=self.timeout)
        except requests.RequestException as e:
            logger.exception('Failed query:{} with requestException {}'.format(full_query, e))
            raise FireflyClientError('Unable to reach Neural endpoint. Please check your connection.')
        return self._handle_response(response, full_query)

    def delete(self, query, data=None, params=None, query_prefix=None):
        full_query = self.__build_query(query, query_prefix)
        try:
            response = self.http_client.delete(full_query, json=data, params=params, timeout=self.timeout)
        except requests.RequestException as e:
            logger.exception('Failed query:{} with requestException {}'.format(full_query, e))
            raise FireflyClientError('Unable to reach a Neural endpoint. Please check your connection.')
        return self._handle_response(response, full_query)

    def _handle_response(self, response, full_query):
        response_json = {}
        try:
            response_json = response.json()
        except ValueError:
            pass
        if response.status_code != 200:
            logger.exception('Failed query:{} with error {}'.format(full_query, response.status_code))
            if response.status_code == 400 and response_json:
                # handled error. contains some description of the underlying error (but no traceback!)
                raise self.handled(response)
            else:  # unhandled error (500). contains only a description of the operation that failed (no traceback!)
                raise self.unhandled(response)
        else:
            return self.ok(response)

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


class Client(_BaseClient, UMMixin, DatasetsMixin, TasksMixin):
    def __init__(self, username, password, endpoint=ENDPOINT, port=443, use_https=True):
        super().__init__(endpoint=endpoint, port=port, use_https=use_https)
        self.token = self.login(username, password)['token']
