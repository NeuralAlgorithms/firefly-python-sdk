from typing import Dict

import requests

from firefly.api_requestor import APIRequestor
from firefly.firefly_response import FireflyResponse


class Datasource(object):
    CLASS_PREFIX = 'datasources'

    @classmethod
    def create(cls):
        pass
    
    @classmethod
    def list(cls, api_key: str = None, search_term: str = None, page: int = None, page_size: int = None, sort: Dict = None,
             filter: Dict = None):
        requestor = APIRequestor()

        filters = requestor.parse_filter_parameters(filter)
        sorts = requestor.parse_sort_parameters(sort)
        params = {'search_term': search_term,
                  'page': page, 'page_size': page_size,
                  'sort': sorts, 'filter': filters
                  }

        response = requestor.get(url=cls.CLASS_PREFIX, params=params, api_key=api_key)
        return response

    @classmethod
    def get(cls, id: int, api_key: str = None):
        requestor = APIRequestor()
        url = "{prefix}/{id}".format(prefix=cls.CLASS_PREFIX, id=id)
        response = requestor.get(url=url, api_key=api_key)
        return response


    @classmethod
    def delete(cls, id: int, api_key: str = None):
        requestor = APIRequestor()
        url = "{prefix}/{id}".format(prefix=cls.CLASS_PREFIX, id=id)
        response = requestor.delete(url, api_key=api_key)
        return response

    @classmethod
    def get_base_types(cls, id: int, api_key: str = None):
        requestor = APIRequestor()
        url = '{prefix}/{id}/data_types/base'.format(prefix=cls.CLASS_PREFIX, id=id)
        response = requestor.get(url, api_key=api_key)
        return response

    @classmethod
    def get_feature_types(cls, id: int, api_key: str = None):
        url = '{id}/data_types/feature'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    @classmethod
    def get_type_warnings(cls, id: int, api_key: str = None):
        api = '{data_id}/data_types/warnings'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')