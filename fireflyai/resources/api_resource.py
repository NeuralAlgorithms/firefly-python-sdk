from collections import OrderedDict
from typing import Dict

import requests

from fireflyai.api_requestor import APIRequestor
from fireflyai.firefly_response import FireflyResponse


class APIResource(object):
    @classmethod
    def class_url(cls):
        if cls == APIResource:
            raise NotImplementedError(
                "APIResource is an abstract class.  You should perform "
                "actions on its subclasses (e.g. Datasource, Dataset)"
            )
        # Namespaces are separated in object names with periods (.) and in URLs
        # with forward slashes (/), so replace the former with the latter.
        base = cls._CLASS_PREFIX.replace(".", "/")
        return base

    @classmethod
    def _list(cls, search_term: str = None, page: int = None, page_size: int = None, sort: Dict = None,
              filter_: Dict = None, api_key: str = None) -> FireflyResponse:
        requestor = APIRequestor()

        filters = requestor.parse_filter_parameters(filter_)
        sorts = requestor.parse_sort_parameters(sort)
        params = {'search_all_columns': search_term,
                  'page': page, 'page_size': page_size,
                  'sort': sorts, 'filter': filters
                  }

        response = requestor.get(url=cls.class_url(), params=params, api_key=api_key)
        return response

    @classmethod
    def _get(cls, id: int, api_key: str = None) -> FireflyResponse:
        requestor = APIRequestor()
        url = "{prefix}/{id}".format(prefix=cls.class_url(), id=id)
        response = requestor.get(url=url, api_key=api_key)
        return response

    @classmethod
    def _delete(cls, id: int, api_key: str = None) -> FireflyResponse:
        requestor = APIRequestor()
        url = "{prefix}/{id}".format(prefix=cls.class_url(), id=id)
        response = requestor.delete(url, api_key=api_key)
        return response
