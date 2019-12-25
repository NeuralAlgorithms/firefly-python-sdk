import io
import os
from typing import Dict

from firefly import utils
from firefly.api_requestor import APIRequestor
from firefly.errors import APIError
from firefly.firefly_response import FireflyResponse
from firefly.resources.api_resource import APIResource


class Datasource(APIResource):
    CLASS_PREFIX = 'datasources'

    @classmethod
    def list(cls, search_term: str = None, page: int = None, page_size: int = None, sort: Dict = None,
              filter_: Dict = None, api_key: str = None) -> FireflyResponse:
        """
        List the existing datasources. Supports filtering, sorting and pagination.

        Args:
            search_term (Optional[str]): Return only records that contain the search_term in one of their fields.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to sort the results by.
            filter_ (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Datasources, which are represented as nested dictionaries under `hits`.
        """
        return cls._list(search_term, page, page_size, sort, filter_, api_key)

    @classmethod
    def get(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get information on a specific datasource.

        Information includes the state of the datasource, and other basic attributes.

        Args:
            id (int): Datasource ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Information about the datasource.
        """
        return cls._get(id, api_key)

    @classmethod
    def delete(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Delete a specific datasource.

        Args:
            id (int): Datasource ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: "true" if deleted successfuly, raises FireflyClientError otherwise.
        """
        return cls._delete(id, api_key)

    @classmethod
    def create(cls, filename: str, wait: bool = False, skip_if_exists: bool = False,
               api_key: str = None) -> FireflyResponse:
        """
        Uploads a file to the server and creates a datasource.

        Args:
            filename (str): File to be uploaded.
            wait (Optional[bool]): Should call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if datasource with same name exists and skip if true.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Datasource ID if successful, raises FireflyError otherwise.
        """
        data_source_name = os.path.basename(filename)

        existing_ds = cls.list(filter_={'name': [data_source_name]}, api_key=api_key)
        if existing_ds and existing_ds['total'] > 0:
            if skip_if_exists:
                return FireflyResponse(data=existing_ds['hits'][0])
            else:
                raise APIError("Datasource with that name exists")

        aws_credentials = cls.__get_upload_details(api_key=api_key)
        utils.s3_upload(data_source_name, filename, aws_credentials.to_dict())

        return cls._create(data_source_name, wait=wait, api_key=api_key)

    @classmethod
    def create_from_dataframe(cls, df, data_source_name: str, wait: bool = False, skip_if_exists: bool = False,
                              api_key: str = None) -> FireflyResponse:
        """
        Creates a datasource from a pandas DataFrame.

        Args:
            df (pandas.DataFrame): DataFrame object to upload to the server.
            data_source_name (str): Name of the datasource.
            wait (Optional[bool]): Should call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if datasource with same name exists and skip if true.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Datasource ID if successful, raises FireflyError otherwise.
        """
        data_source_name = data_source_name if data_source_name.endswith('.csv') else data_source_name + ".csv"
        existing_ds = cls.list(filter_={'name': [data_source_name]}, api_key=api_key)
        if existing_ds and existing_ds['total'] > 0:
            if skip_if_exists:
                return FireflyResponse(data=existing_ds['hits'][0])
            else:
                raise APIError("Datasource with that name exists")

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        aws_credentials = cls.__get_upload_details(api_key=api_key)
        utils.s3_upload_stream(csv_buffer, data_source_name, aws_credentials)

        return cls._create(data_source_name, wait=wait, api_key=api_key)

    @classmethod
    def _create(cls, datasource_name, wait: bool = False, api_key: str = None):
        data = {
            "name": datasource_name,
            "filename": datasource_name,
            "analyze": True,
            "na_values": None}
        requestor = APIRequestor()
        response = requestor.post(url=cls.CLASS_PREFIX, body=data, api_key=api_key)

        if wait:
            id = response['id']
            utils.wait_for_finite_state(cls.get, id, api_key=api_key)
            response = cls.get(id, api_key=api_key)

        return response

    @classmethod
    def get_base_types(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get base types of the features of a specific datasource.

        Args:
            id (int): Datasource ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Containing a mapping of feature name to a base type.
        """
        requestor = APIRequestor()
        url = '{prefix}/{id}/data_types/base'.format(prefix=cls.CLASS_PREFIX, id=id)
        response = requestor.get(url, api_key=api_key)
        return response

    @classmethod
    def get_feature_types(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get feature types of the features of a specific datasource.

        Args:
            id (int): Datasource ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Containing a mapping of feature name to a feature type.
        """
        requestor = APIRequestor()
        url = '{prefix}/{id}/data_types/feature'.format(prefix=cls.CLASS_PREFIX, id=id)
        response = requestor.get(url, api_key=api_key)
        return response

    @classmethod
    def get_type_warnings(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get type warning for the features of a specific datasource.

        Args:
            id (int): Datasource ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Containing a mapping of feature name to a list of warning (can be empty).
        """
        requestor = APIRequestor()
        url = '{prefix}/{id}/data_types/warning'.format(prefix=cls.CLASS_PREFIX, id=id)
        response = requestor.get(url, api_key=api_key)
        return response

    @classmethod
    def __get_upload_details(cls, api_key: str = None):
        requestor = APIRequestor()
        url = "{prefix}/upload/details".format(prefix=cls.CLASS_PREFIX)
        response = requestor.post(url=url, api_key=api_key)
        return response
