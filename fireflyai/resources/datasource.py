"""
While working with Firefly.ai's API, a Datasource represents the raw CSV files. This data  can be used either for model
training purposes or for running batch predictions once they have been analyzed.

‘Datasource’ API includes creating a Datasource from an uploaded CSV file, querying existing Datasources
(Get, List, Preview and Delete) and getting Datasource metadata (e.g. feature types and type insights).
"""

import io
import os
from typing import Dict, List

import fireflyai
from fireflyai import utils
from fireflyai.api_requestor import APIRequestor
from fireflyai.enums import FeatureType, ProblemType
from fireflyai.errors import APIError, InvalidRequestError
from fireflyai.firefly_response import FireflyResponse
from fireflyai.resources.api_resource import APIResource


class Datasource(APIResource):
    _CLASS_PREFIX = 'datasources'

    @classmethod
    def list(cls, search_term: str = None, page: int = None, page_size: int = None, sort: Dict = None,
             filter_: Dict = None, api_key: str = None) -> FireflyResponse:
        """
        Lists the existing Datasources - supports filtering, sorting and pagination.

        Args:
            search_term (Optional[str]): Return only records that contain the `search_term` in any field.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules  to sort the results by.
            filter_ (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: Datasources are represented as nested dictionaries under `hits`.
        """
        return cls._list(search_term, page, page_size, sort, filter_, api_key)

    @classmethod
    def get(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Gets information on a specific Datasource.

        Information includes the state of the Datasource and other attributes.

        Args:
            id (int): Datasource ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Information about the Datasource.
        """
        return cls._get(id, api_key)

    @classmethod
    def get_by_name(cls, name: str, api_key: str = None) -> FireflyResponse:
        """
        Gets information on a specific Datasource identified by its name.

        Information includes the state of the Datasource and other attributes.
        Similar to calling `fireflyai.Datasource.list(filters_={'name': [NAME]})`.

        Args:
            name (str): Datasource name.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Information about the Datasource.
        """
        resp = cls.list(filter_={'name': [name]}, api_key=api_key)
        if resp and 'total' in resp and resp['total'] > 0:
            ds = resp['hits'][0]
            return FireflyResponse(data=ds)
        else:
            raise APIError("Datasource with that name does not exist")

    @classmethod
    def delete(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Deletes a specific Datasource.

        Args:
            id (int): Datasource ID.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: "true" if deleted successfuly, raises FireflyClientError otherwise.
        """
        return cls._delete(id, api_key)

    @classmethod
    def create(cls, filename: str, na_values: List[str] = None, wait: bool = False, skip_if_exists: bool = False,
               api_key: str = None) -> FireflyResponse:
        """
        Uploads a file to the server to creates a new Datasource.

        Args:
            filename (str): File to be uploaded.
            na_values (Optional[List[str]]): List of user specific Null values.
            wait (Optional[bool]): Should the call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if a Datasource with same name exists and skip if true.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Datasource ID, if successful and wait=False or Datasource if successful and wait=True;
            raises FireflyError otherwise.
        """
        data_source_name = os.path.basename(filename)

        existing_ds = cls.list(filter_={'name': [data_source_name]}, api_key=api_key)
        if existing_ds and existing_ds['total'] > 0:
            if skip_if_exists:
                return FireflyResponse(data=existing_ds['hits'][0])
            else:
                raise InvalidRequestError("Datasource with that name already exists")

        aws_credentials = cls.__get_upload_details(api_key=api_key)
        utils.s3_upload(data_source_name, filename, aws_credentials.to_dict())

        return cls._create(data_source_name, na_values=na_values, wait=wait, api_key=api_key)

    @classmethod
    def create_from_dataframe(cls, df, data_source_name: str, na_values: List[str] = None, wait: bool = False,
                              skip_if_exists: bool = False, api_key: str = None) -> FireflyResponse:
        """
        Creates a Datasource from pandas DataFrame.

        Args:
            df (pandas.DataFrame): DataFrame object to upload to server.
            data_source_name (str): Name of the Datasource.
            na_values (Optional[List[str]]): List of user specific Null values.
            wait (Optional[bool]): Should the call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if a Datasource with same name exists and skip if true.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: Datasource ID, if successful and wait=False or Datasource if successful and wait=True;
            raises FireflyError otherwise.
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

        return cls._create(data_source_name, na_values=na_values, wait=wait, api_key=api_key)

    @classmethod
    def _create(cls, datasource_name, na_values: List[str] = None, wait: bool = False, api_key: str = None):
        data = {
            "name": datasource_name,
            "filename": datasource_name,
            "analyze": True,
            "na_values": na_values}
        requestor = APIRequestor()
        response = requestor.post(url=cls._CLASS_PREFIX, body=data, api_key=api_key)

        if wait:
            id = response['id']
            utils.wait_for_finite_state(cls.get, id, api_key=api_key)
            response = cls.get(id, api_key=api_key)

        return response

    @classmethod
    def get_base_types(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Gets base types of features for a specific Datasource.

        Args:
            id (int): Datasource ID.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: Contains mapping of feature names to base types.
        """
        requestor = APIRequestor()
        url = '{prefix}/{id}/data_types/base'.format(prefix=cls._CLASS_PREFIX, id=id)
        response = requestor.get(url, api_key=api_key)
        return response

    @classmethod
    def get_feature_types(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Gets feature types of features for a specific Datasource.

        Args:
            id (int): Datasource ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Contains mapping of feature names to feature types.
        """
        requestor = APIRequestor()
        url = '{prefix}/{id}/data_types/feature'.format(prefix=cls._CLASS_PREFIX, id=id)
        response = requestor.get(url, api_key=api_key)
        return response

    @classmethod
    def get_type_warnings(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Gets type warning of features for a specific Datasource.

        Args:
            id (int): Datasource ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Contains mapping of feature names to a list of type warnings (can be empty).
        """
        requestor = APIRequestor()
        url = '{prefix}/{id}/data_types/warning'.format(prefix=cls._CLASS_PREFIX, id=id)
        response = requestor.get(url, api_key=api_key)
        return response

    @classmethod
    def prepare_data(cls, datasource_id: int, dataset_name: str, target: str, problem_type: ProblemType,
                     header: bool = True, na_values: List[str] = None, retype_columns: Dict[str, FeatureType] = None,
                     rename_columns: List[str] = None, datetime_format: str = None, time_axis: str = None,
                     block_id: List[str] = None, sample_id: List[str] = None, subdataset_id: List[str] = None,
                     sample_weight: List[str] = None, not_used: List[str] = None, hidden: List[str] = False,
                     wait: bool = False, skip_if_exists: bool = False, api_key: str = None) -> FireflyResponse:
        """
        Creates and prepares a Dataset.

        While creating a Dataset, the feature roles are labeled and the feature types can be set by the user.
        Data analysis is done in order to optimize model training and search process.

        Args:
            datasource_id (int): Datasource ID.
            dataset_name (str): The name of the Dataset.
            target (str): The feature name of the target if header=True, otherwise the column index. #TODO
            problem_type (ProblemType): The problem type.
            header (bool): Does the file include a header row or not.
            na_values (Optional[List[str]]): List of user specific Null values.
            retype_columns (Dict[str, FeatureType]): Change the types of certain columns.
            rename_columns (Optional[List[str]]): ??? #TODO
            datetime_format (Optional[str]): The datetime format used in the data.
            time_axis (Optional[str]): In timeseries problems, the feature that is the time axis.
            block_id (Optional[List[str]]): To avoid data leakage, data can be split into blocks. Rows with the same
                `block_id`, must all be in the train set or the test set. Requires at least 50 unique values in the data.
            sample_id (Optional[List[str]]): Row identifier.
            subdataset_id (Optional[List[str]]): Features which specify a subdataset ID in the data.
            sample_weight (Optional[List[str]]): ??? #TODO
            not_used (Optional[List[str]]): List of features to ignore.
            hidden (Optional[List[str]]): ??? #TODO
            wait (Optional[bool]): Should the call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if a Dataset with same name exists and skip if true.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: Dataset ID, if successful and wait=False or Dataset if successful and wait=True;
            raises FireflyError otherwise.
        """
        return fireflyai.Dataset.create(datasource_id=datasource_id, dataset_name=dataset_name, target=target,
                                        problem_type=problem_type, header=header, na_values=na_values,
                                        retype_columns=retype_columns, rename_columns=rename_columns,
                                        datetime_format=datetime_format, time_axis=time_axis, block_id=block_id,
                                        sample_id=sample_id, subdataset_id=subdataset_id, sample_weight=sample_weight,
                                        not_used=not_used, hidden=hidden, wait=wait, skip_if_exists=skip_if_exists,
                                        api_key=api_key)

    @classmethod
    def __get_upload_details(cls, api_key: str = None):
        requestor = APIRequestor()
        url = "{prefix}/upload/details".format(prefix=cls._CLASS_PREFIX)
        response = requestor.post(url=url, api_key=api_key)
        return response
