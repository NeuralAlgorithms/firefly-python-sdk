import io
import os
import time
from typing import Dict, Union, List

import boto3
import pandas as pd
import requests

from firefly.errors import *
from firefly.enums import *

FINITE_STATES = ['AVAILABLE', 'CREATED', 'CANCELED', 'FAILED']


class DatasetsMixin(abc.ABC):

    def delete_datasource(self, data_id: int) -> str:
        """
        Delete a specific datasource.

        Args:
            data_id (int): Datasource ID.

        Returns:
            "true" if deleted successfuly, raises FireflyClientError otherwise.
        """
        api = '{data_id}'
        return self.delete(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def delete_dataset(self, data_id: int) -> str:
        """
        Delete a specific dataset.

        Args:
            data_id (int): Dataset ID.

        Returns:
            "true" if deleted successfuly, raises FireflyClientError otherwise.
        """
        api = '{data_id}'
        return self.delete(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasets')

    def list_datasources(self, search_term: str = None, page: int = None, page_size: int = None,
                         sort: Dict[str, Union[str, int]] = None, filter: Dict[str, Union[str, int]] = None) -> Dict:
        """
        List the existing datasources. Supports filtering, sorting and pagination.

        Args:
            search_term (Optional[str]): Return only records that contain the search_term in one of their fields.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to sort the results by.
            filter (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.

        Returns:
            Dictionary containing the datasources, which are represented as nested dictionaries.
        """
        return self.__list_data_resources(resource_type='datasources', search_term=search_term,
                                          page=page, page_size=page_size, sort=sort, filter=filter)

    def list_datasets(self, search_term: str = None, page: int = None, page_size: int = None,
                      sort: Dict[str, Union[str, int]] = None, filter: Dict[str, Union[str, int]] = None) -> Dict:
        """
        List the existing datasets. Supports filtering, sorting and pagination.

        Args:
            search_term (Optional[str]): Return only records that contain the search_term in one of their fields.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to sort the results by.
            filter (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.

        Returns:
            Dictionary containing the datasets, which are represented as nested dictionaries.
        """
        return self.__list_data_resources(resource_type='datasets', search_term=search_term,
                                          page=page, page_size=page_size, sort=sort, filter=filter)

    def get_dataset(self, data_id: int) -> Dict:
        """
        Get information on a specific dataset.

        Information includes the state of the dataset, and other basic attributes.

        Args:
            data_id (int): Dataset ID.

        Returns:
            Dictionary containing the information about the dataset.
        """
        api = '{data_id}'
        return self.get(api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasets')

    def get_datasource(self, data_id: int) -> Dict:
        """
        Get information on a specific datasource.

        Information includes the state of the datasource, and other basic attributes.

        Args:
            data_id (int): Datasource ID.

        Returns:
            Dictionary containing the information about the datasource.
        """
        api = '{data_id}'
        return self.get(api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_data_head(self, data_id: int, rows: int) -> List[List[str]]:
        """
        View the first few rows of a datasource.

        Returns a table-view of the first `rows` lines, represented as a list of lists.

        Args:
            data_id (int): Datasource ID.
            rows (int): Number of rows to fetch from the datasource.

        Returns:
            List of lists, containing the first `rows` lines of the data.
        """
        api = '{data_id}/head'
        return self.get(query=api.format(data_id=data_id, rows=rows), query_prefix='datasources',
                        params={'jwt': self.token, 'rows': rows})

    def get_dataset_types(self, data_id: int) -> Dict:
        """
        Get details of the deatures of the dataset,

        Details incldue the base type (e.g 'str'), feature type (e.g 'categorical') and warning given by the system.

        Args:
            data_id (int): Dataset ID.

        Returns:
            Dictionary containing details for every feature of the dataset.
        """
        api = '{data_id}/data_types'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasets')

    def get_datasource_types(self, data_id: int) -> Dict:
        """
        Get details of the deatures of the datasource,

        Details incldue the base type (e.g 'str'), feature type (e.g 'categorical') and warning given by the system.

        Args:
            data_id (int): Datasource ID.

        Returns:
            Dictionary containing details for every feature of the datasource.
        """
        api = '{data_id}/data_types'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_base_types(self, data_id: int) -> Dict:
        """
        Get base types of the features of a specific datasource.

        Args:
            data_id (int): Datasource ID.

        Returns:
            Dictionary containing a mapping of feature name to a base type.
        """
        api = '{data_id}/data_types/base'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_feature_types(self, data_id: int) -> Dict:
        """
        Get feature types of the features of a specific datasource.

        Args:
            data_id (int): Datasource ID.

        Returns:
            Dictionary containing a mapping of feature name to a feature type.
        """
        api = '{data_id}/data_types/feature'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_type_warnings(self, data_id: int) -> Dict:
        """
        Get type warning for the features of a specific datasource.

        Args:
            data_id (int): Datasource ID.

        Returns:
            Dictionary containing a mapping of feature name to a list of warning (can be empty).
        """
        api = '{data_id}/data_types/warnings'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_metadata(self, data_id: int) -> Dict:
        """
        Get full metadata of a specific datasource.

        Args:
            data_id (int): datasource ID.

        Returns:
            Dictionary containing full metadata for the specified datasource.
        """
        api = '{data_id}/meta'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_features_description(self, data_id: int) -> Dict:
        """
        Get analysis of all features in a given datasource.

        Details for a feature include: count, frequency, unique values, etc.

        Args:
            data_id (int): datasource ID.

        Returns:
            Dictionary containing analysis information about every feature in the datasource.
        """
        api = '{data_id}/features_description'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_datasources_by_name(self, datasource_name: str) -> Dict:
        """
        Gets details of a datasource by its name.

        Calls `list_datasources` with filter on field `name`.

        Args:
            datasource_name (int): Datasource ID.

        Returns:
            Dictionary containing details of datasource.
        """
        ds = self.list_datasources(filter={'name': [datasource_name]})
        return ds['hits']

    def get_datasets_by_name(self, dataset_name: str) -> Dict:
        """
        Gets details of a dataset by its name.

        Calls `list_datasets` with filter on field `name`.

        Args:
            dataset_name (int): Dataset ID.

        Returns:
            Dictionary containing details of dataset.
        """
        ds = self.list_datasets(filter={'name': [dataset_name]})
        return ds['hits']

    def __list_data_resources(self, resource_type, search_term=None, page=None, page_size=None, sort=None,
                              filter=None):
        api = ''
        filters = self.parse_filter_parameters(filter)
        sorts = self.parse_sort_parameters(sort)

        params = {'search_term': search_term,
                  'page': page, 'page_size': page_size,
                  'sort': sorts, 'filter': filters,
                  'jwt': self.token
                  }
        return self.get(query=api, params=params, query_prefix=resource_type)

    def get_feature_roles(self, dataset_id: int) -> Dict:
        """
        Get roles of the features of a specific dataset.

        Args:
            data_id (int): Dataset ID.

        Returns:
            Dictionary containing a mapping of feature name to a its roles in the dataset.
        """
        api = '{dataset_id}/meta'
        return self.get(query=api.format(dataset_id=dataset_id), params={'jwt': self.token}, query_prefix='datasets')[
            'feature_roles']

    def get_transformations(self, dataset_id: int) -> Dict:
        """
        Get all transformations that are applies to the dataset.

        Args:
            dataset_id (int): Dataset ID.

        Returns:
            Dictionary with a mapping between transformation name (e.g 'retype_columns') to a list of feature names
            the transformation is applied to.
        """
        api = '{data_id}/meta'
        return self.get(query=api.format(data_id=dataset_id), params={'jwt': self.token}, query_prefix='datasets')[
            'transformations']

    # TODO: fill in arguments' descriptions
    def prepare_data(self, data_id: int, dataset_name: str, target: str, problem_type: ProblemType, header: bool,
                     na_values: List[str] = None, retype_columns: Dict[str, FeatureType] = None,
                     rename_columns: List[str] = None, datetime_format: str = None, time_axis: str = None,
                     block_id: List[str] = None, sample_id: List[str] = None, subdataset_id: List[str] = None,
                     sample_weight: List[str] = None, not_used: List[str] = None, hidden: List[str] = False,
                     wait: bool = False, skip_if_exists: bool = False) -> str:
        """
        Creates and prepares a dataset.

        When creating a dataset the feature roles are labled and the feature types can be set by the user.
        Data analysis is done in order to optimize the model training and search process.

        Args:
            data_id (int): Datasource ID.
            dataset_name (str): The name of the dataset in the application.
            target (str): The feature name of the target if the header parameter is true, otherwise the column index.
            problem_type (ProblemType): The problem type.
            header (bool): Does to file include a header row or not.
            na_values (Optional[List[str]]): List of na values.
            retype_columns (Dict[str, FeatureType]): Change the chosen type of certain columns.
            rename_columns (Optional[List[str]]): ???
            datetime_format (Optional[str]): The date time format used in the data.
            time_axis (Optional[str]): In timeseries, the feature that is the time axis.
            block_id (Optional[List[str]]): To avoid data leakage, data can be splitted to blocks. Rows with the same
                block id must be all in the train set or the test set. Requires to have at least 50 unique values.
            sample_id (Optional[List[str]]): Row identifier.
            subdataset_id (Optional[List[str]]): Features which specify a subdataset ID in the data.
            sample_weight (Optional[List[str]]): ???
            not_used (Optional[List[str]]): List of features to ignore.
            hidden (Optional[List[str]]): ???
            wait (Optional[bool]): Should call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if dataset with same name exists and skip if true.

        Returns:
            str: ID of the created dataset.
        """
        ids = self.get_datasets_by_name(dataset_name)
        if ids:
            if skip_if_exists:
                return ids[0]['id']
            else:
                raise FireflyClientError("dataset with that name exists")

        api = ''
        data = {
            "name": dataset_name,
            "data_id": data_id,
            "header": header,
            "problem_type": problem_type.value if problem_type is not None else None,
            "hidden": hidden,
            "na_values": na_values,
            "retype_columns": {key: retype_columns[key].value for key in
                               retype_columns} if retype_columns is not None else None,
            "datetime_format": datetime_format,
            "target": target,
            "time_axis": time_axis,
            "block_id": block_id,
            "sample_id": sample_id,
            "subdataset_id": subdataset_id,
            "sample_weight": sample_weight,
            "not_used": not_used,
            "rename_columns": rename_columns
        }
        id = self.post(query=api, data=data, params={'jwt': self.token}, query_prefix='datasets')
        if wait:
            self.__wait_for_finite_state(data_id=id, getter=self.get_dataset)
        return id

    # TODO: what is this?
    def copy_data_preparation(self, prepared_id, raw_id, job='refit', callback_payload=None, predict_params=None):
        api = '{prepared_id}/{raw_id}/{job}'
        data = {'callback_payload': callback_payload, 'predict_params': predict_params}
        return self.post(query=api.format(prepared_id=prepared_id, raw_id=raw_id, job=job),
                         data=data, params={'jwt': self.token}, query_prefix='datasets')

    # TODO: under '/dev/' prefix, should be here?
    def reprepare(self, dataset_id):
        api = 'dev/datasets/{dataset_id}/prepare'
        return self.post(query=api.format(dataset_id=dataset_id),
                         params={'jwt': self.token}, query_prefix='')

    # TODO: return access key and secret for AWS, should be here?s
    def get_upload_details(self):
        api = 'upload/details'
        return self.post(query=api, params={'jwt': self.token}, query_prefix='datasources')

    # TODO: Fill in missing param desc
    def create_datasource(self, name: str, filename: str, analyze: bool = True, na_values: List[str] = None) -> int:
        """
        Create and analyze a datasource

        Upload data to become a datasource. The data is analyzed to gather insights and determine the possible data
        types of each feature (column). Our possible feature types are categorical, numeric and datetime.
        A feature can be numeric if all it's values are numeric or null.

        Args:
            name (str): A unique name to be given to the datasource.
            filename (str): File name to upload to the server.
            analyze (Optional[bool]): ???
            na_values (Optional[List[str]]): List of values to regard as NULL when analyzing the datasource.

        Returns:
            int: Datasource ID.
        """
        api = ''
        data = {
            "name": name,
            "filename": filename,
            "analyze": analyze,
            "na_values": na_values}
        return self.post(query=api, data=data, params={'jwt': self.token}, query_prefix='datasources')

    def create_from_datasource(self, datasource_id: int, analyze: bool = True, na_values: List[str] = None) -> int:
        """
        Create and analyze a datasource that already exists in the application.

        Args:
            datasource_id (int): Datasource ID.
            analyze (Optional[bool]): ???
            na_values (Optional[List[str]]): List of values to regard as NULL when analyzing the datasource.

        Returns:
            int: Datasource ID.
        """
        api = '{datasource_id}'
        data = {
            "na_values": na_values,
            "analyze": analyze
        }
        return self.post(query=api.format(datasource_id=datasource_id), data=data, params={'jwt': self.token},
                         query_prefix='datasources')

    # TODO: fill in
    def analyze(self, datasource_id: int, na_values: List[str] = None):
        """
        ???

        Args:
            datasource_id (int): Datasource ID.
            na_values (Optional[List[str]]): List of values to regard as NULL when analyzing the datasource.

        Returns:
            ???
        """
        api = '{datasource_id}/analyze'
        data = {
            "na_values": na_values
        }
        return self.post(query=api.format(datasource_id=datasource_id), data=data, params={'jwt': self.token},
                         query_prefix='datasources')

    def get_system_default_na_values(self) -> List[str]:
        """
        Get a list of default NULL values.

        Returns:
            List[str]: Default NULL values.
        """
        api = 'na_values'
        return self.get(query=api, params={'jwt': self.token}, query_prefix='datasources')

    def get_na_values(self, datasource_id: int) -> List[str]:
        """
        Get NULL values for a specific datasource.

        Args:
            datasource_id (int): Datasource ID.

        Returns:
            List[str]: NULL values of a specific datasource.
        """
        api = '{datasource_id}/metadata/na_values'
        return self.get(query=api.format(datasource_id=datasource_id), params={'jwt': self.token},
                        query_prefix='datasources')

    def upload(self, filename: str, wait: bool = False, skip_if_exists: bool = False) -> int:
        """
        Uploads a file to the server and creates a datasource.

        Args:
            filename (str): File to be uploaded.
            wait (Optional[bool]): Should call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if datasource with same name exists and skip if true.

        Returns:
            int: Datasource ID if successful, raises FireflyClientError otherwise.
        """
        dataset = os.path.basename(filename)

        ids = self.get_datasources_by_name(dataset)
        if ids:
            if skip_if_exists:
                return ids[0]['id']
            else:
                raise FireflyClientError("datasource with that name exists")

        self.__s3_upload(dataset, filename)

        id = self.create_datasource(name=dataset, filename=dataset, analyze=True)

        if wait:
            self.__wait_for_finite_state(id, self.get_datasource)
        return id

    def upload_df(self, df, data_source_name: str, wait: bool = False, skip_if_exists: bool = False) -> int:
        """
        Creates a datasource from a pandas DataFrame.

        Args:
            df (pandas.DataFrame): DataFrame object to upload to the server.
            data_source_name (str): Name of the datasource.
            wait (Optional[bool]): Should call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if datasource with same name exists and skip if true.

        Returns:
            int: Datasource ID.
        """

        filename = data_source_name if data_source_name.endswith('.csv') else data_source_name + ".csv"
        ids = self.get_datasources_by_name(filename)
        if ids:
            if skip_if_exists:
                return ids[0]['id']
            else:
                raise FireflyClientError("datasource with that name exists")

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        self.__s3_upload_stream(csv_buffer, filename)

        id = self.create_datasource(name=filename, filename=filename, analyze=True)

        if wait:
            self.__wait_for_finite_state(id, self.get_datasource)
        return id

    def __s3_upload(self, dataset, filename):
        upload_details = self.get_upload_details()
        s3c = boto3.client('s3', region_name=upload_details['region'],
                           aws_access_key_id=upload_details['access_key'],
                           aws_secret_access_key=upload_details['secret_key'],
                           aws_session_token=upload_details['session_token'])
        s3c.upload_file(filename, upload_details['bucket'], os.path.join(upload_details['path'], dataset))

    def __s3_upload_stream(self, csv_buffer, filename):
        upload_details = self.get_upload_details()
        session = boto3.Session(
            region_name=upload_details['region'],
            aws_access_key_id=upload_details['access_key'],
            aws_secret_access_key=upload_details['secret_key'],
            aws_session_token=upload_details['session_token'])
        s3_resource = session.resource('s3')
        s3_resource.Bucket(upload_details['bucket']).put_object(
            Key=upload_details['path'] + '/' + filename
            ,
            Body=csv_buffer.getvalue()
        )

    def __wait_for_finite_state(self, data_id, getter):
        res = getter(data_id)
        state = res['state']
        while state not in FINITE_STATES:
            time.sleep(5)
            res = getter(data_id)
            state = res['state']
