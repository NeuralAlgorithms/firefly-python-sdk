import io
import os
import time

import boto3
import pandas as pd
import requests

from firefly.errors import *

FINITE_STATES = ['AVAILABLE', 'CREATED', 'CANCELED', 'FAILED']


class DatasetsMixin(abc.ABC):

    def delete_datasource(self, data_id):
        api = '{data_id}'
        return self.delete(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def delete_dataset(self, data_id):
        api = '{data_id}'
        return self.delete(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasets')

    def list_datasources(self, search_all_columns=None, page=None, page_size=None, sort=None,
                         filter=None):
        return self.__list_data_resources(resource_type='datasources', search_all_columns=search_all_columns,
                                          page=page, page_size=page_size, sort=sort, filter=filter)

    def list_datasets(self, search_all_columns=None, page=None, page_size=None, sort=None,
                      filter=None):
        return self.__list_data_resources(resource_type='datasets', search_all_columns=search_all_columns,
                                          page=page, page_size=page_size, sort=sort, filter=filter)

    def get_dataset(self, data_id):
        api = '{data_id}'
        return self.get(api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasets')

    def get_datasource(self, data_id):
        api = '{data_id}'
        return self.get(api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_download_details(self):
        api = 'download/details'
        return self.get(api, query_prefix='', params={'jwt': self.token})

    def get_data_head(self, data_id, rows):
        api = '{data_id}/head'
        return self.get(query=api.format(data_id=data_id, rows=rows), query_prefix='datasources',
                        params={'jwt': self.token, 'rows': rows})

    def get_dataset_types(self, data_id):
        api = '{data_id}/data_types'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasets')

    def get_datasource_types(self, data_id):
        api = '{data_id}/data_types'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_base_types(self, data_id):
        api = '{data_id}/data_types/base'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_feature_types(self, data_id):
        api = '{data_id}/data_types/feature'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_type_warnings(self, data_id):
        api = '{data_id}/data_types/warnings'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_metadata(self, data_id):
        api = '{data_id}/meta'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def get_features_description(self, data_id):
        api = '{data_id}/features_description'
        return self.get(query=api.format(data_id=data_id), params={'jwt': self.token}, query_prefix='datasources')

    def __list_data_resources(self, resource_type, search_all_columns=None, page=None, page_size=None, sort=None,
                              filter=None):
        api = ''
        filters = self.parse_filter_parameters(filter)
        sorts = self.parse_sort_parameters(sort)

        params = {'search_all_columns': search_all_columns,
                  'page': page, 'page_size': page_size,
                  'sort': sorts, 'filter': filters,
                  'jwt': self.token
                  }
        return self.get(query=api, params=params, query_prefix=resource_type)

    def get_feature_roles(self, dataset_id):
        api = '{dataset_id}/meta'
        return self.get(query=api.format(dataset_id=dataset_id), params={'jwt': self.token}, query_prefix='datasets')[
            'feature_roles']

    def get_transformations(self, dataset_id):
        api = '{data_id}/meta'
        return self.get(query=api.format(data_id=dataset_id), params={'jwt': self.token}, query_prefix='datasets')[
            'transformations']

    def prepare_data(self, data_id, dataset_name, problem_type='classification', header=False,
                     na_values=None, retype_columns=None, rename_columns=None, datetime_format=None, target=None,
                     time_axis=None, block_id=None, sample_id=None, subdataset_id=None, sample_weight=None,
                     not_used=None, hidden=False, wait=False):
        api = ''
        data = {
            "name": dataset_name,
            "data_id": data_id,
            "header": header,
            "problem_type": problem_type,
            "hidden": hidden,
            "na_values": na_values,
            "retype_columns": retype_columns,
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

    def copy_data_preparation(self, prepared_id, raw_id, job='refit', callback_payload=None, predict_params=None):
        api = '{prepared_id}/{raw_id}/{job}'
        data = {'callback_payload': callback_payload, 'predict_params': predict_params}
        return self.post(query=api.format(prepared_id=prepared_id, raw_id=raw_id, job=job),
                         data=data, params={'jwt': self.token}, query_prefix='datasets')

    def reprepare(self, dataset_id):
        api = 'dev/datasets/{dataset_id}/prepare'
        return self.post(query=api.format(dataset_id=dataset_id),
                         params={'jwt': self.token}, query_prefix='')

    def get_upload_details(self):
        api = 'upload/details'
        return self.post(query=api, params={'jwt': self.token}, query_prefix='datasources')

    def create_datasource(self, name, filename, analyze=True, na_values=None):
        api = ''
        data = {
            "name": name,
            "filename": filename,
            "analyze": analyze,
            "na_values": na_values}
        return self.post(query=api, data=data, params={'jwt': self.token}, query_prefix='datasources')

    def create_from_datasource(self, datasource_id, analyze=True, na_values=None):
        api = '{datasource_id}'
        data = {
            "na_values": na_values,
            "analyze": analyze
        }
        return self.post(query=api.format(datasource_id=datasource_id), data=data, params={'jwt': self.token},
                         query_prefix='datasources')

    def analyze(self, datasource_id, na_values=None):
        api = '{datasource_id}/analyze'
        data = {
            "na_values": na_values
        }
        return self.post(query=api.format(datasource_id=datasource_id), data=data, params={'jwt': self.token},
                         query_prefix='datasources')

    def get_system_default_na_values(self):
        api = 'na_values'
        return self.get(query=api, params={'jwt': self.token}, query_prefix='datasources')

    def get_na_values(self, datasource_id):
        api = '{datasource_id}/metadata/na_values'
        return self.get(query=api.format(datasource_id=datasource_id), params={'jwt': self.token},
                        query_prefix='datasources')

    def upload(self, filename, wait=False, override=False):
        dataset = os.path.basename(filename)
        upload_details = self.get_upload_details()

        s3c = boto3.client('s3', region_name=upload_details['region'],
                           aws_access_key_id=upload_details['access_key'],
                           aws_secret_access_key=upload_details['secret_key'],
                           aws_session_token=upload_details['session_token'])

        s3c.upload_file(filename, upload_details['bucket'], os.path.join(upload_details['path'], dataset))

        try:
            id = self.create(name=dataset, filename=dataset, analyze=True)
        except FireflyClientError as e:
            pass #TODO
        if wait:
            self.__wait_for_finite_state(id, self.get_datasource)
        return id

    def upload_df(self, df, data_source_name, wait=False, override=False):

        upload_details = self.get_upload_details()

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        session = boto3.Session(
            region_name=upload_details['region'],
            aws_access_key_id=upload_details['access_key'],
            aws_secret_access_key=upload_details['secret_key'],
            aws_session_token=upload_details['session_token'])

        s3_resource = session.resource('s3')

        filename = data_source_name if data_source_name.endswith('.csv') else data_source_name + ".csv"
        s3_resource.Bucket(upload_details['bucket']).put_object(
            Key=upload_details['path'] + '/' + filename
            ,
            Body=csv_buffer.getvalue()
        )
        id = self.create(name=filename, filename=filename, analyze=True)
        if wait:
            self.__wait_for_finite_state(id, self.get_datasource)
        return id

    def __wait_for_finite_state(self, data_id, getter):
        res = getter(data_id)
        state = res['state']
        while (state not in FINITE_STATES):
            time.sleep(5)
            res = getter(data_id)
            state = res['state']
