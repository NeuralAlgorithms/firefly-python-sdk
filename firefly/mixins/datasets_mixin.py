import requests

from firefly.errors import *


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
                     not_used=None, hidden=False):
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
        return self.post(query=api, data=data, params={'jwt': self.token}, query_prefix='datasets')

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

    def add_data_resource(self, dataset_name, filename, analyze=True, na_values=None):
        DeprecationWarning(
            'please use the create function instead. this function will be removed in future versions')
        return self.create(jwt=self.token, name=dataset_name, filename=filename, analyze=analyze, na_values=na_values,
                           query_prefix='datasources')

    def create(self, name, filename, analyze=True, na_values=None):
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
