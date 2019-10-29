import firefly

ffclient = firefly.Client(username='gilad@firefly.ai', password='')

# hal mixin
ffclient.list_tasks()
# ffclient.rerun_task(2631)
# print(ffclient.get_task_record(2631))
# print(ffclient.get_task_progress(2631))
# DS mixin
# print(ffclient.list_datasets())
# print(ffclient.list_datasources())
# 'datasource_id': 3221, 'id': 4232,
#
# dataset = 4232
# datasource = 3221
# print(ffclient.get_feature_roles(dataset_id=4232))
#
# print(ffclient.get_dataset(dataset))
#
# print(ffclient.get_datasource(datasource))
#
# print(ffclient.get_download_details())
#
# print(ffclient.get_data_head(datasource, 10))
#
# print(ffclient.get_dataset_types(dataset))
#
# print(ffclient.get_datasource_types(datasource))
#
# print(ffclient.get_base_types(datasource))
#
# print(ffclient.get_feature_types(datasource))
#
# print(ffclient.get_type_warnings(datasource))
#
# print(ffclient.get_metadata(datasource))
#
# print(ffclient.get_features_description(datasource))
#
# print(ffclient.get_feature_roles(dataset))
#
# print(ffclient.get_transformations(dataset))
#
# print(ffclient.get_upload_details())
#
# print(ffclient.get_system_default_na_values())
#
# print(ffclient.get_na_values(datasource))
