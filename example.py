import pandas as pd
import firefly

ffclient = firefly.Client(username='gilad@firefly.ai', password='Neur@lg0')


def test_upload(wait=True, overwrite=True):
    try:
        datasource = ffclient.upload('./script/test2.csv', wait, overwrite)
    except Exception as e:
        datasource = -1
        print(e)
    print(datasource)


def test_upload_df(wait=True, overwrite=True):
    df = pd.read_csv('./script/test2.csv')
    try:
        datasource = ffclient.upload_df(df, 'test3', wait, overwrite)
    except Exception as e:
        datasource = -1
        print(e)
    print(datasource)


test_upload_df(True, True)
test_upload_df(True, False)

test_upload_df(True, True)
test_upload_df(False, True)
test_upload_df(False, False)

#
# test_upload(True, True)
# test_upload(False, False)
# test_upload(False, True)
# print(ffclient.list_datasources())


# hal mixin
# ffclient.list_tasks()
# ffclient.rerun_task(2631)
# print(ffclient.get_task_record(2631))
# print(ffclient.get_task_progress(2631))
# # DS mixin
# print(ffclient.list_datasets())
print(ffclient.list_datasources())

# # 'datasource_id': 3221, 'id': 4232,
# #
# dataset = 4232

# print(ffclient.get_feature_roles(dataset_id=4232))
#
# print(ffclient.get_dataset(dataset))
#
# print(ffclient.get_datasource(datasource))
# #
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
#
