import firefly

ffclient = firefly.Client(username='gilad@firefly.ai', password='Neur@lg0')

# print(ffclient.list_datasets())
# print(ffclient.list_datasources())
# 'datasource_id': 3221, 'id': 4232,

print(ffclient.get_feature_roles(dataset_id=4232))
