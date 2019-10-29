from firefly.errors import *


class PredictionsMixin(abc.ABC):

    def predict(self, ensemble_id, data_id):
        api = ''
        data = {
            "ensemble_id": ensemble_id,
            "datasource_id": data_id,
        }
        return self.post(api.format(ensemble_id=ensemble_id), data=data, query_prefix='predictions',
                         params={'jwt': self.token})

    def get_predict_record(self, pred_id):
        api = '{pred_id}'
        return self.get(api.format(pred_id=pred_id), query_prefix='predictions', params={'jwt': self.token})

    def delete_prediction(self, pred_id):
        api = '{pred_id}'
        return self.delete(api.format(pred_id=pred_id), query_prefix='predictions', params={'jwt': self.token})


    def list_predictions(self, search_all_columns=None, page=None, page_size=None, sort=None, filter=None):
        api = ''
        filters = self.parse_filter_parameters(filter)
        sorts = self.parse_sort_parameters(sort)
        params = {'search_all_columns': search_all_columns, 'page': page, 'page_size': page_size,
                  'sort': sorts, 'filter': filters, 'jwt': self.token}
        pred_id = self.get(api, params=params, query_prefix='predictions')
        return pred_id

