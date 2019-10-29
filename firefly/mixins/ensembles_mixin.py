import time
from collections import OrderedDict

from firefly.errors import *


class EnsemblesMixin(abc.ABC):

    def get_model_sensitivity_report(self, ensemble_id):
        api = '{ensemble_id}/sensitivity'
        return self.get(api.format(ensemble_id=ensemble_id), params={'jwt': self.token}, query_prefix='ensembles')

    def get_threshold_endpoints_report(self, ensemble_id):
        api = '{ensemble_id}/threshold_endpoints'
        return self.get(api.format(ensemble_id=ensemble_id), params={'jwt': self.token}, query_prefix='ensembles')

    def list_ensembles(self, page=None, page_size=None, sort=None, filter=None):
        api = ''
        filters = self.parse_filter_parameters(filter)
        sorts = self.parse_sort_parameters(sort)
        return self.get(api,
                        params={'jwt': self.token, 'page': page, 'page_size': page_size, 'sort': sorts,
                                'filter': filters},
                        query_prefix='ensembles')

    def get_ensemble_details(self, ensemble_id):
        api = '{ensemble_id}'
        return self.get(api.format(ensemble_id=ensemble_id), params={'jwt': self.token}, query_prefix='ensembles')

    def edit_ensemble_notes(self, ensemble_id, notes):
        api = '{ensemble_id}/notes'
        notes = {'notes': notes}
        return self.put(api.format(ensemble_id=ensemble_id), data=notes, params={'jwt': self.token},
                        query_prefix='ensembles')

    def delete_ensemble(self, ensemble_id):
        api = '{ensemble_id}'
        return self.delete(query=api.format(ensemble_id=ensemble_id), params={'jwt': self.token},
                           query_prefix='ensembles')

    def calc_model_sensitivity(self, ensemble_id):
        api = '{ensemble_id}/sensitivity'
        return self.post(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})

    def calc_threshold_endpoints(self, ensemble_id):
        api = '{ensemble_id}/threshold_endpoints'
        return self.post(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})

    def get_ensemble_summary_report(self, ensemble_id):
        api = '{ensemble_id}/summary'
        task_id = self.get(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})
        return task_id

    def get_ensemble_roc_curve(self, ensemble_id):
        api = '{ensemble_id}/roc_curve'
        params = {'jwt': self.token}
        performance = self.get(api.format(ensemble_id=ensemble_id), params=params, query_prefix='ensembles')
        return performance

    def get_ensemble_confusion_matrix(self, ensemble_id):
        api = '{ensemble_id}/confusion'
        params = {'jwt': self.token}
        confusion = self.get(api.format(ensemble_id=ensemble_id), params=params, query_prefix='ensembles')
        return confusion

    def get_ensemble_test_prediction_sample(self, ensemble_id):
        api = '{ensemble_id}/test_prediction_sample'
        params = {'jwt': self.token}
        scatter_plot = self.get(api.format(ensemble_id=ensemble_id), params=params, query_prefix='ensembles')
        return scatter_plot

    def get_model_architecture(self, ensemble_id):
        api = '{ensemble_id}/architecture'
        rep = self.get(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})
        return rep

    def get_model_presentation(self, ensemble_id):
        api = '{ensemble_id}/presentation'
        rep = self.get(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})
        return rep

    def refit_ensemble(self, ensemble_id, data_id):
        data = {
            "datasource_id": data_id,
        }
        api = '{ensemble_id}/refit'
        return self.post(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token},
                         data=data)

    def cancel_ensemble_refit(self, ensemble_id):
        api = '{ensemble_id}/cancel'
        return self.post(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})

    def generate_export_download_link(self, ensemble_id):
        api = "{ensemble_id}/download"
        return self.post(api.format(ensemble_id=ensemble_id), params={'jwt': self.token}, query_prefix='ensembles')

    def get_export_record(self, ensemble_id):
        api = "{ensemble_id}"
        return self.get(api.format(ensemble_id=ensemble_id), params={'jwt': self.token}, query_prefix='exports')

    def create_export_record(self, ensemble_id):
        api = ""
        data = {'ensemble_id': ensemble_id}
        return self.post(api, params={'jwt': self.token}, query_prefix='exports', data=data)

    def rerun_export(self, ensemble_id):
        api = "{ensemble_id}/rerun"
        return self.post(api.format(ensemble_id=ensemble_id), params={'jwt': self.token}, query_prefix='exports')

