import time
from collections import OrderedDict

from firefly.errors import *


class ModelsMixin(abc.ABC):

    def get_task_record(self, task_id):
        api = '{task_id}'
        return self.get(api.format(task_id=task_id), params={'jwt': self.token}, query_prefix='tasks')

    def __list_tasks_params(self, search_all_columns=None, page=None, page_size=None, sort=None,
                            filter=None):
        filters = self.parse_filter_parameters(filter)
        sorts = self.parse_sort_parameters(sort)
        return {'search_all_columns': search_all_columns, 'page': page, 'page_size': page_size,
                'sort': sorts, 'filter': filters, 'jwt': self.token}

    def list_tasks(self, search_all_columns=None, page=None, page_size=None, sort=None,
                   filter=None):
        assert sort is None or isinstance(sort, OrderedDict)
        api = ''
        params = self.__list_tasks_params(search_all_columns=search_all_columns, page=page,
                                          page_size=page_size, sort=sort, filter=filter)
        return self.get(query=api, params=params, query_prefix='tasks')

    def list_tasks_prefix(self, task_prefix=None):
        api = 'list_prefix'
        params = {'prefix': task_prefix, 'jwt': self.token}
        return self.get(query=api, params=params, query_prefix='tasks')

    def edit_notes(self, task_id, notes):
        api = '{task_id}/notes'
        notes = {'notes': notes}
        return self.put(query=api.format(task_id=task_id), data=notes, params={'jwt': self.token}, query_prefix='tasks')

    def delete_task(self, task_id):
        api = '{task_id}'
        return self.delete(query=api.format(task_id=task_id), params={'jwt': self.token}, query_prefix='tasks')

    def create(self, name, estimators, target_metric,
               dataset_id, splitting_strategy, notes=None, ensemble_size=None,
               max_models_num=None,
               single_model_timeout=None,
               pipeline=None, prediction_latency=None, interpretability_level=None, timeout=None,
               cost_matrix_weights=None, train_size=None, test_size=None, validation_size=None, fold_size=None,
               n_folds=None,
               horizon=None, validation_strategy=None, cv_strategy=None, forecast_horizon=None, model_life_time=None,
               refit_on_all=None):
        api = ''
        task_config = {
            'dataset_id': dataset_id,
            'name': name,
            'estimators': estimators,
            'target_metric': target_metric,
            'splitting_strategy': splitting_strategy,
            'ensemble_size': ensemble_size,
            'max_models_num': max_models_num,
            'single_model_timeout': single_model_timeout,
            'pipeline': pipeline,
            'prediction_latency': prediction_latency,
            'interpretability_level': interpretability_level,
            'timeout': timeout,
            'cost_matrix_weights': cost_matrix_weights,
            'train_size': train_size,
            'test_size': test_size,
            'validation_size': validation_size,
            'cv_strategy': cv_strategy,
            'n_folds': n_folds,
            'horizon': horizon,
            'forecast_horizon': forecast_horizon,
            'model_life_time': model_life_time,
            'fold_size': fold_size,
            'validation_strategy': validation_strategy,
            'notes': notes,
            'refit_on_all': refit_on_all
        }
        return self.post(api, data=task_config, params={'jwt': self.token}, query_prefix='tasks')

    def get_task_result(self, task_id):
        api = '{task_id}/results'
        return self.get(api.format(task_id=task_id), params={'jwt': self.token}, query_prefix='tasks')

    def get_task_progress(self, task_id):
        api = '{task_id}/progress'
        return self.get(api.format(task_id=task_id), params={'jwt': self.token}, query_prefix='tasks')

    def get_halixograph_report(self, task_id, config_order):
        api = '{task_id}/halixograph'
        return self.get(api.format(task_id=task_id), query_prefix='tasks',
                        params={'jwt': self.token, 'config_order': config_order})




    def rerun_task(self, task_id):
        return self.__do_operation(op='rerun', task_id=task_id)

    def pause_task(self, task_id):
        return self.__do_operation(op='pause', task_id=task_id)

    def cancel_task(self, task_id):
        return self.__do_operation(op='cancel', task_id=task_id)

    def resume_task(self, task_id):
        return self.__do_operation(op='resume', task_id=task_id)

    def get_user_storage(self, jwt):
        api = 'storage'
        return self.get(query=api, params={'jwt': self.token}, query_prefix='tasks')

    def get_configuration(self, presets, dataset_id=None, task_id=None):
        params = {**presets, 'task_id': task_id, 'dataset_id': dataset_id, 'jwt': self.token}
        api = 'configuration'
        return self.get(query=api, params=params, query_prefix='tasks')

    def get_available_configuration_options(self, presets, dataset_id):
        api = 'configuration/options'
        return self.get(query=api,
                        params={**presets, 'dataset_id': dataset_id, 'jwt': self.token}, query_prefix='tasks')

    def continue_task(self, task_id, name, estimators=None, target_metric=None,
                      splitting_strategy=None, notes=None, ensemble_size=None, max_models_num=None,
                      single_model_timeout=None,
                      pipeline=None, prediction_latency=None, interpretability_level=None, timeout=None,
                      cost_matrix_weights=None, train_size=None, test_size=None, validation_size=None, n_folds=None,
                      horizon=None, validation_strategy=None, forecast_horizon=None, model_life_time=None):
        api = '{task_id}/continue'
        task_config = {
            'name': name,
            'estimators': estimators,
            'target_metric': target_metric,
            'splitting_strategy': splitting_strategy,
            'ensemble_size': ensemble_size,
            'max_models_num': max_models_num,
            'single_model_timeout': single_model_timeout,
            'pipeline': pipeline,
            'prediction_latency': prediction_latency,
            'interpretability_level': interpretability_level,
            'timeout': timeout,
            'cost_matrix_weights': cost_matrix_weights,
            'train_size': train_size,
            'test_size': test_size,
            'validation_size': validation_size,
            'n_folds': n_folds,
            'horizon': horizon,
            'forecast_horizon': forecast_horizon,
            'model_life_time': model_life_time,
            'validation_strategy': validation_strategy,
            'notes': notes
        }
        return self.post(api.format(task_id=task_id), data=task_config, params={'jwt': self.token},
                         query_prefix='tasks')

    def __do_operation(self, task_id, op):
        if op not in ('run', 'resume', 'rerun', 'delete', 'pause', 'cancel'):
            raise FireflyClientError("operation {} not supported".format(op))
        if op == 'delete':
            return self.delete_task(jwt=self.token, task_id=task_id)
        api = '{task_id}/{op}'
        return self.post(api.format(op=op, task_id=task_id), params={'jwt': self.token}, query_prefix='tasks')


    #######################ENSEMBLES###############################
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

#################PREDICTIONS################################

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

##############EXPORTS####################
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

