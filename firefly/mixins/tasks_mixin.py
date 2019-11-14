import time
from collections import OrderedDict

from firefly.errors import *

FINITE_STATES = ['COMPLETED', 'CREATED', 'CANCELED', 'FAILED', 'PAUSED']


class TasksMixin(abc.ABC):

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

    def get_tasks_by_name(self, task_name):
        tasks = self.list_tasks(filter={'name': [task_name]})
        return tasks['hits']

    def edit_notes(self, task_id, notes):
        api = '{task_id}/notes'
        notes = {'notes': notes}
        return self.put(query=api.format(task_id=task_id), data=notes, params={'jwt': self.token}, query_prefix='tasks')

    def delete_task(self, task_id):
        api = '{task_id}'
        return self.delete(query=api.format(task_id=task_id), params={'jwt': self.token}, query_prefix='tasks')

    def train(self, name, estimators, target_metric,
              dataset_id, splitting_strategy, notes=None, ensemble_size=None,
              max_models_num=None,
              single_model_timeout=None,
              pipeline=None, prediction_latency=None, interpretability_level=None, timeout=None,
              cost_matrix_weights=None, train_size=None, test_size=None, validation_size=None, fold_size=None,
              n_folds=None,
              horizon=None, validation_strategy=None, cv_strategy=None, forecast_horizon=None, model_life_time=None,
              refit_on_all=None, wait=False, skip_if_exists=False):

        if skip_if_exists:
            ids = self.get_tasks_by_name(name)
            if ids:
                return ids[0]['id']
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
        result = self.post(api, data=task_config, params={'jwt': self.token}, query_prefix='tasks').get('task_id')
        if wait:
            self.__wait_for_finite_state(task_id=result, getter=self.get_task_record)

        return result

    def refit(self, task_id, data_id, wait=False):
        data = {
            "datasource_id": data_id,
        }
        ensemble_id = self.get_task_record(task_id).get('ensemble_id')
        api = '{ensemble_id}/refit'
        result =  self.post(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token},
                         data=data)
        if wait:
            self.__wait_for_finite_state(task_id, self.get_task_record)


    def cancel_ensemble_refit(self, ensemble_id):
        api = '{ensemble_id}/cancel'
        return self.post(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})

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

    def get_user_storage(self):
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

    def __wait_for_finite_state(self, task_id, getter):
        res = getter(task_id)
        state = res['state']
        while state not in FINITE_STATES:
            time.sleep(5)
            res = getter(task_id)
            state = res['state']
