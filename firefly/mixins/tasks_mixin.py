import time
from collections import OrderedDict
from typing import List, Union, Dict

from firefly.errors import *
from firefly.enums import *

FINITE_STATES = ['COMPLETED', 'CREATED', 'CANCELED', 'FAILED', 'PAUSED']


class TasksMixin(abc.ABC):

    def get_task_record(self, task_id: int) -> Dict[str, Union[str, int]]:
        """
        Get details of a task.

        Returns a dictionary with metadata regarding the Task, e.g. state, run time and ensemble ID.

        Args:
            task_id (int): ID of task.

        Returns:
            Dictionary with metadata if Task exists, raises FireflyClientError otherwise.
        """
        api = '{task_id}'
        return self.get(api.format(task_id=task_id), params={'jwt': self.token}, query_prefix='tasks')

    def __list_tasks_params(self, search_term: str = None, page: int = None, page_size: int = None,
                            sort: Dict[str, Union[str, int]] = None, filter: Dict[str, Union[str, int]] = None) -> Dict:
        filters = self.parse_filter_parameters(filter)
        sorts = self.parse_sort_parameters(sort)
        return {'search_term': search_term, 'page': page, 'page_size': page_size,
                'sort': sorts, 'filter': filters, 'jwt': self.token}

    def list_tasks(self, search_term: str = None, page: int = None, page_size: int = None,
                   sort: Dict[str, Union[str, int]] = None, filter: Dict[str, Union[str, int]] = None) -> Dict:
        """
        List all of the user's Tasks' metadata.

        Returns all of the Tasks' metadata, represented as dictionaries in a list. Every dict contains
        metadata regarding a Task, same as returned with `get_task_record`.

        Args:
            search_term (Optional[str]): Return only records that contain the search_term in one of their fields.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to sort the results by.
            filter (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.

        Returns:
            Dictionary containing the Tasks, which are represented as nested dictionaries.
        """
        assert sort is None or isinstance(sort, OrderedDict)
        api = ''
        params = self.__list_tasks_params(search_term=search_term, page=page,
                                          page_size=page_size, sort=sort, filter=filter)
        return self.get(query=api, params=params, query_prefix='tasks')

    def list_tasks_prefix(self, task_prefix: str = None) -> Dict:
        """
        List all Tasks with name starting with `prefix`.

        Args:
            task_prefix (Optional[str]): Prefix to filter Tasks.

        Returns:
            Dictionary containing the Tasks, which are represented as nested dictionaries.
        """
        api = 'list_prefix'
        params = {'prefix': task_prefix, 'jwt': self.token}
        return self.get(query=api, params=params, query_prefix='tasks')

    def get_tasks_by_name(self, task_name: str) -> List:
        """
        List all Tasks with a specific name.

        Args:
            task_name (str): Name of the Task.

        Returns:
            List all Tasks with name equals to `task_name`.
        """
        tasks = self.list_tasks(filter={'name': [task_name]})
        return tasks['hits']

    def edit_notes(self, task_id: int, notes: str) -> Dict:
        """
        Edits notes of a Task.

        Args:
            task_id (int): Task ID.
            notes (str): New notes value.

        Returns:
            Dict with `task_id` value if successfull, fireflyClientError otherwise.
        """
        api = '{task_id}/notes'
        notes = {'notes': notes}
        return self.put(query=api.format(task_id=task_id), data=notes, params={'jwt': self.token}, query_prefix='tasks')

    def delete_task(self, task_id: int) -> str:
        """
        Deletes a Task from the server.

        Args:
            task_id (int): ID of Task to delete.

        Returns:
            "done" (str) if completed successfully, raises FireflyClientError otherwise.
        """
        api = '{task_id}'
        return self.delete(query=api.format(task_id=task_id), params={'jwt': self.token}, query_prefix='tasks')

    def train(self, name: str, estimators: List[Estimator], target_metric: TargetMetric,
              dataset_id: int, splitting_strategy: SplittingStrategy, notes: str = None, ensemble_size: int = None,
              max_models_num: int = None,
              single_model_timeout: int = None,
              pipeline: List[Pipeline] = None, prediction_latency: int = None,
              interpretability_level: InterpretabilityLevel = None, timeout: int = None,
              cost_matrix_weights: List[List[str]] = None, train_size: float = None, test_size: float = None,
              validation_size: float = None, fold_size: int = None,
              n_folds: int = None,
              horizon: int = None, validation_strategy: ValidationStrategy = None, cv_strategy: CVStrategy = None,
              forecast_horizon: int = None, model_life_time: int = None,
              refit_on_all: bool = None, wait: bool = False, skip_if_exists: bool = False) -> int:
        """
        Create and run a training task.

        A task is responssible for searching for hyper prameters that would maximize the model scores.
        The task constructs ensembles made of selected models. Seeking ways to combine different models allows us
        a smarter decision making.

        Args:
            name (str): Task's name.
            estimators (List[Estimator]): Estimators to use in the train task.
            target_metric (TargetMetric): The target metric is the metric the model hyperparameter search process
                attempts to optimize.
            dataset_id (int): Dataset ID of the training data.
            splitting_strategy (SplittingStrategy): Splitting strategy of the data.
            notes (Optional[str]): Notes of the task.
            ensemble_size (Optional[int]): Maximum number for models in ensemble.
            max_models_num (Optional[int]): Maximum number of models to train.
            single_model_timeout (Optional[int]): Maximum time for training one model.
            pipeline (Optional[List[Pipeline]): Possible pipeline steps.
            prediction_latency (Optional[int]): Maximum number of seconds ensemble prediction should take.
            interpretability_level (Optional[InterpretabilityLevel]): Determines how interpertable your ensemble is. Higher level
                of interpretability leads to more interpretable ensembles
            timeout (Optional[int]): timeout for the search process.
            cost_matrix_weights (Optional[List[List[str]]]): For classification and anomaly detection problems, the weights allow
                determining a custom cost metric, which assigns different weights to the entries of the confusion matrix.
            train_size (Optional[int]): The ratio of data taken for the train set of the model.
            test_size (Optional[int]): The ratio of data taken for the test set of the model.
            validation_size (Optional[int]): The ratio of data taken for the validation set of the model.
            fold_size (Optional[int]): Fold size where performing cross-validation splitting.s
            n_folds (Optional[int]): Number of folds when performing cross-validation splitting.\
            validation_strategy (Optional[ValidationStrategy]): Validation strategy used for the train task.
            cv_strategy (Optional[CVStrategy]): Cross-validation strategy to use for the train task.
            horizon (Optional[int]): Something related to time-series models.
            forecast_horizon (Optional[int]): Something related to time-series models.
            model_life_time (Optional[int]): Something related to time-series models.
            refit_on_all (Optional[bool]): Determines if the final ensemble will be refit on all data after
                search process is done.
            wait (Optional[bool]): Should call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if train task with same name exists and skip if it does.

        Returns:
            Task ID of train task created.
        """
        if skip_if_exists:
            ids = self.get_tasks_by_name(name)
            if ids:
                return ids[0]['id']
        api = ''
        task_config = {
            'dataset_id': dataset_id,
            'name': name,
            'estimators': [e.value for e in estimators],
            'target_metric': target_metric.value if target_metric is not None else None,
            'splitting_strategy': splitting_strategy.value if splitting_strategy is not None else None,
            'ensemble_size': ensemble_size,
            'max_models_num': max_models_num,
            'single_model_timeout': single_model_timeout,
            'pipeline': [p.value for p in pipeline],
            'prediction_latency': prediction_latency,
            'interpretability_level': interpretability_level.value if interpretability_level is not None else None,
            'timeout': timeout,
            'cost_matrix_weights': cost_matrix_weights,
            'train_size': train_size,
            'test_size': test_size,
            'validation_size': validation_size,
            'cv_strategy': cv_strategy.value if cv_strategy is not None else None,
            'n_folds': n_folds,
            'horizon': horizon,
            'forecast_horizon': forecast_horizon,
            'model_life_time': model_life_time,
            'fold_size': fold_size,
            'validation_strategy': validation_strategy.value if validation_strategy is not None else None,
            'notes': notes,
            'refit_on_all': refit_on_all
        }
        result = self.post(api, data=task_config, params={'jwt': self.token}, query_prefix='tasks').get('task_id')
        if wait:
            self.__wait_for_finite_state(task_id=result, getter=self.get_task_record)

        return result

    def refit(self, task_id: int, data_id: int, wait: bool = False) -> None:
        """
        Refit the models of a chosen ensemble.

        A refit trains a the chosen ensemble's models with the data of the given datasource. The model training is done
        from scratch and uses all the given data. A new ensemble is created that is made of all the refitted models of
        the chosen ensemble and their original combination. The returned result is the new ensemble id of the refitted
        ensemble, and the dataset_id of the dataset created from the given datasource.

        Args:
            task_id (int): The Task ID to be refitted.
            data_id (int): The datasource ID to use.
            wait (Optional[bool]): Should call be synchronous or not.

        Returns:
            None.
        """
        data = {
            "datasource_id": data_id,
        }
        ensemble_id = self.get_task_record(task_id).get('ensemble_id')
        api = '{ensemble_id}/refit'
        result = self.post(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token},
                           data=data)
        if wait:
            self.__wait_for_finite_state(task_id, self.get_task_record)

    def cancel_ensemble_refit(self, ensemble_id: int) -> str:
        """
        Cancels run of ensemble's refit process.

        Args:
            ensemble_id (int): Ensemble ID to cancel redit process.

        Returns:
            "canceled" if refit termination message was sent successfully, raises FireflyClientError otherwise.
        """
        api = '{ensemble_id}/cancel'
        return self.post(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})

    def get_task_result(self, task_id: int) -> Dict:
        """
        Get full train task results.

        Explain all the fields.

        Args:
            task_id (int): Task ID to return results of.

        Returns:
            Dictionary of train task's results.
        """
        api = '{task_id}/results'
        return self.get(api.format(task_id=task_id), params={'jwt': self.token}, query_prefix='tasks')

    def get_task_progress(self, task_id: int) -> Dict:
        """
        List the existing ensemble scores.

        Get the ensemble's scores produced so far by the task. Allows to see the progress of the task.

        Args:
            task_id (int): Task ID to get progress of.

        Returns:
            Dictionary containing a list of all ensemble's scores.
        """
        api = '{task_id}/progress'
        return self.get(api.format(task_id=task_id), params={'jwt': self.token}, query_prefix='tasks')

    def get_halixograph_report(self, task_id: int, config_order: Dict = None) -> Dict:
        """
        Get halixograph report.

        Args:
            task_id (int): Task ID to get the report.
            config_order (Optional[Dict]): Configuration for the graph.

        Returns:
            Dictionary - explain more?
        """
        api = '{task_id}/halixograph'
        return self.get(api.format(task_id=task_id), query_prefix='tasks',
                        params={'jwt': self.token, 'config_order': config_order})

    def rerun_task(self, task_id: int) -> str:
        """
        Rerun a task that has been completed or stopped.

        Args:
            task_id (int): Task ID to rerun.

        Returns:
            "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        return self.__do_operation(op='rerun', task_id=task_id)

    def pause_task(self, task_id: int) -> str:
        """
        Pauses a running task.

        Args:
            task_id (int): Task ID to pause.

        Returns:
            "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        return self.__do_operation(op='pause', task_id=task_id)

    def cancel_task(self, task_id: int) -> str:
        """
        Cancels a running task.

        Args:
            task_id (int): Task ID to cancel.

        Returns:
            "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        return self.__do_operation(op='cancel', task_id=task_id)

    def resume_task(self, task_id: int) -> str:
        """
        Resume a paused task.

        Args:
            task_id (int): Task ID to resume.

        Returns:
            "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        return self.__do_operation(op='resume', task_id=task_id)

    # TODO: API doesn't exists - remove?
    def get_user_storage(self):
        api = 'storage'
        return self.get(query=api, params={'jwt': self.token}, query_prefix='tasks')

    # TODO: not needed due to enums - remove?
    def get_configuration(self, presets, dataset_id=None, task_id=None):
        params = {**presets, 'task_id': task_id, 'dataset_id': dataset_id, 'jwt': self.token}
        api = 'configuration'
        return self.get(query=api, params=params, query_prefix='tasks')

    def get_available_configuration_options(self, dataset_id: int, presets: Dict = {}) -> Dict:
        """
        Get possible configurations for a specific dataset.

        Return lists of possible values for estimators, pipelines, target metrics and splitting strategies for the
        specified dataset.

        Args:
            dataset_id (int): Dataset ID to get possible configuration.
            presets (Optional[dict]): Dictionary with presets for the configuration.

        Returns:
            Dictionary containing lists of possible values for parameters.
        """
        api = 'configuration/options'
        return self.get(query=api,
                        params={**presets, 'dataset_id': dataset_id, 'jwt': self.token}, query_prefix='tasks')

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
