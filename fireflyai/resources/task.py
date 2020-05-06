"""
The task entity represents the model training process. When users train a model,
a task is created for that process, using a Dataset as the input for the model.

When creating a Task, set the training configuration (e.g. target metric, ensemble size),
which will determine the way the model is trained.

One of the outputs of a Task is an Ensemble; a combination of one or more models optimized for the Dataset and configuration.
In addition, it is possible to use Ensembles to perform predictions, as well as other purposes.

‘Task’ API includes creating a task and querying existing tasks (Get, List, Delete and Get configuration).
"""
from typing import Dict, List

import fireflyai

from fireflyai import utils, logger
from fireflyai.api_requestor import APIRequestor
from fireflyai.enums import Estimator, Pipeline, InterpretabilityLevel, ValidationStrategy, SplittingStrategy, \
    TargetMetric, CVStrategy, ProblemType
from fireflyai.errors import APIError, InvalidRequestError
from fireflyai.firefly_response import FireflyResponse
from fireflyai.resources.api_resource import APIResource


class Task(APIResource):
    _CLASS_PREFIX = 'tasks'

    @classmethod
    def list(cls, search_term: str = None, page: int = None, page_size: int = None, sort: Dict = None,
             filter_: Dict = None, api_key: str = None) -> FireflyResponse:
        """
        List the existing Tasks - supports filtering, sorting and pagination.

        Args:
            search_term (Optional[str]): Return only records that contain the `search_term` in any field.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules  to sort the results by.
            filter_ (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: Tasks are represented as nested dictionaries under `hits`.
        """
        return cls._list(search_term, page, page_size, sort, filter_, api_key)

    @classmethod
    def get(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get information on a specific Task.

        Information includes the state of the Task and other attributes.

        Args:
            id (int): Task ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Information about the Task.
        """
        return cls._get(id, api_key)

    @classmethod
    def get_by_name(cls, name: str, api_key: str = None) -> FireflyResponse:
        """
        Gets information on a specific Task identified by its name.

        Information includes the state of the Task and other attributes.
        Similar to calling `fireflyai.Task.list(filters_={'name': [NAME]})`.

        Args:
            name (str): Task name.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Information about the Task.
        """
        resp = cls.list(filter_={'name': [name]}, api_key=api_key)
        if resp and 'total' in resp and resp['total'] > 0:
            ds = resp['hits'][0]
            return FireflyResponse(data=ds)
        else:
            raise APIError("Dataset with that name does not exist")

    @classmethod
    def delete(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Deletes a specific Task.

        Args:
            id (int): Task ID.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: "true" if deleted successfuly, raises FireflyClientError otherwise.
        """
        return cls._delete(id, api_key)

    @classmethod
    def create(cls, name: str, dataset_id: int, estimators: List[Estimator] = None, target_metric: TargetMetric = None,
               splitting_strategy: SplittingStrategy = None, notes: str = None, ensemble_size: int = None,
               max_models_num: int = None, single_model_timeout: int = None, pipeline: List[Pipeline] = None,
               prediction_latency: int = None, interpretability_level: InterpretabilityLevel = None,
               timeout: int = 7200, cost_matrix_weights: List[List[str]] = None, train_size: float = None,
               test_size: float = None, validation_size: float = None, fold_size: int = None, n_folds: int = None,
               horizon: int = None, validation_strategy: ValidationStrategy = None, cv_strategy: CVStrategy = None,
               forecast_horizon: int = None, model_life_time: int = None, refit_on_all: bool = None, wait: bool = False,
               skip_if_exists: bool = False, api_key: str = None) -> FireflyResponse:
        """
        Create and run a training task.

        A task is responsible for searching for hyper-parameters that would maximize the model scores.
        The task constructs ensembles made of selected models. Seeking ways to combine different models allows us
        a smarter decision making.

        Args:
            name (str): Task's name.
            dataset_id (int): Dataset ID of the training data.
            estimators (List[Estimator]): Estimators to use in the train task.
            target_metric (TargetMetric): The target metric is the metric the model hyperparameter search process
                attempts to optimize.
            splitting_strategy (SplittingStrategy): Splitting strategy of the data.
            notes (Optional[str]): Notes of the task.
            ensemble_size (Optional[int]): Maximum number for models in ensemble.
            max_models_num (Optional[int]): Maximum number of models to train.
            single_model_timeout (Optional[int]): Maximum time for training one model.
            pipeline (Optional[List[Pipeline]): Possible pipeline steps.
            prediction_latency (Optional[int]): Maximum number of seconds ensemble prediction should take.
            interpretability_level (Optional[InterpretabilityLevel]): Determines how interpertable your ensemble is. Higher level
                of interpretability leads to more interpretable ensembles
            timeout (Optional[int]): timeout in seconds for the search process (default: 2 hours).
            cost_matrix_weights (Optional[List[List[str]]]): For classification and anomaly detection problems, the weights allow
                determining a custom cost metric, which assigns different weights to the entries of the confusion matrix.
            train_size (Optional[int]): The ratio of data taken for the train set of the model.
            test_size (Optional[int]): The ratio of data taken for the test set of the model.
            validation_size (Optional[int]): The ratio of data taken for the validation set of the model.
            fold_size (Optional[int]): Fold size where performing cross-validation splitting.s
            n_folds (Optional[int]): Number of folds when performing cross-validation splitting.\
            validation_strategy (Optional[ValidationStrategy]): Validation strategy used for the train task.
            cv_strategy (Optional[CVStrategy]): Cross-validation strategy to use for the train task.
            horizon (Optional[int]): DEPRECATED. Please use `forecast_horizon` and `model_life_time`.
            forecast_horizon (Optional[int]): Something related to time-series models.
            model_life_time (Optional[int]): Something related to time-series models.
            refit_on_all (Optional[bool]): Determines if the final ensemble will be refit on all data after
                search process is done.
            wait (Optional[bool]): Should the call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if a Datasource with same name exists and skip if true.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Task ID, if successful and wait=False or Task if successful and wait=True;
            raises FireflyError otherwise.
        """
        if horizon is not None:
            logger.warning("Parameter `horizon` is DEPRECATED. Please use `forecast_horizon` and `model_life_time`.")

        existing_ds = cls.list(filter_={'name': [name]}, api_key=api_key)
        if existing_ds and existing_ds['total'] > 0:
            if skip_if_exists:
                return FireflyResponse(data=existing_ds['hits'][0])
            else:
                raise InvalidRequestError("Task with that name already exists")

        try:
            dataset = fireflyai.Dataset.get(id=dataset_id, api_key=api_key)
        except InvalidRequestError as e:
            raise e

        problem_type = ProblemType(dataset['problem_type'])

        task_config = cls._get_config_defaults(dataset_id=dataset_id, problem_type=problem_type,
                                               inter_level=interpretability_level)

        user_config = {
            'dataset_id': dataset_id,
            'name': name,
            'estimators': [e.value for e in estimators] if estimators is not None else None,
            'target_metric': target_metric.value if target_metric is not None else None,
            'splitting_strategy': splitting_strategy.value if splitting_strategy is not None else None,
            'ensemble_size': ensemble_size,
            'max_models_num': max_models_num,
            'single_model_timeout': single_model_timeout,
            'pipeline': [p.value for p in pipeline] if pipeline is not None else None,
            'prediction_latency': prediction_latency,
            'interpretability_level': interpretability_level.value if interpretability_level is not None else None,
            'timeout': timeout,
            'cost_matrix_weights': cost_matrix_weights,
            'train_size': train_size,
            'test_size': test_size,
            'validation_size': validation_size,
            'cv_strategy': cv_strategy.value if cv_strategy is not None else None,
            'n_folds': n_folds,
            'forecast_horizon': forecast_horizon,
            'model_life_time': model_life_time,
            'fold_size': fold_size,
            'validation_strategy': validation_strategy.value if validation_strategy is not None else None,
            'notes': notes,
            'refit_on_all': refit_on_all
        }
        task_config.update({k: v for k, v in user_config.items() if v is not None})

        requestor = APIRequestor()
        response = requestor.post(url=cls._CLASS_PREFIX, body=task_config, api_key=api_key)
        id = response['task_id']
        if wait:
            utils.wait_for_finite_state(cls.get, id, api_key=api_key)
            response = cls.get(id, api_key=api_key)
        else:
            response = FireflyResponse(data={'id': id})

        return response

    @classmethod
    def refit(cls, id: int, datasource_id: int, wait: bool = False, api_key: str = None) -> FireflyResponse:
        """
        Refits the chosen Ensemble of a Task on a specific Datasource.

        A refit trains the chosen Ensemble's models with the data of the given Datasource. The model training is done
        from scratch and uses all the given data. A new Ensemble is created that is made of all the refitted models of
        the chosen Ensemble and their original combination.

        Args:
            id (int): Task ID.
            datasource_id (int): Datasource ID.
            wait (Optional[bool]): Should the call be synchronous or not.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Ensemble ID, if successful and wait=False or Ensemble if successful and wait=True;
            raises FireflyError otherwise.
        """
        data = {
            "datasource_id": datasource_id,
        }

        ensemble_id = cls.get(id=id, api_key=api_key).get('ensemble_id', None)
        if not ensemble_id:
            raise InvalidRequestError(message="No ensemble exists for this Task.")

        requestor = APIRequestor()
        url = "ensembles/{ensemble_id}/refit".format(ensemble_id=ensemble_id)
        response = requestor.post(url=url, body=data, api_key=api_key)
        new_ens_id = response.get('ensemble_id')

        if wait:
            utils.wait_for_finite_state(fireflyai.Ensemble.get, new_ens_id, api_key=api_key)
            response = fireflyai.Ensemble.get(new_ens_id, api_key=api_key)
        else:
            response = FireflyResponse(data={'id': new_ens_id}, headers=response.headers,
                                       status_code=response.status_code)

        return response

    @classmethod
    def edit_notes(cls, id: int, notes: str, api_key: str = None) -> FireflyResponse:
        """
        Edits notes of the Task.

        Args:
            id (int): Task ID.
            notes (str): New notes value.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: `task_id` value if successful, raises FireflyError otherwise.
        """
        requestor = APIRequestor()
        url = "{prefix}/{task_id}/notes".format(prefix=cls._CLASS_PREFIX, task_id=id)
        response = requestor.put(url=url, body={'notes': notes}, api_key=api_key)
        return response

    @classmethod
    def get_task_progress(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Lists existing Ensembles` scores.

        Get the Ensembles' scores produced thus far by the task. Enables you to track the progress of the task.

        Args:
            id (int): Task ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse:  List of all the Task's Ensembles' scores.
        """
        requestor = APIRequestor()
        url = "{prefix}/{task_id}/progress".format(prefix=cls._CLASS_PREFIX, task_id=id)
        response = requestor.get(url=url, api_key=api_key)
        return response

    @classmethod
    def get_task_result(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Gets full results of the Task.

        Args:
            id (int): Task ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Task's full results.
        """
        requestor = APIRequestor()
        url = "{prefix}/{task_id}/results".format(prefix=cls._CLASS_PREFIX, task_id=id)
        response = requestor.get(url=url, api_key=api_key)
        return response

    @classmethod
    def add_additional_time_to_completed_task(cls, id: int, new_time_budget: int,
                                              api_key: str = None) -> FireflyResponse:
        """
        Add addition time to train completed_task.

        Args:
            id (int): Task ID.
            new_time_budget (int): Time in seconds.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        requestor = APIRequestor()
        url = "{prefix}/{task_id}/add_additional_time/{new_time_budget}".format(prefix=cls._CLASS_PREFIX, task_id=id,
                                                                                new_time_budget=new_time_budget)
        response = requestor.post(url=url, api_key=api_key)
        return response

    @classmethod
    def rerun_task(cls, task_id: int, api_key: str = None) -> FireflyResponse:
        """
        Reruns a task that has been completed or stopped.

        Args:
            task_id (int): Task ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        return cls.__do_operation(op='rerun', task_id=task_id, api_key=api_key)

    @classmethod
    def pause_task(cls, task_id: int, api_key: str = None) -> FireflyResponse:
        """
        Pauses a running task.

        Args:
            task_id (int): Task ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        return cls.__do_operation(op='pause', task_id=task_id, api_key=api_key)

    @classmethod
    def cancel_task(cls, task_id: int, api_key: str = None) -> FireflyResponse:
        """
        Cancels a running task.

        Args:
            task_id (int): Task ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        return cls.__do_operation(op='cancel', task_id=task_id, api_key=api_key)

    @classmethod
    def resume_task(cls, task_id: int, api_key: str = None) -> FireflyResponse:
        """
        Resumes a paused task.

        Args:
            task_id (int): Task ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        return cls.__do_operation(op='resume', task_id=task_id, api_key=api_key)

    @classmethod
    def __do_operation(cls, task_id, op, api_key=None):
        if op not in ('resume', 'rerun', 'pause', 'cancel'):
            raise APIError("Operation {} is not supported".format(op))
        requestor = APIRequestor()
        url = '{prefix}/{task_id}/{op}'.format(prefix=cls._CLASS_PREFIX, task_id=task_id, op=op)
        response = requestor.post(url=url, api_key=api_key)
        return response

    @classmethod
    def _get_config_defaults(cls, dataset_id, problem_type, inter_level, api_key=None):
        config = {}
        if problem_type in [ProblemType.CLASSIFICATION, ProblemType.ANOMALY_DETECTION]:
            config['target_metric'] = TargetMetric.RECALL_MACRO.value
            config['splitting_strategy'] = SplittingStrategy.STRATIFIED.value
        elif problem_type in [ProblemType.TIMESERIES_CLASSIFICATION, ProblemType.TIMESERIES_ANOMALY_DETECTION]:
            config['target_metric'] = TargetMetric.RECALL_MACRO.value
            config['splitting_strategy'] = SplittingStrategy.TIME_ORDER.value
        elif problem_type == ProblemType.TIMESERIES_REGRESSION:
            config['target_metric'] = TargetMetric.MAE.value
            config['splitting_strategy'] = SplittingStrategy.TIME_ORDER.value
        elif problem_type == ProblemType.REGRESSION:
            config['target_metric'] = TargetMetric.R2.value
            config['splitting_strategy'] = SplittingStrategy.SHUFFLED.value

        if inter_level == InterpretabilityLevel.PRECISE:
            config['ensemble_size'] = 5
            config['max_models_num'] = 200
        elif inter_level == InterpretabilityLevel.EXPLAINABLE:
            config['ensemble_size'] = 1
            config['max_models_num'] = 20

        estimators = fireflyai.Dataset.get_available_estimators(id=dataset_id, inter_level=inter_level)
        pipeline = fireflyai.Dataset.get_available_pipeline(id=dataset_id, inter_level=inter_level)

        config['estimators'] = [e.value for e in estimators] if estimators is not None else None
        config['pipeline'] = [p.value for p in pipeline] if pipeline is not None else None

        return config
