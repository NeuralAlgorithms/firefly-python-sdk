from typing import Dict, List

from firefly import utils
from firefly.api_requestor import APIRequestor
from firefly.enums import Estimator, Pipeline, InterpretabilityLevel, ValidationStrategy, SplittingStrategy, \
    TargetMetric, CVStrategy
from firefly.errors import APIError
from firefly.firefly_response import FireflyResponse
from firefly.resources.api_resource import APIResource


class Task(APIResource):
    CLASS_PREFIX = 'tasks'

    @classmethod
    def list(cls, search_term: str = None, page: int = None, page_size: int = None, sort: Dict = None,
             filter_: Dict = None, api_key: str = None) -> FireflyResponse:
        """
        List the existing tasks. Supports filtering, sorting and pagination.

        Args:
            search_term (Optional[str]): Return only records that contain the search_term in one of their fields.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to sort the results by.
            filter_ (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Tasks, which are represented as nested dictionaries under `hits`.
        """
        return cls._list(search_term, page, page_size, sort, filter_, api_key)

    @classmethod
    def get(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get information on a specific task.

        Information includes the state of the task, and other basic attributes

        Args:
            task_id (int): Task ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Task if exists, raises FireflyClientError otherwise.
        """
        return cls._get(id, api_key)

    @classmethod
    def get_by_name(cls, name: str, api_key: str = None) -> FireflyResponse:
        """
        Get information on a specific task, identified by its name.

        Information includes the state of the task, and other basic attributes
        Similar to calling `firefly.Task.list(filters_={'name': [NAME]})`.

        Args:
            name (str): Task name.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Information about the dataset.
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
        Delete a specific model.

        Args:
            id (int): Task ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: "true" if deleted successfuly, raises FireflyClientError otherwise.
        """
        return cls._delete(id, api_key)

    @classmethod
    def create(cls, name: str, estimators: List[Estimator], target_metric: TargetMetric, dataset_id: int,
               splitting_strategy: SplittingStrategy, notes: str = None, ensemble_size: int = None,
               max_models_num: int = None, single_model_timeout: int = None, pipeline: List[Pipeline] = None,
               prediction_latency: int = None, interpretability_level: InterpretabilityLevel = None,
               timeout: int = None, cost_matrix_weights: List[List[str]] = None, train_size: float = None,
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
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Task ID if successful or task data if wait=True, raises FireflyError otherwise.
        """
        existing_ds = cls.list(filter_={'name': [name]}, api_key=api_key)
        if existing_ds and existing_ds['total'] > 0:
            if skip_if_exists:
                return FireflyResponse(data=existing_ds['hits'][0])
            else:
                raise APIError("Task with that name already exists")

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

        requestor = APIRequestor()
        response = requestor.post(url=cls.CLASS_PREFIX, body=task_config, api_key=api_key)
        id = response['task_id']
        if wait:
            utils.wait_for_finite_state(cls.get, id, api_key=api_key)
            response = cls.get(id, api_key=api_key)
        else:
            response = FireflyResponse(data={'id': id})

        return response

    @classmethod
    def edit_notes(cls, id: int, notes: str, api_key: str = None) -> FireflyResponse:
        """
        Edit notes of a Task.

        Args:
            id (int): Task ID.
            notes (str): New notes value.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: `task_id` value if successfull, raises FireflyError otherwise.
        """
        requestor = APIRequestor()
        url = "{prefix}/{task_id}/notes".format(prefix=cls.CLASS_PREFIX, task_id=id)
        response = requestor.put(url=url, body={'notes': notes}, api_key=api_key)
        return response

    @classmethod
    def get_task_progress(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        List the existing ensembles` scores.

        Get the ensemble's scores produced so far by the task. Allows to see the progress of the task.

        Args:
            id (int): Task ID to get progress of.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse:  List of all task's ensembles' scores.
        """
        requestor = APIRequestor()
        url = "{prefix}/{task_id}/progress".format(prefix=cls.CLASS_PREFIX, task_id=id)
        response = requestor.get(url=url, api_key=api_key)
        return response

    @classmethod
    def get_task_result(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get full train task results.

        Explain all the fields.

        Args:
            id (int): Task ID to return results of.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            Dictionary of train task's results.
        """
        requestor = APIRequestor()
        url = "{prefix}/{task_id}/results".format(prefix=cls.CLASS_PREFIX, task_id=id)
        response = requestor.get(url=url, api_key=api_key)
        return response

    @classmethod
    def rerun_task(cls, task_id: int, api_key: str = None) -> FireflyResponse:
        """
        Rerun a task that has been completed or stopped.

        Args:
            task_id (int): Task ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

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
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

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
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        return cls.__do_operation(op='cancel', task_id=task_id, api_key=api_key)

    @classmethod
    def resume_task(cls, task_id: int, api_key: str = None) -> FireflyResponse:
        """
        Resume a paused task.

        Args:
            task_id (int): Task ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        return cls.__do_operation(op='resume', task_id=task_id, api_key=api_key)

    @classmethod
    def __do_operation(cls, task_id, op, api_key=None):
        if op not in ('resume', 'rerun', 'pause', 'cancel'):
            raise APIError("Operation {} is not supported".format(op))
        requestor = APIRequestor()
        url = '{prefix}/{task_id}/{op}'.format(prefix=cls.CLASS_PREFIX, task_id=task_id, op=op)
        response = requestor.post(url=url, api_key=api_key)
        return response
