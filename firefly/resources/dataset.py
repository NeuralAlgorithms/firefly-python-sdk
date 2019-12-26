from typing import Dict, List

import firefly
from firefly import utils
from firefly.api_requestor import APIRequestor
from firefly.enums import ProblemType, FeatureType, Estimator, TargetMetric, SplittingStrategy, Pipeline, \
    InterpretabilityLevel, ValidationStrategy, CVStrategy
from firefly.errors import APIError
from firefly.firefly_response import FireflyResponse
from firefly.resources.api_resource import APIResource


class Dataset(APIResource):
    CLASS_PREFIX = 'datasets'

    @classmethod
    def list(cls, search_term: str = None, page: int = None, page_size: int = None, sort: Dict = None,
             filter_: Dict = None, api_key: str = None) -> FireflyResponse:
        """
        List the existing datasets. Supports filtering, sorting and pagination.

        Args:
            search_term (Optional[str]): Return only records that contain the search_term in one of their fields.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to sort the results by.
            filter_ (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Datasets, which are represented as nested dictionaries under `hits`.
        """
        return cls._list(search_term, page, page_size, sort, filter_, api_key)

    @classmethod
    def get(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get information on a specific dataset.

        Information includes the state of the dataset, and other basic attributes.

        Args:
            id (int): Dataset ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Information about the dataset.
        """
        return cls._get(id, api_key)

    @classmethod
    def get_by_name(cls, name: str, api_key: str = None) -> FireflyResponse:
        """
        Get information on a specific dataset, identified by its name.

        Information includes the state of the dataset, and other basic attributes.
        Similar to calling `firefly.Dataset.list(filters_={'name': [NAME]})`.

        Args:
            name (str): Dataset name.
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
        Delete a specific dataset.

        Args:
            id (int): Dataset ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: "true" if deleted successfuly, raises FireflyClientError otherwise.
        """
        return cls._delete(id, api_key)

    @classmethod
    def create(cls, datasource_id: int, dataset_name: str, target: str, problem_type: ProblemType, header: bool,
               na_values: List[str] = None, retype_columns: Dict[str, FeatureType] = None,
               rename_columns: List[str] = None, datetime_format: str = None, time_axis: str = None,
               block_id: List[str] = None, sample_id: List[str] = None, subdataset_id: List[str] = None,
               sample_weight: List[str] = None, not_used: List[str] = None, hidden: List[str] = False,
               wait: bool = False, skip_if_exists: bool = False, api_key: str = None) -> FireflyResponse:
        """
        Creates and prepares a dataset.

        When creating a dataset the feature roles are labled and the feature types can be set by the user.
        Data analysis is done in order to optimize the model training and search process.

        Args:
            datasource_id (int): Datasource ID.
            dataset_name (str): The name of the dataset in the application.
            target (str): The feature name of the target if the header parameter is true, otherwise the column index.
            problem_type (ProblemType): The problem type.
            header (bool): Does to file include a header row or not.
            na_values (Optional[List[str]]): List of na values.
            retype_columns (Dict[str, FeatureType]): Change the chosen type of certain columns.
            rename_columns (Optional[List[str]]): ???
            datetime_format (Optional[str]): The date time format used in the data.
            time_axis (Optional[str]): In timeseries, the feature that is the time axis.
            block_id (Optional[List[str]]): To avoid data leakage, data can be splitted to blocks. Rows with the same
                block id must be all in the train set or the test set. Requires to have at least 50 unique values.
            sample_id (Optional[List[str]]): Row identifier.
            subdataset_id (Optional[List[str]]): Features which specify a subdataset ID in the data.
            sample_weight (Optional[List[str]]): ???
            not_used (Optional[List[str]]): List of features to ignore.
            hidden (Optional[List[str]]): ???
            wait (Optional[bool]): Should call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if dataset with same name exists and skip if true.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Dataset ID if successful or dataset data if wait=True, raises FireflyError otherwise.
        """
        existing_ds = cls.list(filter_={'name': [dataset_name]}, api_key=api_key)
        if existing_ds and existing_ds['total'] > 0:
            if skip_if_exists:
                return FireflyResponse(data=existing_ds['hits'][0])
            else:
                raise APIError("Dataset with that name already exists")

        data = {
            "name": dataset_name,
            "data_id": datasource_id,
            "header": header,
            "problem_type": problem_type.value if problem_type is not None else None,
            "hidden": hidden,
            "na_values": na_values,
            "retype_columns": {key: retype_columns[key].value for key in
                               retype_columns} if retype_columns is not None else None,
            "datetime_format": datetime_format,
            "target": target,
            "time_axis": time_axis,
            "block_id": block_id,
            "sample_id": sample_id,
            "subdataset_id": subdataset_id,
            "sample_weight": sample_weight,
            "not_used": not_used,
            "rename_columns": rename_columns
        }

        requestor = APIRequestor()
        response = requestor.post(url=cls.CLASS_PREFIX, body=data, api_key=api_key)

        if wait:
            id = response['id']
            utils.wait_for_finite_state(cls.get, id, api_key=api_key)
            response = cls.get(id, api_key=api_key)

        return response

    @classmethod
    def train(cls, task_name: str, estimators: List[Estimator], target_metric: TargetMetric, dataset_id: int,
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
        Similar to calling `firefly.Task.create(...)`.

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
        return firefly.Task.create(task_name, estimators, target_metric, dataset_id, splitting_strategy, notes,
                                   ensemble_size, max_models_num, single_model_timeout, pipeline, prediction_latency,
                                   interpretability_level, timeout, cost_matrix_weights, train_size, test_size,
                                   validation_size, fold_size, n_folds, validation_strategy, cv_strategy, horizon,
                                   forecast_horizon, model_life_time, refit_on_all, wait, skip_if_exists, api_key)

    @classmethod
    def get_available_configuration_options(cls, id: int, presets=None, api_key: str = None) -> FireflyResponse:
        """
        Get possible configurations for a specific dataset.

        Return lists of possible values for estimators, pipelines, target metrics and splitting strategies for the
        specified dataset.

        Args:
            dataset_id (int): Dataset ID to get possible configuration.
            presets (Optional[dict]): Dictionary with presets for the configuration.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            Dictionary containing lists of possible values for parameters.
        """
        if presets is None:
            presets = {}
        requestor = APIRequestor()
        url = "tasks/configuration/options"
        response = requestor.get(url=url, params={'dataset_id': id, **presets}, api_key=api_key)
        new_data = {
            'estimators': [Estimator(e) for e in response['estimators']],
            'target_metric': [TargetMetric(e) for e in response['target_metric']],
            'splitting_strategy': [SplittingStrategy(e) for e in response['splitting_strategy']],
            'pipeline': [Pipeline(e) for e in response['pipeline']],
        }
        return FireflyResponse(data=new_data)
