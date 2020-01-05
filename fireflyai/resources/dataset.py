"""
The Dataset entity represents a feature labeling transformation, applied to a Datasource, before initiating training models.
Alternatively, a Dataset, is a translated version of raw data that is presented in a format that Firefly.ai can read.
A Dataset is the same data from the raw CSV file presented as a list of numerical, categorical and date/time features,
alongside respected feature roles (target, block-id, etc.).

‘Dataset’ API includes creating a Dataset from a Datasource and querying existing Datasets (Get, List, Preview and Delete).
"""
from typing import Dict, List

import fireflyai
from fireflyai import utils
from fireflyai.api_requestor import APIRequestor
from fireflyai.enums import ProblemType, FeatureType, Estimator, TargetMetric, SplittingStrategy, Pipeline, \
    InterpretabilityLevel, ValidationStrategy, CVStrategy
from fireflyai.errors import APIError, InvalidRequestError
from fireflyai.firefly_response import FireflyResponse
from fireflyai.resources.api_resource import APIResource


class Dataset(APIResource):
    _CLASS_PREFIX = 'datasets'

    @classmethod
    def list(cls, search_term: str = None, page: int = None, page_size: int = None, sort: Dict = None,
             filter_: Dict = None, api_key: str = None) -> FireflyResponse:
        """
        Lists the existing Datasets - supports filtering, sorting and pagination.

        Args:
            search_term (Optional[str]): Return only records that contain the `search_term` in any field.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules  to sort the results by.
            filter_ (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: Datasets are represented as nested dictionaries under `hits`.
        """
        return cls._list(search_term, page, page_size, sort, filter_, api_key)

    @classmethod
    def get(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Gets information on a specific Dataset.

        Information includes the state of the Dataset and other attributes.

        Args:
            id (int): Dataset ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Information about the Dataset.
        """
        return cls._get(id, api_key)

    @classmethod
    def get_by_name(cls, name: str, api_key: str = None) -> FireflyResponse:
        """
        Gets information on a specific Dataset identified by its name.

        Information includes the state of the Dataset and other attributes.
        Similar to calling `fireflyai.Dataset.list(filters_={'name': [NAME]})`.

        Args:
            name (str): Dataset name.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Information about the Dataset.
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
        Deletes a specific Dataset.

        Args:
            id (int): Dataset ID.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: "true" if deleted successfuly, raises FireflyClientError otherwise.
        """
        return cls._delete(id, api_key)

    @classmethod
    def create(cls, datasource_id: int, dataset_name: str, target: str, problem_type: ProblemType, header: bool = True,
               na_values: List[str] = None, retype_columns: Dict[str, FeatureType] = None,
               rename_columns: List[str] = None, datetime_format: str = None, time_axis: str = None,
               block_id: List[str] = None, sample_id: List[str] = None, subdataset_id: List[str] = None,
               sample_weight: List[str] = None, not_used: List[str] = None, hidden: List[str] = False,
               wait: bool = False, skip_if_exists: bool = False, api_key: str = None) -> FireflyResponse:
        """
        Creates and prepares a Dataset.

        While creating a Dataset, the feature roles are labeled and the feature types can be set by the user.
        Data analysis is done in order to optimize model training and search process.

        Args:
            datasource_id (int): Datasource ID.
            dataset_name (str): The name of the Dataset.
            target (str): The name of the target feature, or its column index if header=False.
            problem_type (ProblemType): The problem type.
            header (bool): Does the file include a header row or not.
            na_values (Optional[List[str]]): List of user specific Null values.
            retype_columns (Dict[str, FeatureType]): Change the types of certain columns.
            rename_columns (Optional[List[str]]): ??? #TODO
            datetime_format (Optional[str]): The datetime format used in the data.
            time_axis (Optional[str]): In timeseries problems, the feature that is the time axis.
            block_id (Optional[List[str]]): To avoid data leakage, data can be split into blocks. Rows with the same
                `block_id`, must all be in the train set or the test set. Requires at least 50 unique values in the data.
            sample_id (Optional[List[str]]): Row identifier.
            subdataset_id (Optional[List[str]]): Features which specify a subdataset ID in the data.
            sample_weight (Optional[List[str]]): ??? #TODO
            not_used (Optional[List[str]]): List of features to ignore.
            hidden (Optional[List[str]]): List of features to mark as hidden.
            wait (Optional[bool]): Should the call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if a Dataset with same name exists and skip if true.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: Dataset ID, if successful and wait=False or Dataset if successful and wait=True;
            raises FireflyError otherwise.
        """
        existing_ds = cls.list(filter_={'name': [dataset_name]}, api_key=api_key)
        if existing_ds and existing_ds['total'] > 0:
            if skip_if_exists:
                return FireflyResponse(data=existing_ds['hits'][0])
            else:
                raise InvalidRequestError("Dataset with that name already exists")

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
        response = requestor.post(url=cls._CLASS_PREFIX, body=data, api_key=api_key)

        if wait:
            id = response['id']
            utils.wait_for_finite_state(cls.get, id, api_key=api_key)
            response = cls.get(id, api_key=api_key)

        return response

    @classmethod
    def train(cls, task_name: str, dataset_id: int, estimators: List[Estimator] = None,
              target_metric: TargetMetric = None,
              splitting_strategy: SplittingStrategy = None, notes: str = None, ensemble_size: int = None,
              max_models_num: int = None, single_model_timeout: int = None, pipeline: List[Pipeline] = None,
              prediction_latency: int = None, interpretability_level: InterpretabilityLevel = None,
              timeout: int = 7200, cost_matrix_weights: List[List[str]] = None, train_size: float = None,
              test_size: float = None, validation_size: float = None, fold_size: int = None, n_folds: int = None,
              horizon: int = None, validation_strategy: ValidationStrategy = None, cv_strategy: CVStrategy = None,
              forecast_horizon: int = None, model_life_time: int = None, refit_on_all: bool = None, wait: bool = False,
              skip_if_exists: bool = False, api_key: str = None) -> FireflyResponse:
        """
        Creates and runs a training task.

        A task is responsible for searching hyper-parameters that maximize model scores.
        The task constructs ensembles made of select models. Seeking ways to combine different models, allows optimal decision making.
        Similar to calling `fireflyai.Task.create(...)`.

        Args:
            task_name (str): Task name.
            dataset_id (int): Dataset ID.
            estimators (List[Estimator]): Estimators to use in the train task.
            target_metric (TargetMetric): The target metric, is the metric, the search process attempts to optimize.
            splitting_strategy (SplittingStrategy): Splitting strategy for the data.
            notes (Optional[str]): Notes of the task.
            ensemble_size (Optional[int]): Maximum number of models in an ensemble.
            max_models_num (Optional[int]): Maximum number of models to train during search process.
            single_model_timeout (Optional[int]): Maximum time for training one model.
            pipeline (Optional[List[Pipeline]): Pipeline steps to use in the train task.
            prediction_latency (Optional[int]): Maximum number of seconds ensemble prediction should take.
            interpretability_level (Optional[InterpretabilityLevel]): Determines how interpretable your ensemble is.
            timeout (Optional[int]): Timeout, in seconds, for the search process (default: 2 hours).
            cost_matrix_weights (Optional[List[List[str]]]): For classification and anomaly detection problems,
                the weights determine custom cost metric. This assigns different weights to the entries of the confusion matrix.
            train_size (Optional[int]): The ratio of data taken for the train set of the model.
            test_size (Optional[int]): The ratio of data taken for the test set of the model.
            validation_size (Optional[int]): The ratio of data taken for the validation set of the model.
            fold_size (Optional[int]): Fold size when performing cross-validation splitting.
            n_folds (Optional[int]): Number of folds when performing cross-validation splitting.
            validation_strategy (Optional[ValidationStrategy]): Validation strategy used for the train task.
            cv_strategy (Optional[CVStrategy]): Cross-validation strategy to use for the train task.
            horizon (Optional[int]): Something related to time-series models. #TODO
            forecast_horizon (Optional[int]): Something related to time-series models. #TODO
            model_life_time (Optional[int]): Something related to time-series models. #TODO
            refit_on_all (Optional[bool]): Determines if the final ensemble will be refit on all data after
                search process is done.
            wait (Optional[bool]): Should the call be synchronous or not.
            skip_if_exists (Optional[bool]): Check if a Dataset with same name exists and skip if true.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: Task ID, if successful and wait=False or Task if successful and wait=True;
            raises FireflyError otherwise.
        """
        return fireflyai.Task.create(name=task_name, dataset_id=dataset_id, estimators=estimators,
                                     target_metric=target_metric, splitting_strategy=splitting_strategy, notes=notes,
                                     ensemble_size=ensemble_size, max_models_num=max_models_num,
                                     single_model_timeout=single_model_timeout, pipeline=pipeline,
                                     prediction_latency=prediction_latency,
                                     interpretability_level=interpretability_level, timeout=timeout,
                                     cost_matrix_weights=cost_matrix_weights, train_size=train_size,
                                     test_size=test_size, validation_size=validation_size, fold_size=fold_size,
                                     n_folds=n_folds, validation_strategy=validation_strategy, cv_strategy=cv_strategy,
                                     horizon=horizon, forecast_horizon=forecast_horizon,
                                     model_life_time=model_life_time, refit_on_all=refit_on_all, wait=wait,
                                     skip_if_exists=skip_if_exists, api_key=api_key)

    @classmethod
    def get_available_estimators(cls, id: int, inter_level: InterpretabilityLevel = None,
                                 api_key: str = None) -> FireflyResponse:
        """
        Gets possible Estimators for a specific Dataset.

        Args:
            id (int): Dataset ID.
            inter_level (Optional[InterpretabilityLevel]): Interpretability level.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: List of possible values for estimators.
        """
        return cls._get_available_configuration_options(id=id, inter_level=inter_level, api_key=api_key)['estimators']

    @classmethod
    def get_available_pipeline(cls, id: int, inter_level: InterpretabilityLevel = None,
                               api_key: str = None) -> FireflyResponse:
        """
        Gets possible pipeline for a specific dataset.

        Args:
            id (int): Dataset ID to get possible pipeline.
            inter_level (Optional[InterpretabilityLevel]): Interpretability level.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: List of possible values for pipeline.
        """
        return cls._get_available_configuration_options(id=id, inter_level=inter_level, api_key=api_key)['pipeline']

    @classmethod
    def get_available_splitting_strategy(cls, id: int, inter_level: InterpretabilityLevel = None,
                                         api_key: str = None) -> FireflyResponse:
        """
        Gets possible splitting strategies for a specific dataset.

        Args:
            id (int): Dataset ID to get possible splitting strategies.
            inter_level (Optional[InterpretabilityLevel]): Interpretability level.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: List of possible values for splitting strategies.
        """
        return cls._get_available_configuration_options(id=id, inter_level=inter_level, api_key=api_key)[
            'splitting_strategy']

    @classmethod
    def get_available_target_metric(cls, id: int, inter_level: InterpretabilityLevel = None,
                                    api_key: str = None) -> FireflyResponse:
        """
        Gets possible target metrics for a specific dataset.

        Args:
            id (int): Dataset ID to get possible target metrics.
            inter_level (Optional[InterpretabilityLevel]): Interpretability level.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: List of possible values for target metrics.
        """
        return cls._get_available_configuration_options(id=id, inter_level=inter_level, api_key=api_key)[
            'target_metric']

    @classmethod
    def _get_available_configuration_options(cls, id: int, inter_level: InterpretabilityLevel = None,
                                             api_key: str = None) -> FireflyResponse:
        inter_level = inter_level.value if inter_level is not None else None
        requestor = APIRequestor()
        url = "tasks/configuration/options"
        response = requestor.get(url=url, params={'dataset_id': id, 'interpretable': inter_level}, api_key=api_key)
        new_data = {
            'estimators': [Estimator(e) for e in response['estimators']],
            'target_metric': [TargetMetric(e) for e in response['target_metric']],
            'splitting_strategy': [SplittingStrategy(e) for e in response['splitting_strategy']],
            'pipeline': [Pipeline(e) for e in response['pipeline']],
        }
        return FireflyResponse(data=new_data)
