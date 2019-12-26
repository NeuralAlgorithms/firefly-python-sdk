"""
An ensemble is the main entity that represents the result of a machine learning training process (done using the
‘task’ entity). An ensemble is a combination of one or more machine learning models optimized for the dataset and model
training configuration (target metric, selected algorithms etc.).

To be able to work with ensembles, users can invoke the Get List ensembles API, which is a part of the ‘tasks’ entity.

‘Ensembles’ APIs include querying existing ensembles (Get and List), but also the Refit ensemble which allows users to
keep their machine learning models updated. In the future explainability features such as ROC curve, Confusion matrix
and other tools will be available as well.
"""
from typing import Dict

from firefly.api_requestor import APIRequestor
from firefly.firefly_response import FireflyResponse
from firefly.resources.api_resource import APIResource


class Ensemble(APIResource):
    _CLASS_PREFIX = 'ensembles'

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
            filter (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Ensembles, which are represented as nested dictionaries under `hits`.
        """
        return cls._list(search_term, page, page_size, sort, filter_, api_key)

    @classmethod
    def get(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get basic information of a specific ensemble.

        Information includes the scores of the ensembles on various metrics

        Args:
            id (int): Ensemble ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Information about the ensemble.
        """
        return cls._get(id, api_key)

    @classmethod
    def delete(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Delete a specific ensemble.

        Args:
            id (int): Ensemble ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: "done" if deleted successfuly, raises FireflyClientError otherwise.
        """
        return cls._delete(id, api_key)

    @classmethod
    def edit_ensemble_notes(cls, id: int, notes: str, api_key: str = None) -> FireflyResponse:
        """
        Edits an ensemble's notes.

        Args:
            id (int): Ensemble ID.
            notes (str): Notes.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        requestor = APIRequestor()
        url = "{prefix}/{id}/notes".format(prefix=cls._CLASS_PREFIX, id=id)
        response = requestor.put(url=url, body={'notes': notes}, api_key=api_key)
        return response

    @classmethod
    def get_model_sensitivity_report(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Gets the sensitivity report for a specific ensemble.

        Contains each feature sensitivity score for missing values and feature value.

        Args:
            id (int): Ensemble ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Score for each feature in every sensitivity test.
        """
        requestor = APIRequestor()
        url = "{prefix}/{id}/sensitivity".format(prefix=cls._CLASS_PREFIX, id=id)
        response = requestor.get(url=url, api_key=api_key)
        result = response.to_dict()
        cls.__cleanup_report(result)
        return FireflyResponse(data=result)

    @classmethod
    def get_ensemble_test_prediction_sample(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get ensemble's prediction samples.

        Args:
            id (int): Ensemble ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Prediction samples.
        """
        requestor = APIRequestor()
        url = "{prefix}/{id}/test_prediction_sample".format(prefix=cls._CLASS_PREFIX, id=id)
        response = requestor.get(url=url, api_key=api_key)
        return response

    @classmethod
    def get_ensemble_summary_report(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Gets ensemble's summary report.

        Args:
            id (int): Ensemble ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Summary report.
        """
        requestor = APIRequestor()
        url = "{prefix}/{id}/summary".format(prefix=cls._CLASS_PREFIX, id=id)
        response = requestor.get(url=url, api_key=api_key)
        return response

    @classmethod
    def get_ensemble_roc_curve(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Gets ensemble's ROC curve data.

        Args:
            id (int): Ensemble ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: ROC curve data.
        """
        requestor = APIRequestor()
        url = "{prefix}/{id}/roc_curve".format(prefix=cls._CLASS_PREFIX, id=id)
        response = requestor.get(url=url, api_key=api_key)
        return response

    @classmethod
    def get_ensemble_confusion_matrix(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get ensemble's confusion matrix.

        Args:
            id (int): Ensemble ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Confusion matrix.
        """
        requestor = APIRequestor()
        url = "{prefix}/{id}/confusion".format(prefix=cls._CLASS_PREFIX, id=id)
        response = requestor.get(url=url, api_key=api_key)
        return response

    @classmethod
    def get_model_architecture(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get ensemble's architecture.

        Args:
            id (int): Ensemble ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Architecture.
        """
        requestor = APIRequestor()
        url = "{prefix}/{id}/architecture".format(prefix=cls._CLASS_PREFIX, id=id)
        response = requestor.get(url=url, api_key=api_key)
        return response

    @classmethod
    def get_model_presentation(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get ensemble's presentation.

        Args:
            id (int): Ensemble ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Ensemble's presentation.
        """
        requestor = APIRequestor()
        url = "{prefix}/{id}/presentation".format(prefix=cls._CLASS_PREFIX, id=id)
        response = requestor.get(url=url, api_key=api_key)
        return response

    @classmethod
    def generate_export_download_link(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get a link to download the ensemble.

        Args:
            id (int): Ensemble ID.
            api_key (Optional[str]): Explicit api_key, not required if `firefly.authenticate` was run beforehand.

        Returns:
            FireflyResponse: URL with which to download a TAR file containing the model.
        """
        requestor = APIRequestor()
        url = "{prefix}/{id}/download".format(prefix=cls._CLASS_PREFIX, id=id)
        response = requestor.post(url=url, api_key=api_key)
        return response

    @classmethod
    def __cleanup_report(cls, result):
        if result:
            if result.get('NA value', {}).get('_title'):
                result['NA value'].pop('_title')
            if result.get('NA value', {}).get('_description'):
                result['NA value'].pop('_description')
            if result.get('Permutation', {}).get('_title'):
                result['Permutation'].pop('_title')
            if result.get('Permutation', {}).get('_description'):
                result['Permutation'].pop('_description')
