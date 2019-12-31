"""
In the context of Firefly.ai's API, 'predictions' is the way to productize the machine learning ensemble created in the
previous steps. Once an ensemble is created, users can upload an additional datasource that will be used for performing
the predictions.

‘Predictions’ APIs includes Create prediction to get predictions for existing ensembles and the uploaded datasource,
as well as querying of predictions (Get, List and Delete).
"""
from typing import Dict

from fireflyai import utils

from fireflyai.api_requestor import APIRequestor
from fireflyai.firefly_response import FireflyResponse
from fireflyai.resources.api_resource import APIResource


class Prediction(APIResource):
    _CLASS_PREFIX = 'predictions'

    @classmethod
    def list(cls, search_term: str = None, page: int = None, page_size: int = None, sort: Dict = None,
             filter_: Dict = None, api_key: str = None) -> FireflyResponse:
        """
        List all of the user's Predictions' metadata.

        Returns a list containing all Predictions even run by the user, as dictionaries in a list. Every dict contains
        metadata regarding the Prediction, same as returned with `get_predict_record`.

        Args:
            search_term (Optional[str]): Return only records that contain the search_term in one of their fields.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to sort the results by.
            filter_ (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Predictions, which are represented as nested dictionaries under `hits`.
        """
        return cls._list(search_term, page, page_size, sort, filter_, api_key)

    @classmethod
    def get(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get details on a Prediction batch.

        Returns a dict with metadata regarding the Prediction, e.g. run status, ensemble_id, data_id and
        results path if the task has completed its run.

        Args:
            id (int): Prediction ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run beforehand.

        Returns:
            FireflyResponse: Information on the Prediction if it exists, raises FireflyClientError otherwise.
        """
        return cls._get(id, api_key)

    @classmethod
    def delete(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Deletes a Prediction batch from the server.

        Args:
            id (int): Prediction ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run beforehand.

        Returns:
            FireflyResponse: "done" if deleted successfuly, raises FireflyClientError otherwise.
        """
        return cls._delete(id, api_key)

    @classmethod
    def create(cls, ensemble_id: int, data_id: int, wait: bool = None, api_key: str = None) -> FireflyResponse:
        """
        Create a prediction from a given ensemble and prediction datasource.

        The prediction datasource should include all the of original features, without the target column (unless the
        ensemble belongs to a timeseries task).
        The prediction uses the ensemble to produce the prediction's results file.

        Args:
            ensemble_id (int): Ensemble to use for the prediction.
            data_id (int): Datasource to run the prediction on.
            wait (Optional[bool]): Should call be synchronous or not.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run beforehand.

        Returns:
            FireflyResponse: ID for the Prediction task that has been created on the server.
        """
        data = {
            "ensemble_id": ensemble_id,
            "datasource_id": data_id,
        }

        requestor = APIRequestor()
        response = requestor.post(url=cls._CLASS_PREFIX, body=data, api_key=api_key)
        id = response['id']
        if wait:
            utils.wait_for_finite_state(cls.get, id, api_key=api_key)
            response = cls.get(id, api_key=api_key)
        else:
            response = FireflyResponse(data={'id': id})

        return response
