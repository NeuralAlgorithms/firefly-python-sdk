"""
Prediction is the way to productize the Ensemble created in the previous steps. Once an Ensemble is created,
users can upload additional Datasources that may be used for predictions.

‘Prediction’ API includes querying of predictions (Get, List and Delete) and creating a Prediction to get predictions
on existing Ensembles and uploaded Datasources.
"""
import os
from typing import Dict, List

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
        List the existing Predictions - supports filtering, sorting and pagination.

        Args:
            search_term (Optional[str]): Return only records that contain the `search_term` in any field.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules  to sort the results by.
            filter_ (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: Predictions are represented as nested dictionaries under `hits`.
        """
        return cls._list(search_term, page, page_size, sort, filter_, api_key)

    @classmethod
    def get(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Get information on a specific Prediction.

        Information includes the state of the Prediction and other attributes.

        Args:
            id (int): Prediction ID.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Information about the Prediction.
        """
        return cls._get(id, api_key)

    @classmethod
    def delete(cls, id: int, api_key: str = None) -> FireflyResponse:
        """
        Deletes a specific Prediction.

        Args:
            id (int): Prediction ID.
            api_key (Optional[str]): Explicit `api_key`, not required, if `fireflyai.authenticate()` was run prior.

        Returns:
            FireflyResponse: "true" if deleted successfuly, raises FireflyClientError otherwise.
        """
        return cls._delete(id, api_key)

    @classmethod
    def create(cls, ensemble_id: int, data_id: int = None, file_path: str = None, download_details: Dict = None,
               remove_header: bool = False,
               data_name: str = None, header: List = None, wait: bool = None, api_key: str = None) -> FireflyResponse:
        """
        Create a prediction from a given ensemble and prediction datasource.

        The prediction datasource should include all the of original features, without the target column (unless the
        ensemble belongs to a timeseries task).
        The prediction uses the ensemble to produce the prediction's results file.

        Args:
            ensemble_id (int): Ensemble to use for the prediction.
            data_id (int): Datasource to run the prediction on.
            wait (Optional[bool]): Should the call be synchronous or not.
            api_key (Optional[str]): Explicit api_key, not required if `fireflyai.authenticate` was run prior.

        Returns:
            FireflyResponse: Prediction ID, if successful and wait=False or Prediction if successful and wait=True;
            raises FireflyError otherwise.
        """
        data_name = data_name or os.path.basename(file_path) if file_path else None
        data = {
            "ensemble_id": ensemble_id,
            "datasource_id": data_id,
            "header": header,
            "data_name": data_name,
            "file_path": file_path,
            "remove_header": remove_header,
        }
        if download_details:
            data['download_details'] = download_details
        requestor = APIRequestor()
        response = requestor.post(url=cls._CLASS_PREFIX, body=data, api_key=api_key)
        id = response['id']
        if wait:
            utils.wait_for_finite_state(cls.get, id, state_field='stage', api_key=api_key)
            response = cls.get(id, api_key=api_key)
        else:
            response = FireflyResponse(data={'id': id})

        return response
