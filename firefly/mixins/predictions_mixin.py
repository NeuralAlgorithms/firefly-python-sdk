from typing import Dict, Union

from firefly.errors import *


class PredictionsMixin(abc.ABC):

    def predict(self, ensemble_id: int, data_id: int) -> int:
        """
        Performs prediction of a specified DataSource on a pecific Ensemble.

        Starts a task that runs on the server, to see the actual predictions use the returned `pred_id` and
        call the get_predict_record method.

        Args:
            ensemble_id (int): Ensemble to use for the prediction.
            data_id (int): Datasource to run the prediction on.

        Returns:
            int: ID for the Prediction task that has been created on the server.
        """
        api = ''
        data = {
            "ensemble_id": ensemble_id,
            "datasource_id": data_id,
        }
        return self.post(api.format(ensemble_id=ensemble_id), data=data, query_prefix='predictions',
                         params={'jwt': self.token})

    def get_predict_record(self, pred_id: int) -> Dict:
        """
        Get details on a Prediction batch.

        Returns a dict with metadata regarding the Prediction, e.g. run status, ensemble_id, data_id and
        results path if the task has completed its run.

        Args:
            pred_id (int):  Prediction ID to get status of.

        Returns:
            dict: Dictionary with metadata regarding the Prediction if it exists, raises FireflyClientError otherwise.
        """
        api = '{pred_id}'
        return self.get(api.format(pred_id=pred_id), query_prefix='predictions', params={'jwt': self.token})

    def delete_prediction(self, pred_id: int) -> str:
        """
        Deletes a Prediction batch from the server.

        Args:
            pred_id (int): OD of Prediction to delete.

        Returns:
            "done" (str) if completed successfully, raises FireflyClientError otherwise.
        """
        api = '{pred_id}'
        return self.delete(api.format(pred_id=pred_id), query_prefix='predictions', params={'jwt': self.token})

    def list_predictions(self, search_term: str = None, page: int = None, page_size: int = None,
                         sort: Dict[str, Union[str, int]] = None, filter: Dict[str, Union[str, int]] = None) -> Dict:
        """
        List all of the user's Predictions' metadata.

        Returns a list containing all Predictions even run by the user, as dictionaries in a list. Every dict contains
        metadata regarding the Prediction, same as returned with `get_predict_record`.

        Args:
            search_term (Optional[str]): Return only records that contain the search_term in one of their fields.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to sort the results by.
            filter (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.

        Returns:
            Dictionary containing the Predictions, which are represented as nested dictionaries.
        """
        api = ''
        filters = self.parse_filter_parameters(filter)
        sorts = self.parse_sort_parameters(sort)
        params = {'search_term': search_term, 'page': page, 'page_size': page_size,
                  'sort': sorts, 'filter': filters, 'jwt': self.token}
        pred_id = self.get(api, params=params, query_prefix='predictions')
        return pred_id
