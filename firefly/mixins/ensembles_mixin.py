from typing import Dict, Union, List

from firefly.errors import *


class EnsemblesMixin(abc.ABC):

    def get_model_sensitivity_report(self, ensemble_id: int) -> Dict:
        """
        Gets the sensitivity report for a specific ensemble.

        Contains each feature sensitivity score for missing values and feature value.

        Args:
            ensemble_id (int): Ensemble ID.

        Returns:
            Dict: Score for each feature in every sensitivity test.
        """
        api = '{ensemble_id}/sensitivity'
        result = self.get(api.format(ensemble_id=ensemble_id), params={'jwt': self.token}, query_prefix='ensembles')
        self.__cleanup_report(result)
        return result

    # TODO: what is this?
    def get_threshold_endpoints_report(self, ensemble_id):
        api = '{ensemble_id}/threshold_endpoints'
        return self.get(api.format(ensemble_id=ensemble_id), params={'jwt': self.token}, query_prefix='ensembles')

    # TODO: needs more information
    def list_ensembles(self, page: int = None, page_size: int = None, sort: Dict[str, Union[str, int]] = None,
                       filter: Dict[str, Union[str, int]] = None) -> Dict:
        """
        List the existing tasks. Supports filtering, sorting and pagination.

        Args:
            search_term (Optional[str]): Return only records that contain the search_term in one of their fields.
            page (Optional[int]): For pagination, which page to return.
            page_size (Optional[int]): For pagination, how many records will appear in a single page.
            sort (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to sort the results by.
            filter (Optional[Dict[str, Union[str, int]]]): Dictionary of rules to filter the results by.

        Returns:
            Dict: User's tasks and ensembles.
        """
        api = ''
        filters = self.parse_filter_parameters(filter)
        sorts = self.parse_sort_parameters(sort)
        return self.get(api,
                        params={'jwt': self.token, 'page': page, 'page_size': page_size, 'sort': sorts,
                                'filter': filters},
                        query_prefix='ensembles')

    def get_ensemble_details(self, ensemble_id: int) -> Dict:
        """
        Returns metadata regarding a specific model.

        Metadata includes information regarding the task which created the model and the train dataset.

        Args:
            ensemble_id (int): Ensemble ID.

        Returns:
            Dict: Metadata regarding a specific model.
        """
        api = '{ensemble_id}'
        return self.get(api.format(ensemble_id=ensemble_id), params={'jwt': self.token}, query_prefix='ensembles')

    def edit_ensemble_notes(self, ensemble_id: int, notes: str) -> str:
        """
        Edits an ensemble's notes.

        Args:
            ensemble_id (int): Ensemble ID.
            notes (str): Notes.

        Returns:
            str: "submitted" if operation was successful, raises FireflyClientError otherwise.
        """
        api = '{ensemble_id}/notes'
        notes = {'notes': notes}
        return self.put(api.format(ensemble_id=ensemble_id), data=notes, params={'jwt': self.token},
                        query_prefix='ensembles')

    def delete_ensemble(self, ensemble_id: int) -> str:
        """
        Deletes an ensemble.

        Args:
            ensemble_id (int): Ensemble ID.

        Returns:
            str: "done" if operation was successful, raises FireflyClientError otherwise.
        """
        api = '{ensemble_id}'
        return self.delete(query=api.format(ensemble_id=ensemble_id), params={'jwt': self.token},
                           query_prefix='ensembles')

    # TODO: fill in
    def calc_model_sensitivity(self, ensemble_id: int):
        """
        ???

        Args:
            ensemble_id (int): Ensemble ID.

        Returns:
            ???
        """
        api = '{ensemble_id}/sensitivity'
        return self.post(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})

    # TODO: fill in
    def calc_threshold_endpoints(self, ensemble_id):
        """
        ???

        Args:
            ensemble_id (int): Ensemble ID.

        Returns:
            ???
        """
        api = '{ensemble_id}/threshold_endpoints'
        return self.post(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})

    # TODO: more details
    def get_ensemble_summary_report(self, ensemble_id: int) -> List:
        """
        Gets ensemble's summary report.

        Args:
            ensemble_id (int): Ensemble ID.

        Returns:
            List: Summary report.
        """
        api = '{ensemble_id}/summary'
        task_id = self.get(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})
        return task_id

    # TODO: more details
    def get_ensemble_roc_curve(self, ensemble_id: int) -> Dict:
        """
        Gets ensemble's ROC curve data.

        Args:
            ensemble_id (int): Ensemble ID.

        Returns:
            Dict: ROC curve data.
        """
        api = '{ensemble_id}/roc_curve'
        params = {'jwt': self.token}
        performance = self.get(api.format(ensemble_id=ensemble_id), params=params, query_prefix='ensembles')
        return performance

    # TODO: more details
    def get_ensemble_confusion_matrix(self, ensemble_id: int) -> List:
        """
        Get ensemble's confusion matrix.

        Args:
            ensemble_id (int): Ensemble ID.

        Returns:
            Dict: Confusion matrix.
        """
        api = '{ensemble_id}/confusion'
        params = {'jwt': self.token}
        confusion = self.get(api.format(ensemble_id=ensemble_id), params=params, query_prefix='ensembles')
        return confusion

    # TODO: more details
    def get_ensemble_test_prediction_sample(self, ensemble_id: int) -> Dict:
        """
        Get ensemble's prediction samples.

        Args:
            ensemble_id (int): Ensemble ID.

        Returns:
            List: Prediction samples.
        """
        api = '{ensemble_id}/test_prediction_sample'
        params = {'jwt': self.token}
        scatter_plot = self.get(api.format(ensemble_id=ensemble_id), params=params, query_prefix='ensembles')
        return scatter_plot

    # TODO: more details
    def get_model_architecture(self, ensemble_id: int) -> Dict:
        """
        Get ensemble's architecture.

        Args:
            ensemble_id (int): Ensemble ID.

        Returns:
            Dict: Architecture.
        """
        api = '{ensemble_id}/architecture'
        rep = self.get(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})
        return rep

    # TODO: more details
    def get_model_presentation(self, ensemble_id: int) -> Dict:
        """
        Get ensemble's presentation.

        Args:
            ensemble_id (int): Ensemble ID.

        Returns:
            Dict: Ensemble's presentation.
        """
        api = '{ensemble_id}/presentation'
        rep = self.get(api.format(ensemble_id=ensemble_id), query_prefix='ensembles', params={'jwt': self.token})
        return rep

    def generate_export_download_link(self, ensemble_id: int) -> str:
        """
        Get a link to download the ensemble.

        Args:
            ensemble_id (int): Ensemble ID.

        Returns:
            str: URL with which to download a TAR file containing the model.
        """
        api = "{ensemble_id}/download"
        return self.post(api.format(ensemble_id=ensemble_id), params={'jwt': self.token}, query_prefix='ensembles')

    # TODO: what is this?
    def get_export_record(self, ensemble_id: int):
        api = "{ensemble_id}"
        return self.get(api.format(ensemble_id=ensemble_id), params={'jwt': self.token}, query_prefix='exports')

    # TODO: what is this?
    def create_export_record(self, ensemble_id):
        api = ""
        data = {'ensemble_id': ensemble_id}
        return self.post(api, params={'jwt': self.token}, query_prefix='exports', data=data)

    # TODO: what is this?
    def rerun_export(self, ensemble_id):
        api = "{ensemble_id}/rerun"
        return self.post(api.format(ensemble_id=ensemble_id), params={'jwt': self.token}, query_prefix='exports')

    def __cleanup_report(self, result):
        if result:
            if result.get('NA value', {}).get('_title'):
                result['NA value'].pop('_title')
            if result.get('NA value', {}).get('_description'):
                result['NA value'].pop('_description')
            if result.get('Permutation', {}).get('_title'):
                result['Permutation'].pop('_title')
            if result.get('Permutation', {}).get('_description'):
                result['Permutation'].pop('_description')
