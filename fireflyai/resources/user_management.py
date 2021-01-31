"""
Wisdom is the user interaction with Whatify wisdom, it contains

"""
# Prediction is the way to productize the Ensemble created in the previous steps. Once an Ensemble is created,
# users can upload additional Datasources that may be used for predictions.
#
# ‘Prediction’ API includes querying of predictions (Get, List and Delete) and creating a Prediction to get predictions
# on existing Ensembles and uploaded Datasources.
import logging
from typing import Dict
import fireflyai
from fireflyai.token_json import UserToken
from fireflyai.api_requestor import APIRequestor
from fireflyai.firefly_response import FireflyResponse
from fireflyai.resources.api_resource import APIResource


class UserManagement(APIResource):
    _CLASS_PREFIX = ''

    @classmethod
    def list(cls, url: str='users/list', search_term: str = None, page: int = None, page_size: int = None, sort: Dict = None,
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
            :param url: the API url of users\accounts

        Returns:
            FireflyResponse: users are represented as nested dictionaries under `hits`.
        """
        return cls._list(url=url, search_term=search_term, page=page, page_size=page_size, sort=sort, filter_=filter_, api_key=api_key)

    @classmethod
    def login(cls, email: str, password: str) -> FireflyResponse:
        """
        Authenticates user and stores temporary token in `fireflyai.token`.

        Other modules automatically detect if a token exists and use it, unless a user specifically provides a token
        for a specific request.
        The token is valid for a 24-hour period, after which this method needs to be called again in order to generate
        a new token.

        Args:
            email (str): email.
            password (str): Password.

        Returns:
            FireflyResponse: Empty FireflyResponse if successful, raises FireflyError otherwise.
        """
        logging.info(' '.join(['login with user:', str(email)]))
        url = 'login'
        requestor = APIRequestor()
        try:
            response = requestor.post(url, body={'username': email, 'password': password, 'tnc': None}, api_key="")
            fireflyai.token = response['token']
            my_token = UserToken(fireflyai.token)
            logging.info(' '.join(['user mail:', str(email), '- Login successful, User ID: ',
                                   str(my_token.get_user_id()), ' Account ID: ' + str(my_token.get_account_id())]))
        except Exception as ex:
            logging.warning(' '.join(['user mail:', str(email), '- Login FAILED']))
            raise ex

        return FireflyResponse(status_code=response.status_code, headers=response.headers)

    @classmethod
    def impersonate(cls, user_id: str = None, admin_token: str = None) -> FireflyResponse:
        """
       impersonate user and stores temporary token in `fireflyai.token`

       Args:
           user_id (str): user ID.
           email (str): User email.
           admin_token (str): Admin user token.

       Returns:
           FireflyResponse: Empty FireflyResponse if successful, raises FireflyError otherwise.
       """
        logging.info(' '.join(['impersonate to user_ID:', str(user_id)]))
        url = ''.join(['users/login_as/', str(user_id)])
        requester = APIRequestor()
        try:
            response = requester.post(url, api_key=admin_token)
            fireflyai.token = response['result']
            my_token = UserToken(fireflyai.token)
            logging.info(' '.join(['user ID:', str(user_id), '- Login successful with Account ID: ' + str(my_token.get_account_id())]))
        except Exception as ex:
            logging.warning(' '.join(['user ID:', str(user_id), '- Login Failed']))
            raise ex

        return FireflyResponse(status_code=response.status_code, headers=response.headers)

    @classmethod
    def get_user_list(cls, username: str, admin_token: str = None) -> FireflyResponse:
        ret_val = {}
        resp = cls.list(filter_={'username': [username]}, api_key=admin_token)
        if resp and 'hits' in resp and len(resp['hits']) == 1:
            ret_val = resp['hits'][0]
        return ret_val

    @classmethod
    def get_account_list(cls, name: str, admin_token: str = None) -> FireflyResponse:
        ret_val = {}
        resp = cls.list(url='accounts', filter_={'name': [name]}, api_key=admin_token)
        if resp and 'result' in resp and len(resp['result']) == 1:
            ret_val = resp['result'][0]
        return ret_val

    @classmethod
    def get_user_id(cls, email: str = None):
        user_id = -1
        if email:
            user = cls.get_user_list(username=email)
            if user:
                user_id = user['user_id']
        return user_id

    @classmethod
    def get_account_id(cls, email: str = None):
        account_id = -1
        if email:
            account = cls.get_account_list(name=email)
            if account:
                account_id = account['id']
        return account_id


