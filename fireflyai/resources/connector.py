"""
Wisdom is the user interaction with Whatify wisdom, it contains

"""
# Prediction is the way to productize the Ensemble created in the previous steps. Once an Ensemble is created,
# users can upload additional Datasources that may be used for predictions.
#
# ‘Prediction’ API includes querying of predictions (Get, List and Delete) and creating a Prediction to get predictions
# on existing Ensembles and uploaded Datasources.
import os
from io import StringIO
import boto3
import pandas as pd
import s3fs
import fireflyai
from fireflyai.api_requestor import APIRequestor
from fireflyai.resources.api_resource import APIResource


class Connector(APIResource):
    _CLASS_PREFIX = 'connectors'
    credentials = None
    s3_path = None
    s3_client = None
    s3_resource = None

    @classmethod
    def get_credentials(cls, user_token: str = None, renew: bool = False):
        """
        get user token and store it in `fireflyai.token`

       Args:
            user_token (str): user token
            renew (bool): if need to renew the token

       Returns:
           FireflyResponse: the user token
        """
        if (cls.credentials is None) or renew:
            url = '/'.join([cls._CLASS_PREFIX, 'whatify_connect_permissions'])
            requester = APIRequestor()
            if user_token is None:
                user_token = fireflyai.token
            cls.credentials = requester.get(url, params={'jwt': user_token})
        return cls.credentials

    @classmethod
    def get_s3_path(cls, user_token: str = None):
        """
        get S3 path for relevant user token

       Args:
            user_token (str): user token

       Returns:
           String: the user path in S3
        """
        cls.get_credentials(user_token)
        if cls.s3_path is None:
            bucket = cls.credentials['bucket']
            PATH = cls.credentials['path']
            cls.s3_path = f's3://{bucket}/{PATH}/'
        return cls.s3_path

    @classmethod
    def get_client_path(cls, path: bool = False, user_token: str = None):
        """
        get client files in path for relevant user token

       Args:
           path (bool): if we want to see the path in the start of each file
           user_token (str): user token

       Returns:
           String: the user path in S3
        """
        file_list = []
        cls.get_credentials(user_token)
        os.environ['AWS_ACCESS_KEY_ID'] = cls.credentials['access_key']
        os.environ['AWS_SECRET_ACCESS_KEY'] = cls.credentials['secret_key']
        os.environ['AWS_SESSION_TOKEN'] = cls.credentials['session_token']
        os.environ['AWS_DEFAULT_REGION'] = cls.credentials['region']
        fs = s3fs.S3FileSystem()
        try:
            file_list = fs.ls(cls.get_s3_path(user_token=user_token), refresh=True)
            if not path:
                if len(file_list) > 0:
                    # remove first item - path location
                    pre_file = cls.get_s3_path()
                    pre_path = pre_file.split('//')[1]
                    # pre_path = file_list.pop(0)
                    # remove path from list
                    file_list = {x.replace(str(pre_path), '') for x in file_list}

        except PermissionError as ex:
            print(' '.join(['folder is empty:', str(ex)]))
        return file_list

    @classmethod
    def get_s3_boto_client(cls, user_token: str = None):
        cls.get_credentials(user_token)
        cls.s3_client = boto3.client('s3',
                                     aws_access_key_id=cls.credentials['access_key'],
                                     aws_secret_access_key=cls.credentials['secret_key'],
                                     aws_session_token=cls.credentials['session_token'])
        return cls.s3_client

    @classmethod
    def get_s3_boto_resource(cls, user_token: str = None):
        cls.get_credentials(user_token)
        cls.s3_resource = boto3.resource('s3',
                                         aws_access_key_id=cls.credentials['access_key'],
                                         aws_secret_access_key=cls.credentials['secret_key'],
                                         aws_session_token=cls.credentials['session_token'])
        return cls.s3_resource

    @classmethod
    def read_to_pandas_df(cls, file_name: str, user_token: str = None, **kwargs):
        cls.get_s3_boto_client(user_token=user_token)
        obj = cls.s3_client.get_object(Bucket=cls.credentials['bucket'], Key='/'.join([cls.credentials['path'], file_name]))
        df = pd.read_csv(obj['Body'], **kwargs)
        return df

    @classmethod
    def write_pandas_df_to_s3(cls, df, file_name: str, user_token: str = None, **kwargs):
        cls.get_s3_boto_resource(user_token=user_token)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, **kwargs)
        cls.s3_resource.Object(cls.credentials['bucket'], '/'.join([cls.credentials['path'], file_name])).put(Body=csv_buffer.getvalue())
