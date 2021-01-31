import logging
import os
import time
from typing import Dict

import boto3

FINITE_STATES = ['AVAILABLE', 'CREATED', 'CANCELED', 'FAILED', 'COMPLETED', 'ABORTED']


def s3_upload(dataset, filename: str, aws_credentials: Dict):
    s3c = boto3.client('s3', region_name=aws_credentials['region'],
                       aws_access_key_id=aws_credentials['access_key'],
                       aws_secret_access_key=aws_credentials['secret_key'],
                       aws_session_token=aws_credentials['session_token'])
    s3c.upload_file(filename, aws_credentials['bucket'], os.path.join(aws_credentials['path'], dataset))


def s3_upload_stream(csv_buffer, filename, aws_credentials):
    session = boto3.Session(
        region_name=aws_credentials['region'],
        aws_access_key_id=aws_credentials['access_key'],
        aws_secret_access_key=aws_credentials['secret_key'],
        aws_session_token=aws_credentials['session_token'])
    s3_resource = session.resource('s3')
    s3_resource.Bucket(aws_credentials['bucket']).put_object(
        Key='{dir}/{filename}'.format(dir=aws_credentials['path'], filename=filename),
        Body=csv_buffer.getvalue()
    )


def wait_for_finite_state(getter, id, state_field='state', max_time=1800, **kwargs):
    res = getter(id, **kwargs)
    state = res[state_field]
    time_counter = 0
    while state not in FINITE_STATES:
        time.sleep(5)
        time_counter += 5
        res = getter(id, **kwargs)
        state = res[state_field]
        if time_counter > max_time:
            raise TimeoutError(' '.join(['TimeoutError - Failed to get FINITE_STATES in max_time =', str(max_time)]))
    logging.info(' '.join(['Done waiting, we got status: ', str(state)]))
