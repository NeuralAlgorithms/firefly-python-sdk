import firefly
from firefly.api_requestor import APIRequestor
from firefly.firefly_response import FireflyResponse


def authenticate(username, password):
    url = 'login'

    requestor = APIRequestor()
    # url = requestor.build_query(api)
    response = requestor.post(url, body={'username': username, 'password': password, 'tnc': None}, api_key="")
    firefly.token = response['token']
    return FireflyResponse(data={'token': firefly.token})
