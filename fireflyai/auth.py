import fireflyai
from fireflyai.api_requestor import APIRequestor
from fireflyai.firefly_response import FireflyResponse


def authenticate(username, password):
    url = 'login'

    requestor = APIRequestor()
    response = requestor.post(url, body={'username': username, 'password': password, 'tnc': None}, api_key="")
    return FireflyResponse(status_code=response.status_code, headers=response.headers)
