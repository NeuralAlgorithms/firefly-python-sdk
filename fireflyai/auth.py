import fireflyai
from fireflyai.api_requestor import APIRequestor
from fireflyai.firefly_response import FireflyResponse


def authenticate(username: str, password: str) -> FireflyResponse:
    """
    Authenticates user and stores temporary token in `fireflyai.token`.

    Other modules automatically detect if a token exists and use it, unless a user specifically provides a token
    for a specific request.
    The token is valid for a 24-hour period, after which this method needs to be called again in order to generate
    a new token.

    Args:
        username (str): Username.
        password (str): Password.

    Returns:
        FireflyResponse: Empty FireflyResponse if successful, raises FireflyError otherwise.
    """
    url = 'login'

    requestor = APIRequestor()
    response = requestor.post(url, body={'username': username, 'password': password, 'tnc': None}, api_key="")
    fireflyai.token = response['token']
    return FireflyResponse(status_code=response.status_code, headers=response.headers)
