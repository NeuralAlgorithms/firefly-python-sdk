from firefly.errors import *


class UMMixin(abc.ABC):
    def login(self, username: str, password: str, tnc=None) -> str:
        """
        Performs a login with the server.

        Args:
            username (str): The username to login with.
            password (str): User's password as plain test.
            tnc (Optional[]): Should be removed!

        Returns:
            Token (JWT) as string if successful, raises FireflyClientError otherwise.
        """
        api = 'login'
        return self.post(query=api, data={'username': username, 'password': password, 'tnc': tnc}, query_prefix='')

    # problematic - return True if the token is valid but raises an exception if not
    def authenticate(self, jwt: str) -> bool:
        """
        Authenticates the JWT with the server.

        Args:
            jwt (str): User's JWT as a string.

        Returns:
            True if token is authenticated, raises FireflyClientError otherwise,s
        """
        api = 'authenticate'
        return self.get(api, params={'jwt': jwt}, query_prefix='users')

    # admin method? probably shouldn't be here right now
    def list_accounts(self):
        api = 'accounts'
        return self.get(api, params={'jwt': self.token}, query_prefix='')
