from firefly.errors import *


class UMMixin(abc.ABC):

    def login(self, username, password, tnc=None):
        api = 'login'
        return self.post(query=api, data={'username': username, 'password': password, 'tnc': tnc}, query_prefix='')

    def authenticate(self, jwt):
        api = 'authenticate'
        return self.get(api, params={'jwt': jwt})

    def list_accounts(self):
        api = 'accounts'
        return self.get(api, params={'jwt': self.token}, query_prefix='')
