import json
import jwt


class UserToken:
    jwt = None
    token_json = None

    def __init__(self, token):
        if token:
            self.jwt = token
            self.token_json = jwt.decode(token, verify=False)
            self.token_json['udt'] = json.loads(self.token_json['udt'])

    def get_token(self):
        return self.jwt

    def get_user_id(self):
        return self.token_json['udt']['user_id']

    def get_account_id(self):
        return self.token_json['udt']['account']['id']


