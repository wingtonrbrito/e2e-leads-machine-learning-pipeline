import os
import jwt
import time
from flask_restful_swagger_3 import Resource, swagger, Schema
from flask_restful import reqparse, abort
from flask import request
import hashlib
import secrets
from libs.config_singleton import ConfigSingleton


class AuthModel(Schema):
    type = 'object'
    properties = {
        'client-assertion-type': {
            'type': 'string'
        },
        'client-assertion': {
            'type': 'string'
        }
    }


class Auth(Resource):
    @swagger.doc({
        'tags': ['/auth'],
        'description': 'Get auth token',
        'requestBody': {
            'description': 'Be sure to have valid token.',
            'required': 'true',
            'content': {
                'application/json': {
                    'schema': AuthModel,
                    'examples':
                    {
                        'application/json': {
                            'summary': 'application/json',
                            'value': {
                                'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
                                'client_assertion': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.Bm8tu4m18oA96xwhBL8AV_4hRpIU6OrK5UaOmGqBEsk'  # noqa: E501
                            }
                        }
                    }
                }
            }
        },
        'responses': {
            '200': {
                'description': 'Ok'
            }
        }
    })
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('client_assertion_type', required=True)
        parser.add_argument('client_assertion', required=True)
        args = parser.parse_args()

        # Verify client_assertion_type value
        if args['client_assertion_type'] != 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer':
            abort(400, message='unrecognized client_assertion_type')

        # Get domain
        domain = request.host.split(':')[0]

        # Get client info
        client_id = None
        key = None
        try:
            client_id, key = self.get_auth_key(domain)
        except KeyError:
            abort(400, message='Invalid client request')

        # Decode token
        try:
            self.decode_token(args['client_assertion'], key)
        except jwt.exceptions.InvalidSignatureError:
            abort(400, message='Invalid Token Signature')

        # Generate access token
        access_token = None
        try:
            access_token = self.create_access_token(
                client_id,
                domain,
                ConfigSingleton.config_instance().secret(os.environ['ENV'], 'recommendation-engine-jwt'))
        except Exception:
            abort(500, message='Error creating access token')

        return access_token

    def decode_token(self, auth_token, secret):
        payload = jwt.decode(auth_token, secret, algorithms=['HS256'])
        return payload

    def create_access_token(self, client_id, domain, secret):
        payload = {
            'tok': 'access',
            'jit': self.create_jit(client_id),
            'sub': domain,
            'exp': time.time() + 600
        }
        return jwt.encode(payload, secret, algorithm='HS256').decode()

    def get_auth_key(self, domain):
        config = ConfigSingleton().get_config(os.environ['ENV']).get('api', {})

        try:
            client_key = config['domains'][domain]
            return client_key, config['api_keys'][client_key]['key']
        except KeyError:
            raise KeyError('Could not retrieve keys')

    def get_auth_client(self, domain):
        config = ConfigSingleton().get_config(os.environ['ENV']).get('api', {})
        try:
            client_key = config['domains'][domain]
            return config['api_keys'][client_key]['client']
        except KeyError:
            raise KeyError('Could not retrieve client')

    def create_jit(self, client_id):
        return hashlib.sha256(client_id.encode()).hexdigest()

    def get_domain(self, payload):
        return payload.get('sub', None)

    def validate_access_token_identity(self, host, payload):
        domain = payload.get('sub', None)
        jit = payload.get('jit', None)

        if domain != host.split(':')[0]:
            abort(405, message='Token identity error [1]')

        client_id, key = self.get_auth_key(domain)

        if jit != self.create_jit(client_id):
            abort(405, message='Token identity error [2]')

    def generate_secret():
        return secrets.token_hex(32)
