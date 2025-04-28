from .gcloud import GCLOUD as gcloud
from google.cloud import secretmanager
from pyconfighelper import ConfigHelper
from google.api_core.exceptions import NotFound, FailedPrecondition


class Config():
    cache = {}

    @classmethod
    def get_secret_version_path(cls, env, secret, secret_version='latest'):
        '''
            v1 libraries contain a method called `secret_version_path`.  Google since
            removed it from their repo and recommends us to formulate the string directly.
            Please refer to the Readme.md docs for more information:
            https://github.com/googleapis/python-secret-manager/blob/41960323415701f3b358be201857fe04f58652be/UPGRADING.md
        '''
        project = gcloud.project(env)

        if 'projects/' in secret:
            if '/secrets/' in secret:
                if '/versions/' in secret:
                    secret_version_path = secret
                else:
                    secret_version_path = f'{secret}/versions/{secret_version}'
        else:
            secret_version_path = f'projects/{project}/secrets/{secret}/versions/{secret_version}'

        return secret_version_path

    @classmethod
    def get_config(cls, env):
        helper = ConfigHelper(
            kms_project=gcloud.project(env),
            kms_location='us-west3',
            kms_key_ring='configs',
            kms_key_name='kek-{}1'.format(env[:1])
        )
        url = 'REPLACE-URL/{}'
        url = url.format(gcloud.env(env).replace('-', '/'))
        secret = cls.secret(env, 'github')
        return helper.get_config(url, secret)

    @classmethod
    def secret(cls, env, secret_name):
        name = cls.get_secret_version_path(env, secret_name)
        response = cls.access_secret_version(env, name)
        return response.payload.data.decode('UTF-8')

    @classmethod
    def create_secret(cls, env, secret_name):
        client = secretmanager.SecretManagerServiceClient()
        parent = f'projects/{gcloud.project(env)}'
        client.create_secret(
            request={
                'parent': parent,
                'secret_id': secret_name,
                'secret': {'replication': {'automatic': {}}},
            }
        )

    @classmethod
    def add_secret_version(cls, env, secret_name, payload):
        path = f'projects/{gcloud.project(env)}/secrets/{secret_name}'
        client = secretmanager.SecretManagerServiceClient()
        client.add_secret_version(
            request={'parent': path, 'payload': {'data': payload.encode('UTF-8')}})

    @classmethod
    def disable_secret_version(cls, env, secret_name, version):
        path = cls.get_secret_version_path(env, secret_name, version)
        client = secretmanager.SecretManagerServiceClient()
        client.disable_secret_version(
            request={'name': path}
        )

    @classmethod
    def list_secret_versions(cls, env, secret_name):
        path = f'projects/{gcloud.project(env)}/secrets/{secret_name}'
        client = secretmanager.SecretManagerServiceClient()
        return client.list_secret_versions(
            request={'parent': path}
        )

    @classmethod
    def list_secret_keys(cls, env, secret_name):
        keys = []
        for secret in cls.list_secret_versions(env, secret_name):
            key = cls.access_secret_version(env, secret.name)
            if key:
                keys.append(key.payload.data.decode())
        return keys

    @classmethod
    def access_secret_version(cls, env, secret_name, version=None):
        path = cls.get_secret_version_path(env, secret_name, version)
        client = secretmanager.SecretManagerServiceClient()
        try:
            return client.access_secret_version(
                request={'name': path}
            )

        except FailedPrecondition as e:
            print('Unable to access key.  Key might be destroyed or expired.', e)

    @classmethod
    def add_secret(cls, env, secret_name, payload):
        try:
            print(f'Creating new version for {secret_name}')
            cls.add_secret_version(env, secret_name, payload)
        except NotFound:
            print(f'Secret {secret_name} was not found. Creating Secret.')
            cls.create_secret(env, secret_name)
            print(f'Creating new version for {secret_name}')
            cls.add_secret_version(env, secret_name, payload)

    def get_cache(self, env):
        return self.cache.get(env, None)

    def set_cache(self, env, config):
        self.cache[env] = config

    def clear_cache(self):
        self.cache = {}
