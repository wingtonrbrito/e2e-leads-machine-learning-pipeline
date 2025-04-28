import subprocess

from googleapiclient.discovery import build


class GCLOUD():
    """
    Utility class for gcloud related stuff like reading the local config file or determining
    project name.

    @author Noah Goodrich
    @date 3/5/2020
    """
    conf = None
    proj = None

    @classmethod
    def config(cls):
        if cls.conf is None:
            """
            Reads the local gcloud current config and stores in a dict for use.
            """
            rs = subprocess.run(['gcloud', 'config', 'list'], capture_output=True, text=True)
            lines = rs.stdout.splitlines()
            cls.conf = {}
            for ln in lines:
                i = ln.split(' ')
                if len(i) == 3:
                    cls.conf[i[0]] = i[2]
            account = cls.conf['account'].split('.')
            cls.conf['username'] = account[0][0] + account[1].split('@')[0]
        return cls.conf

    @classmethod
    def env(cls, env):
        if env == 'local':
            return 'local-{}'.format(cls.config()['username'])
        return env

    @classmethod
    def project(cls, env):
        """
        Determines project based on environment.

        For local, a username is appended so each developer can have their own.
        """
        if env is None:
            env = 'local'
        cls.proj = 'ml-{}'.format(cls.env(env))
        return cls.proj

    @classmethod
    def parse_table_name(cls, str):
        """
        Returns a the table name from a fully qualified `dataset.tablename` or `project:dataset.tablename`.
        """
        splits = str.split('.')
        return splits[len(splits)-1]


class GoogleCloudServiceFactory():
    instances = {}

    services_version = {
        'dataflow': 'v1b3'
    }

    @classmethod
    def build(cls, service, requestBuilder=None, http=None):
        """
        Factory wrapper for build function so we only have to authenticate to any given service once.
        """
        if service not in cls.instances:
            # Passing a None value in place of RequestBuilder will throw an exception. Stu M. 4/27/20
            if requestBuilder is not None:
                cls.instances[service] = build(
                    service,
                    cls.services_version[service],
                    requestBuilder=requestBuilder,
                    http=http,
                    cache_discovery=False)
            else:
                cls.instances[service] = build(service, cls.services_version[service], cache_discovery=False)

        return cls.instances[service]
