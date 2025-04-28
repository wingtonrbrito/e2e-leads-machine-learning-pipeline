import pytest
import os

from .gcloud import GCLOUD as gcloud


@pytest.fixture(scope='session')
def env():
    return {
        'env': os.environ['ENV'],
        'project': gcloud.project(os.environ['ENV']),
        'user': gcloud.config()['username'],
        'client': 'bluesun'
    }
