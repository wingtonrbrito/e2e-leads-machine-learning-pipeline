from setuptools import setup, find_packages

REQUIRED_PACKAGES = [
    'google-cloud-bigquery==1.27.2',
    'google-api-python-client==1.10.0',
    'google-cloud-storage==1.30.0',
    'google-cloud-secret-manager==2.0.0',
    'neo4j==4.0.0',
    'gunicorn==20.0.4',
    'flask==1.1.2',
    'flask-jwt-extended==3.24.1',
    'flask-restful==0.3.8',
    'flask-mail==0.9.1',
    'pyjwt==1.7.1',
    'flask_restful_swagger_3==0.1',
    'swagger-ui-py==0.3.0',
    'locust==1.3.0'
]

setup(name="ml-apis",
      packages=find_packages(),
      install_requires=REQUIRED_PACKAGES)
