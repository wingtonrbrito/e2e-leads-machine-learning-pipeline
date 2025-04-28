import os
from flask_restful_swagger_3 import Resource
from flask_restful import request
from flask_jwt_extended import jwt_required, get_raw_jwt
from libs.shared.gcloud import GCLOUD as gcloud
from libs.response_helper import handle_exception
from .auth import Auth

env = os.environ['ENV']
project_id = gcloud.project(env)


class Sendgrid(Resource):
    @handle_exception
    @jwt_required
    def get(self):
        a = Auth()
        payload = get_raw_jwt()
        a.validate_access_token_identity(request.host, payload)
        raise Exception('Forced error for testing sendgrid')
