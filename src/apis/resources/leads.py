import os
from flask_restful_swagger_3 import Resource, swagger
from flask_restful import request, reqparse
from flask_jwt_extended import jwt_required, get_raw_jwt
from libs.shared.gcloud import GCLOUD as gcloud
from libs.response_helper import ResponseHelper, handle_exception
from libs.validation_helper import not_valid_csv, parse_csv
from models.leads_model import LeadsModel, LeadIdsQueryParameters
from services.leads_service import LeadsService
from .auth import Auth
import logging


env = os.environ['ENV']
project_id = gcloud.project(env)
log = logging.getLogger('LeadsAPI')


class Leads(Resource):
    @swagger.doc({
        'tags': ['/leads'],
        'description': 'Returns recommendations based on leads_ids',
        'security': [
            {'bearerAuth': []}
        ],
        'parameters': [
            {
                'in': 'query',
                "description": "Comma separated string of lead_ids. Ex 1,2,3,4",
                "required": True,
                'name': 'lead_ids',
                'schema': LeadIdsQueryParameters,
                'style': 'form',
                'explode': 'false'
            }
        ],
        'responses': {
            '200': {
                'description': 'Ok',
                'content': {
                    'application/json': {
                        'schema': LeadsModel
                    }
                }
            },
            '405': {
                'description': 'Token identity error'
            },
            '410': {
                'description': 'Resource no longer exists'
            },
            '422': {
                'description': 'Unprocessable Entity - usually a bad token'
            },
        }
    })
    @handle_exception
    @jwt_required
    def get(self):
        a = Auth()
        payload = get_raw_jwt()
        domain = a.get_domain(payload)
        a.validate_access_token_identity(request.host, payload)
        client = a.get_auth_client(domain)
        parser = reqparse.RequestParser()
        parser.add_argument('lead_ids', type=str)
        args = parser.parse_args()

        lead_ids = args.get('lead_ids', None)
        if lead_ids is None or len(lead_ids) == 0:
            return ResponseHelper.abort(400, msg='lead_ids is malformed or missing')

        if not_valid_csv(lead_ids):
            return ResponseHelper.abort(400, msg='lead_ids is malformed or missing')
        leads_service = LeadsService(env=env)
        leads = leads_service.get_leads(client=client, lead_ids=parse_csv(lead_ids))
        if leads is None:
            return ResponseHelper.abort(410, msg=f'The recommendations for client {client} no longer exists')
        return leads
