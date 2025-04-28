import os
from flask_restful_swagger_3 import Resource, swagger
from flask_restful import request, reqparse
from flask_jwt_extended import jwt_required, get_raw_jwt
from libs.shared.gcloud import GCLOUD as gcloud
from libs.response_helper import ResponseHelper, handle_exception
from libs.validation_helper import not_valid_csv, parse_csv
from models.contacts_model import GetContactsResponse, GetContactsRequest, PostContactsRequest, Contact
from services.contacts_service import ContactsService
from .auth import Auth
import logging
from datetime import datetime

env = os.environ['ENV']
project_id = gcloud.project(env)
log = logging.getLogger('ContactsAPI')


class Contacts(Resource):
    @swagger.doc({
        'tags': ['/contacts'],
        'description': 'Returns recommendations based on contacts_ids',
        'security': [
            {'bearerAuth': []}
        ],
        'parameters': [
            {
                'in': 'query',
                "description": "Comma separated string of contact_ids. Ex 1,2,3,4",
                "required": True,
                'name': 'contact_ids',
                'schema': GetContactsRequest,
                'style': 'form',
                'explode': 'false'
            }
        ],
        'responses': {
            '200': {
                'description': 'Ok',
                'content': {
                    'application/json': {
                        'schema': GetContactsResponse
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
        parser.add_argument('contact_ids', type=str)
        args = parser.parse_args()

        contact_ids = args.get('contact_ids', None)
        if contact_ids is None:
            return ResponseHelper.abort(400, msg='contact_ids is malformed or missing')

        if not_valid_csv(contact_ids):
            return ResponseHelper.abort(400, msg='contact_ids is malformed or missing')
        contacts_service = ContactsService(env=env)
        contacts = contacts_service.get_contacts(client=client, contact_ids=parse_csv(contact_ids))
        if contacts is None:
            return ResponseHelper.abort(410, msg=f'The recommendations for client {client} no longer exists')
        return contacts

    @swagger.doc({
        'tags': ['/contacts'],
        'description': 'Upload contacts',
        'security': [
            {'bearerAuth': []}
        ],
        'requestBody': {
            'description': 'JSON array of contact objects',
            'required': True,
            'content': {
                'application/json': {
                    'schema': PostContactsRequest
                }
            }
        },
        'responses': {
            '200': {
                'description': 'Ok'
            },
            '400': {
                'description': 'Payload error'
            },
            '405': {
                'description': 'Token identity error'
            },
            '422': {
                'description': 'Unprocessable Entity - usually a bad token'
            },
        }
    })
    @handle_exception
    @jwt_required
    def post(self):
        a = Auth()
        payload = get_raw_jwt()
        domain = a.get_domain(payload)
        a.validate_access_token_identity(request.host, payload)
        client = a.get_auth_client(domain)

        request_body = request.get_json()

        if request_body is None or type(request_body) is not dict or 'contacts' not in request_body:
            return ResponseHelper.abort(400, msg='Request is malformed or missing')

        try:
            for c in request_body['contacts']:
                # Valid if an exception is not thrown
                Contact(**c)
                # Enrich payload
                c.update({'client': client, 'ingestion_timestamp': str(datetime.utcnow())})
        except Exception as e:
            return ResponseHelper.abort(400, msg='Request is malformed', error=str(e))

        contacts_service = ContactsService(env=env)
        contacts_service.post_contacts(request_body['contacts'])

        return {'records_uploaded': len(request_body['contacts'])}
