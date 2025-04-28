from flask_restful_swagger_3 import Schema
from models.base_model import (
    EntityModel,
    MatchScoreModel,
    AdoptCurveModel,
    PersuasionAngleModel,
    ChannelStrategyModel,
    RelationshipFitModel,
    ArchetypeModel,
    # LcvLevelModel
    # DaysToFirstSaleLevelModel
)


class Contacts(Schema):
    type = 'object'
    properties = {
        'contact_id': {
            'type': 'integer'
        },
        'entity': EntityModel,
        'match_score': MatchScoreModel.array(),
        'adopt_curve': AdoptCurveModel,
        # #'personna': PersonnaModel,
        'persuasion_angle': PersuasionAngleModel,
        'channel_strategy': ChannelStrategyModel,
        'relationship_fit': RelationshipFitModel,
        'archetype': ArchetypeModel,
        # 'lcv_level': LcvLevelModel # include when Leadengine is ready
        # 'days_to_first_sale_level': DaysToFirstSaleLevelModel # include when Leadengine is ready
    }


class GetContactsResponse(Schema):
    type = 'object'
    properties = {
        'contacts': Contacts.array()
    }


class GetContactsRequest(Schema):
    type = 'string'


class Contact(Schema):
    type = 'object'
    required = ['id', 'first_name', 'last_name']
    properties = {
        'id': {
            'type': 'integer'
        },
        'first_name': {
            'type': 'string'
        },
        'last_name': {
            'type': 'string'
        },
        'email': {
            'type': 'string'
        },
        'email2': {
            'type': 'string'
        },
        'email3': {
            'type': 'string'
        },
        'phone': {
            'type': 'string'
        },
        'phone2': {
            'type': 'string'
        },
        'phone3': {
            'type': 'string'
        },
        'twitter': {
            'type': 'string'
        },
        'city': {
            'type': 'string'
        },
        'state': {
            'type': 'string'
        },
        'zip': {
            'type': 'string'
        },
        'country': {
            'type': 'string'
        }
    }


class PostContactsRequest(Schema):
    type = 'object'
    properties = {
        'contacts': Contact.array()
    }
