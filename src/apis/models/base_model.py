from flask_restful_swagger_3 import Schema


class Preference(Schema):
    type = 'string'


class Persuasion(Schema):
    type = 'string'


class EntityModel(Schema):
    type = 'object'
    properties = {
        "entity_id": {
            'type': 'string'
        },
        "email": {
            'type': 'string'
        },
        "phone": {
            'type': 'string'
        },
        "twitter": {
            'type': 'string'
        }
    }


class MatchScoreModel(Schema):
    type = 'object'
    properties = {
        "client": {
            'type': 'string'
        },
        "score": {
            'type': 'integer'
        }
    }


class AdoptCurveModel(Schema):
    type = 'object'
    properties = {
        "client": {
            'type': 'string'
        },
        "score": {
            'type': 'integer'
        }
    }


# class PersonnaModel(Schema):
#     type = 'object'
#     properties = {
#         "investor": {
#             'type': 'string'
#         },
#         "angel": {
#             'type': 'string'
#         },
#         "ia": {
#             'type': 'string'
#         },
#         "vc": {
#             'type': 'string'
#         },
#         "accredited": {
#             'type': 'string'
#         },
#         "label": {
#             'type': 'string'
#         }
#     }


class PersuasionAngleModel(Schema):
    type = 'object'
    properties = {
        'available_persuasion': [
            'Personalization',
            'Authority',
            'Consensus',
            'Liking',
            'Commitment',
            'Reciprocity',
            'Comparison',
            'Cooperation',
            'Self Monitoring',
            'Simulation',
            'Suggestions',
            'Praise'
        ],
        'persuasion': Persuasion.array()
    }


class ChannelStrategyModel(Schema):
    type = 'object'
    properties = {
        'available_preferences': [
            "Email",
            "Web Chat",
            "Social Chat",
            "SMS",
            "Phone"
        ],
        'preferences': Preference.array()
    }


class RelationshipFitModel(Schema):
    type = 'object'
    properties = {
        'prediction': {
            'type': 'string'
        }
    }


class ArchetypeModel(Schema):
    type = 'string'


class LcvLevelModel(Schema):
    type = 'string'


class DaysToFirstSaleLevelModel(Schema):
    type = 'string'
