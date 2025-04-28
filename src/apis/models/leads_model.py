from flask_restful_swagger_3 import Schema
from models.base_model import (
    EntityModel,
    MatchScoreModel,
    AdoptCurveModel,
    PersuasionAngleModel,
    ChannelStrategyModel,
    RelationshipFitModel,
    ArchetypeModel,
    # LcvLevelModel,
    # DaysToFirstSaleLevelModel
)


class Leads(Schema):
    type = 'object'
    properties = {
        'lead_id': {
            'type': 'integer'
        },
        'entity': EntityModel,
        'match_score': MatchScoreModel.array(),
        'adopt_curve': AdoptCurveModel,
        # 'personna': PersonnaModel,
        'persuasion_angle': PersuasionAngleModel,
        'channel_strategy': ChannelStrategyModel,
        'relationship_fit': RelationshipFitModel,
        'archetype': ArchetypeModel,
        # 'lcv_level': LcvLevelModel # include when Leadengine is ready
        # 'days_to_first_sale_level': DaysToFirstSaleLevelModel # include when Leadengine is ready
    }


class LeadsModel(Schema):
    type = 'object'
    properties = {
        'leads': Leads.array()
    }


class LeadIdsQueryParameters(Schema):
    type = 'string'
