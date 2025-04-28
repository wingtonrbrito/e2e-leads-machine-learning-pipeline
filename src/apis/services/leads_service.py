
from models.leads_model import LeadsModel
from libs.shared.bigquery import BigQuery


class LeadsService():

    def __init__(self, env):
        self.client = BigQuery(env=env)

    def get_leads(self, client, lead_ids):

        def process_lead_response(lead_response):
            return {
                'lead_id': lead_response['lead_id'],
                'entity': {
                    "entity_id": lead_response['entity_id'],
                    'email': lead_response['email'],
                    'phone': lead_response['phone'],
                    'twitter': lead_response['twitter']
                },
                'match_score': lead_response['match_scores'],
                'adopt_curve': lead_response['adopt_scores'],
                'persuasion_angle': {
                    'persuasion': lead_response['persuasion']
                },
                'channel_strategy': {
                    'preferences': lead_response['preferences']
                },
                'relationship_fit': {
                    'prediction': lead_response['relationship_fit']
                },
                'archetype': lead_response['archetype'],
                'lcv_level': lead_response['lcv_level'],
                'days_to_first_sale_level': lead_response['days_to_first_sale_level']
            }

        str_ids = ','.join(map(str, lead_ids))
        lead_response = self.client.query(f'''
            select
                e.entity_id,
                el.lead_id,
                e.email,
                e.phone,
                e.twitter,
                lc.preferences,
                lmc.scores as match_scores,
                ac.scores as adopt_scores,
                lp.persuasion,
                rf.prediction relationship_fit,
                a.archetype,
                lcv.prediction lcv_level,
                dtfs.prediction days_to_first_sale_level
            from
                leadengine.entities e
                join leadengine.lead_entities_lk el on e.entity_id = el.entity_id
                left join leadengine.channel lc on e.entity_id = lc.entity_id
                left join leadengine.match_score lmc on e.entity_id = lmc.entity_id
                left join leadengine.adopt_curve ac on e.entity_id = ac.entity_id
                left join leadengine.persuasion lp on e.entity_id = lp.entity_id
                left join leadengine.relationship_fit rf on e.entity_id = rf.entity_id
                left join leadengine.archetypes a on e.entity_id = a.entity_id
                left join leadengine.lcv_level lcv on e.entity_id = lcv.entity_id
                left join leadengine.days_to_first_sale_level dtfs on e.entity_id = dtfs.entity_id
                where
                    e.client = '{client}'
                    and el.lead_id in ({str_ids})
        ''')

        bq_response = {
            'leads': list(map(process_lead_response, lead_response))
        }

        return LeadsModel(**bq_response)
