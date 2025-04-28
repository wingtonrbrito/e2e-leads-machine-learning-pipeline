from models.contacts_model import GetContactsResponse
from libs.shared.bigquery import BigQuery, QueryBuilder


class ContactsService():

    def __init__(self, env):
        self.client = BigQuery(env=env)

    def get_contacts(self, client, contact_ids):

        def process_contact_response(contact_response):
            return {
                'contact_id': contact_response['contact_id'],
                'entity': {
                    "entity_id": contact_response['entity_id'],
                    'email': contact_response['email'],
                    'phone': contact_response['phone'],
                    'twitter': contact_response['twitter']
                },
                'match_score': contact_response['match_scores'],
                'adopt_curve': contact_response['adopt_scores'],
                'persuasion_angle': {
                    'persuasion': contact_response['persuasion']
                },
                'channel_strategy': {
                    'preferences': contact_response['preferences']
                },
                'relationship_fit': {
                    'prediction': contact_response['relationship_fit']
                },
                'archetype': contact_response['archetype'],
                'lcv_level': contact_response['lcv_level'],
                'days_to_first_sale_level': contact_response['days_to_first_sale_level']
            }

        str_ids = ','.join(map(str, contact_ids))
        contact_response = self.client.query(f'''
            select
                e.entity_id,
                el.contact_id,
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
                join leadengine.contact_entities_lk el on e.entity_id = el.entity_id
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
                and el.contact_id in ({str_ids})
        ''')

        bq_response = {
            'contacts': list(map(process_contact_response, contact_response))
        }

        return GetContactsResponse(**bq_response)

    def post_contacts(self, contacts):
        query = QueryBuilder(
            table='lake.contacts',
            insert=contacts
        )
        result = self.client.query(query)
        return result
