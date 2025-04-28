from __future__ import print_function

import sys
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, DebugOptions, StandardOptions, SetupOptions
from transforms.io.gcp import bigquery as bq
import requests
from datetime import datetime
from libs import WrenchAuthWrapper
import json
from apache_beam.options.value_provider import ValueProvider


class ReadWrenchInsights(beam.DoFn):
    def __init__(self, env, insight_id):
        self._env = env
        self._insight_id = insight_id

    def process(self, element):
        log = logging.getLogger()

        if isinstance(self._insight_id, ValueProvider):
            self._insight_id = self._insight_id.get()

        entity_ids = [e['entity_id'] for e in element[1]]
        log.info(f'entity_ids: {entity_ids}')

        api_key = WrenchAuthWrapper.instance(self._env).get_api_key(element[0])
        url = f"{api_key['host']}/{self._insight_id}"

        response = requests.post(
            url,
            data=json.dumps({
                'entity_id': entity_ids,
            }),
            headers={
                'authorization': api_key['api_key'],
                'content_type': 'application/json'
            })

        ignored_codes = [102, 204, 402]
        if response.status_code in ignored_codes:
            log.info(f'Insight response: status {response.status_code}; return case 1')
            return
        elif response.status_code == 200:
            log.info(f'Insight response: status {response.status_code}; response (trucated): {response.text[:250]}')
            for i in response.json().get('scores', None):
                yield i
        else:
            raise Exception(f'''Error reading insights from Leadengine:
                                Url: {url};
                                Entities: {entity_ids};
                                Status: {response.status_code};
                                Body: {response.text}''')


class MatchScoreTransform(beam.DoFn):
    def __init__(self, insight_id):
        self._insight_id = insight_id

    def process(self, element):
        if isinstance(self._insight_id, ValueProvider):
            self._insight_id = self._insight_id.get()

        if self._insight_id == 'match-score':
            scores = []
            for k, v in element.items():
                if k == 'entity_id':
                    continue
                scores.append({'client': k, 'score': int(v)})
            i = {'entity_id': element['entity_id'], 'scores':  scores}
            yield i
        else:
            yield element


class AdoptCurveTransform(beam.DoFn):
    def __init__(self, insight_id):
        self._insight_id = insight_id

    def process(self, element):
        if isinstance(self._insight_id, ValueProvider):
            self._insight_id = self._insight_id.get()

        if self._insight_id == 'adopt-curve':
            scores = []
            for k, v in element.items():
                if k == 'entity_id':
                    continue
                scores.append({'corpus': k, 'score': v})
            i = {'entity_id': element['entity_id'], 'scores':  scores}
            yield i
        else:
            yield element


class EnrichInsight(beam.DoFn):
    def process(self, element):
        element['ingestion_timestamp'] = f'{datetime.utcnow()}'
        yield element


class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env', help='local, dev, prd')
        parser.add_value_provider_argument('--insight_id',
                                           help='Endpoint name for the model')
        parser.add_value_provider_argument('--destination_table',
                                           help='Destination table for the insights')
        parser.add_value_provider_argument('--entity_query',
                                           help='Query used to select insights')
        parser.add_value_provider_argument('--page-size',
                                           type=int,
                                           default=10000,
                                           help='Page size for BigQuery results')


class Runner():
    @classmethod
    def _run(cls, p, options):
        with p:
            (
                p
                | 'Fetch entities from BQ' >> bq.ReadAsJson(
                    env=options.env,
                    query=options.entity_query,
                    page_size=options.page_size)
                | 'Group by client' >> beam.GroupBy(lambda x: x['client'])
                | 'Read Insights' >> beam.ParDo(ReadWrenchInsights(env=options.env, insight_id=options.insight_id))
                | 'Transform match-score' >> beam.ParDo(MatchScoreTransform(insight_id=options.insight_id))
                | 'Transform adopt-curve' >> beam.ParDo(AdoptCurveTransform(insight_id=options.insight_id))
                | 'Enrich Insight' >> beam.ParDo(EnrichInsight())
                | 'Write Insight to BigQuery' >> bq.WriteToBigQueryTransform(
                    table=options.destination_table,
                    env=options.env
                )
            )

    @classmethod
    def run(cls):
        log = logging.getLogger()
        log.warning('>> Running Leadengine to Bigquery Pipeline')

        options = RuntimeOptions()
        options.view_as(SetupOptions).save_main_session = True
        options.view_as(DebugOptions).experiments = ['use_beam_bq_sink']
        options.view_as(StandardOptions).streaming = False

        cls._run(beam.Pipeline(options=options), options)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().warning('> load_wrench_to_bq - Starting DataFlow Pipeline Runner')
    Runner.run()
