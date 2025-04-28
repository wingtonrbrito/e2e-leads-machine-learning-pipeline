from __future__ import print_function

import logging
import apache_beam as beam
import sys
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, DebugOptions, SetupOptions
from transforms.io.gcp import bigquery as bq
from transforms.datetime import InsertIngestionTimestamp, StringifyDatetimes


class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env', help='local, dev, prd')
        parser.add_value_provider_argument('--dest',
                                           default='staging.contacts',
                                           help='Table to write to. Ex: project_id:lake.pyr')
        parser.add_value_provider_argument('--query', type=str, help='BigQuery query statement')
        parser.add_value_provider_argument('--page-size',
                                           type=int,
                                           default=10000,
                                           help='Page size for BigQuery results')


class Runner():
    @classmethod
    def _run(cls, p, options):
        with p:
            (p | 'Read BigQuery Data' >> bq.ReadAsJson(
                env=options.env,
                query=options.query,
                page_size=options.page_size)
                | 'Insert Ingestion Timestamp' >> beam.ParDo(InsertIngestionTimestamp())
                | 'Transform Nested Datetimes' >> beam.ParDo(StringifyDatetimes())
                | 'Write To BigQuery' >> bq.WriteToBigQueryTransform(table=options.dest, env=options.env))

    @classmethod
    def run(cls):
        log = logging.getLogger()
        log.warning('>> Running Lake to Staging contacts pipeline')

        options = RuntimeOptions()

        # Save main session state so pickled functions and classes
        # defined in __main__ can be unpickled
        options.view_as(SetupOptions).save_main_session = True
        options.view_as(DebugOptions).experiments = ['use_beam_bq_sink']
        options.view_as(StandardOptions).streaming = False

        cls._run(beam.Pipeline(options=options), options)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().warning('> load_lake_to_staging_contacts - Starting DataFlow Pipeline Runner')
    Runner().run()
