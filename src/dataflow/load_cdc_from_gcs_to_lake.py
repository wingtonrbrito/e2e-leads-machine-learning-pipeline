from __future__ import print_function

import sys
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, DebugOptions, StandardOptions, SetupOptions
from transforms.io.gcp import bigquery as bq
import json
import dill
from transforms.datetime import InsertIngestionTimestamp, StringifyDatetimes, StringToDatetime
from libs import GCLOUD as gcloud, Crypto, Config
from transforms.io.filelist_iterator import FileListIteratorTransform


class RecordDecryption(beam.DoFn):
    def start_bundle(self):
        self._keys = self.get_keys()

    def get_keys(self):
        return Config.list_secret_keys(self._env, 'vibe-cdc')

    def __init__(self, env):
        self._env = env
        self._log = logging.getLogger()

    def process(self, line):
        line_dec = Crypto.decrypt(line, self._keys)
        line_dec = json.loads(line_dec)
        yield line_dec


# Note: Update sort_key function based on the filename format or pass it at runtime wbrito 05/05/2020
class RuntimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--env', help='local, dev, prd')
        parser.add_value_provider_argument('--files_startwith', help='file names starting pattern')
        parser.add_value_provider_argument(
            '--sort_key',
            default=bytes.hex(
                dill.dumps(
                    lambda f: int(f[f.rfind('-') + 1:]) if f[f.rfind('-') + 1:].isdigit() else f)),
            help='serialized function to sort file list'
        )
        parser.add_value_provider_argument('--dest', help='Table to write to. Ex: project_id:lake.pyr')
        parser.add_value_provider_argument('--page-size',
                                           type=int,
                                           default=10000,
                                           help='Page size for BigQuery results')


class Runner():
    @classmethod
    def _run(cls, p, options):
        with p:
            project_id = gcloud.project(options.env)
            bucket = f'{project_id}-cdc-imports'
            (p
                | 'Iterate File Paths' >> FileListIteratorTransform(
                    env=options.env,
                    bucket=bucket,
                    files_startwith=options.files_startwith,
                    sort_key=options.sort_key)
                | 'Read from a File' >> beam.io.ReadAllFromText()
                | 'Apply Decryption Transform' >> beam.ParDo(RecordDecryption(env=options.env))
                | 'Insert Ingestion Timestamp' >> beam.ParDo(InsertIngestionTimestamp())
                | 'Ingest table schema' >> beam.ParDo(bq.InjectTableSchema(table=options.dest, env=options.env))
                | 'Transform String to Standard SQL Datetime' >> beam.ParDo(StringToDatetime())
                | 'Transform Nested Datetimes' >> beam.ParDo(StringifyDatetimes('%Y-%m-%d %H:%M:%S.%f'))
                | 'Write To BigQuery' >> bq.WriteToBigQueryTransform(table=options.dest, env=options.env)
             )

    @classmethod
    def run(cls):
        log = logging.getLogger()
        log.warning('>> Running GCS to Lake Pipeline')

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
    logging.getLogger().warning('> load_cdc_from_gcs_to_lake - Starting DataFlow Pipeline Runner')
    Runner.run()
