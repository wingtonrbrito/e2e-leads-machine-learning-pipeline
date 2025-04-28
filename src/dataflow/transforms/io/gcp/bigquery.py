import logging
import apache_beam as beam
from apache_beam.transforms import PTransform
from apache_beam.options.value_provider import StaticValueProvider, ValueProvider
from google.cloud.bigquery.job import LoadJobConfig
from google.cloud.bigquery import SourceFormat
from libs.shared.bigquery import BigQuery
from past.builtins import unicode


class GetBigQueryDataFn(beam.DoFn):
    def __init__(self, **kwargs):
        self._client = None
        for k, v in kwargs.items():
            if isinstance(v, (str, unicode, int)):
                v = StaticValueProvider(type(v), v)
            setattr(self, '_'+k, v)

    def build_query(self):
        if self._table is not None:
            qry = f'SELECT * FROM {self._table.get()}'
        else:
            qry = self._query.get()
        return qry

    def process(self, element):
        if self._client is None:
            self._client = BigQuery(env=self._env.get()).client

        qry = self.build_query()
        logging.getLogger().info('>> query: %s' % qry)
        logging.getLogger().info('>> page_size: %d' % self._page_size.get())
        query_job = self._client.query(qry)
        rows = query_job.result(page_size=self._page_size.get())
        logging.getLogger().info('>> total_rows: %d' % rows.total_rows)

        for row in rows:
            yield dict(row.items())


class ReadAsJson(PTransform):
    def __init__(self, env=None, query=None, table=None, page_size=10000, **kwargs):
        if query and table:
            raise Exception('Cannot use both table and query arguments!')

        if (table or query) and not env:
            raise Exception('Must specify env param')

        if not query and not table:
            raise Exception('Must specify query or full table path')

        self.env = env
        self.table = table
        self.query = query
        self.page_size = page_size
        self.kwargs = kwargs

    def expand(self, pcoll):
        return (pcoll
                | "Initializing with empty collection" >> beam.Create([1])
                | 'Read records from BigQuery' >> beam.ParDo(
                    GetBigQueryDataFn(
                        query=self.query,
                        table=self.table,
                        env=self.env,
                        page_size=self.page_size,
                        **self.kwargs
                    ))
                )


class InjectTableSchema(beam.DoFn):
    def __init__(self, table, env):
        self._client = None
        self._table = table
        self._env = env
        self._table_schema = None

    def process(self, element):
        def _schematize(el):
            if isinstance(self._env, ValueProvider):
                self._env = self._env.get()
            if self._client is None:
                self._client = BigQuery(self._env).client
            if isinstance(self._table, ValueProvider):
                self._table = self._table.get()

            table_id = self._table.replace(':', '.')
            if self._table_schema is None:
                self._table_schema = self._client.get_table(table_id).schema  # Make an API request.
            return {**{'schema': self._table_schema}, **{'payload': element}}
        yield _schematize(element)


class WriteToBigQuery(beam.DoFn):
    def __init__(self, table, env):
        self._table = table
        self._env = env

    def process(self, element):
        if isinstance(self._table, ValueProvider):
            self._table = self._table.get()
        if isinstance(self._env, ValueProvider):
            self._env = self._env.get()
        client = self.client(self._env)
        table_id = self._table.replace(':', '.')
        logging.getLogger().info(f'Writing to bigquery >> {table_id}')
        table = client.get_table(table_id)
        table_schema = table.schema
        job_config = self.load_job_config()
        job_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = table_schema
        job = client.load_table_from_json(element, table_id, job_config=job_config)
        job.result()

    # expose property through a method for test mocking purposes - jc 10/2/2020
    def client(self, env):
        return BigQuery(env).client

    # expose property through a method for test mocking purposes - jc 10/2/2020
    def load_job_config(self):
        return LoadJobConfig()


class WriteToBigQueryTransform(PTransform):
    def __init__(self, table, env):
        self._env = env
        self._table = table
        if env == 'prd':
            self._write_to_bigquery = beam.io.WriteToBigQuery(table, env)
        else:
            self._write_to_bigquery = WriteToBigQuery(table, env)

    def expand(self, pcoll):
        if self._env == 'prd':
            return (pcoll | 'Write to BigQuery' >> self._write_to_bigquery)
        else:
            return (pcoll
                    | 'Combine' >> beam.combiners.ToList()
                    | 'Write to BigQuery' >> beam.ParDo(self._write_to_bigquery)
                    )
