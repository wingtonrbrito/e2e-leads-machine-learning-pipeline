import apache_beam as beam
from apache_beam.transforms import PTransform
import gzip
from libs.shared.storage import CloudStorage
from apache_beam.options.value_provider import ValueProvider


class UploadByKey(beam.DoFn):
    def __init__(self, env, bucket, table, file):
        self._env = env
        self._bucket = bucket
        self._table = table
        self._file = file

    @classmethod
    def csv_and_compress_elements(self, elements):
        content = ','.join(
            ['"' + str(k).replace('"', r'\"') + '"' for k in list(elements)[0].keys() if k not in ['client']])
        content += '\n'

        for e in elements:
            content += ','.join(
                ['"' + str(v).replace('"', r'\"') + '"' for k, v in e.items() if k not in ['client']])
            content += '\n'

        return gzip.compress(content.encode())

    def process(self, element):
        if isinstance(self._bucket, ValueProvider):
            self._bucket = self._bucket.get()
        if isinstance(self._table, ValueProvider):
            self._table = self._table.get()
        if isinstance(self._file, ValueProvider):
            self._file = self._file.get()

        client = element[0]
        content = self.csv_and_compress_elements(element[1])
        blob_name = f'{self._table}-{client}-{self._file}'
        CloudStorage.factory(self._env).upload_blob_content(
            self._bucket,
            blob_name,
            content,
            is_binary=True)


class UploadTransform(PTransform):
    def __init__(self, env, bucket, table, file):
        self._env = env
        self._bucket = bucket
        self._table = table
        self._file = file

    def expand(self, pcoll):
        return (pcoll
                | 'Write to GCS' >> beam.ParDo(
                    UploadByKey(self._env, self._bucket, self._table, self._file)))
