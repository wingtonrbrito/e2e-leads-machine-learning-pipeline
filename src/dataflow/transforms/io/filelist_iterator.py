import apache_beam as beam
from apache_beam.options.value_provider import ValueProvider
from libs import CloudStorage, GCLOUD
import dill


class IterateFilePathsFn(beam.DoFn):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, '_' + k, v)

    def process(self, unused_el):
        if isinstance(self._files_startwith, ValueProvider):
            self._files_startwith = self._files_startwith.get()
        if isinstance(self._file_name, ValueProvider):
            self._file_name = self._file_name.get()
        if isinstance(self._sort_key, ValueProvider):
            self._sort_key = self._sort_key.get()
        if isinstance(self._env, ValueProvider):
            self._env = self._env.get()
        if isinstance(self._bucket, ValueProvider):
            self._bucket = self._bucket.get()

        project_id = GCLOUD.project(self._env)
        blobs = CloudStorage.factory(project_id).list_blobs(self._bucket, self._files_startwith)
        paths = [f'gs://{self._bucket}/{b.name}' for b in blobs if self._file_name in b.name]

        if isinstance(self._sort_key, str):
            self._sort_key = dill.loads(bytes.fromhex(self._sort_key))
        paths.sort(key=self._sort_key) if len(paths) > 1 else None
        for file in paths:
            yield file


class FileListIteratorTransform(beam.PTransform):
    def __init__(self, env, sort_key, bucket, files_startwith='', file_name=''):
        self._env = env
        self._bucket = bucket
        self._files_startwith = files_startwith
        self._file_name = file_name
        self._sort_key = sort_key

    def expand(self, pcoll):
        return (pcoll
                | 'Create empty IO' >> beam.Create([None])
                | 'Iterate File Paths' >> beam.ParDo(
                    IterateFilePathsFn(
                        env=self._env,
                        bucket=self._bucket,
                        sort_key=self._sort_key,
                        files_startwith=self._files_startwith,
                        file_name=self._file_name
                    ))
                )
