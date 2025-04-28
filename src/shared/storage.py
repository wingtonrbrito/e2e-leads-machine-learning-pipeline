from google.cloud import storage
from .utils import tempdir
from google.cloud import exceptions


class CloudStorage():
    cloud_storage = False

    @classmethod
    def factory(cls, project_id):
        if not cls.cloud_storage:
            cls.cloud_storage = CloudStorage(project_id)
        return cls.cloud_storage

    def __init__(self, project_id):
        self.client = storage.Client(project=project_id)

    def get_bucket(self, bucket_name):
        return self.client.get_bucket(bucket_name)

    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

    def upload_blob_from_string(self, bucket_name, source_string, destination_blob_name, content_type='text/plain'):
        """Uploads a string to the bucket."""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(source_string, content_type=content_type)

    def upload_blob_content(self, bucket_name, destination_blob_name, file_content, is_binary=False):
        # This directory should be removed once the operation is complete, because of GDPR. Stu M. 11/29/19
        with tempdir() as tmp:
            source_file_name = f"{tmp}/{destination_blob_name.replace('/', '-')}"
            write_mode = 'wb' if is_binary else 'w'
            with open(source_file_name, write_mode) as file:
                file.write(file_content)
                file.close()
            self.upload_blob(bucket_name, source_file_name, destination_blob_name)

    def download_blob_as_string(self, bucket_name, source_blob_name):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.get_blob(source_blob_name)
        if (blob is not None):
            return blob.download_as_string()

    def delete_blob(self, bucket_name, source_blob_name):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        try:
            blob.delete()
        except exceptions.NotFound:
            pass

    def delete_files(self, bucket_name, pattern=''):
        blobs = self.client.list_blobs(bucket_name)
        for blob in blobs:
            if pattern in blob.name:
                blob.delete()

    def create_subdirectory(self, bucket_name, subdirectory):
        bucket = self.client.bucket(bucket_name)
        bucket.blob('subdirectory/')

    def list_files(self, bucket_name, prefix=None, delimiter=None):
        blobs = self.client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

        # Filter out path including the root blob. Just return file names - jc 2/18/2020
        files = []
        for b in blobs:
            name = b.name.replace(f"{prefix}/", "")
            if (len(name) > 0):
                files.append(name)

        return files

    def list_blobs(self, bucket_name, prefix=None, delimiter=None):
        return self.client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    def blob_exists(self, bucket_name, source_blob_name):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        return blob.exists()

    def move_files(self, from_bucket, to_bucket, prefix=None, pattern=None):
        for b in from_bucket.list_blobs(prefix=prefix):
            if pattern is None or pattern in b.name:
                from_bucket.copy_blob(b, to_bucket)
                from_bucket.delete_blob(b.name)

    def has_file(self, bucket, prefix=None):
        if prefix:
            return len(list(bucket.list_blobs(prefix=prefix))) > 0
        else:
            return len(list(bucket.list_blobs())) > 0

    @classmethod
    def remove_prefix(cls, text, prefix):
        if text.startswith(prefix):
            return text[len(prefix):]
        return text
