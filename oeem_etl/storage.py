import json
from luigi.format import MixedUnicodeBytesFormat
from luigi.s3 import S3Client, S3Target, S3FlagTarget
from luigi.file import LocalFileSystem, LocalTarget
from luigi.contrib.gcs import GCSClient, GCSTarget, GCSFlagTarget
from oauth2client.service_account import ServiceAccountCredentials
import os


class StorageClient():

    def __init__(self, config):
        self.config = config
        self.storage_service = config['file_storage']
        self._make_target_classes()

    def _make_target_classes(self):
        '''Create client and target objects for storage service.'''

        if self.storage_service == 'local':
            client = LocalFileSystem()
            target = LocalTarget
            flag_target = LocalTarget # just use a normal target

        elif self.storage_service == 's3':
            client = S3Client(
                aws_access_key_id=self.config['aws_access_key_id'],
                aws_secret_access_key=self.config['aws_secret_access_key'])
            target = S3Target
            flag_target = S3FlagTarget

        elif self.storage_service == 'gcs':

            # Use GCP Service Account private key credentials to
            # authenticate with the GCS API. Service Accounts
            # are unique to the GCP project.
            key_json = json.loads(
                    self.config['gcs_service_account_private_key_json'])
            cred = ServiceAccountCredentials.from_json_keyfile_dict(key_json)
            client = GCSClient(oauth_credentials=cred)
            target = GCSTarget
            flag_target = GCSFlagTarget
        else:
            raise EnvironmentError('Please add known file_storage value to config.')

        # Targets will be initialized many times in luigi tasks.
        # Subclass the chosen Target and add the Client to it, so
        # you don't have to pass the Client to the DAG.

        class TargetWithClient(target):
            def __init__(self, path):
                super().__init__(path, client=client, format=MixedUnicodeBytesFormat())

        class FlagTargetWithClient(target):
            def __init__(self, path):
                super().__init__(path, client=client, format=MixedUnicodeBytesFormat())


        self.client = client
        self.target_class = TargetWithClient
        self.flag_target_class = FlagTargetWithClient

    def get_target_class(self):
        return self.target_class

    def get_flag_target_class(self):
        return self.flag_target_class

    def get_base_directory(self, prefix=''):
        """
        Returns a fully qualified path to the base directory given a particular
        prefix. Prefix should be given without trailing slash.
        """

        if self.storage_service == 's3':
            bucket = self.config['s3_bucket_name']
            path = 's3://{}/'.format(bucket)

        elif self.storage_service == 'gcs':
            bucket = self.config['gcs_bucket_name']
            path = 'gs://{}/'.format(bucket)

        elif self.storage_service == 'local':
            bucket = self.config['local_data_directory']
            path = os.path.join(bucket)

        else:
            message = (
                'Please add known file_storage value to config.'
                ' Options are "gcs", "s3", and "local".'
            )
            raise EnvironmentError(message)

        if prefix != "":
            if prefix.endswith("/"):
                return os.path.join(path, prefix)
            else:
                return os.path.join(path, prefix + "/")
        else:
            return path

    def get_existing_paths(self, prefix=""):
        '''
        Returns a list of paths to the client's raw data files based on
        prefix.
        '''
        base_directory = self.get_base_directory(prefix)

        # Return a list instead of a generator because luigi needs to
        # iterate through paths multiple times.
        dir_paths = list(self.client.listdir(base_directory))

        if self.storage_service in ['s3', 'gcs']:
            return dir_paths[1:]  # first path is parent dir, skip it.
        else:
            return dir_paths

