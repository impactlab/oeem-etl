import json
from luigi.s3 import S3Client, S3Target
from luigi.file import LocalFileSystem, LocalTarget
from luigi.contrib.gcs import GCSClient, GCSTarget
from oauth2client.service_account import ServiceAccountCredentials


class StorageClient():
    def __init__(self, config):
        self.config = config
        self.storage_service = config['file_storage']
        self._make_target()

    def _make_target(self):
        '''Create client and target objects for storage service.'''

        if self.storage_service == 'local':
            self.client = LocalFileSystem()
            return  LocalTarget

        elif self.storage_service == 's3':
            client = S3Client(aws_access_key_id=self.config['aws_access_key'],
                              aws_secret_access_key=self.config['aws_secret_key'])
            target = S3Target

        elif self.storage_service == 'gcs':
            # Use GCP Service Account private key credentials to
            # authenticate with the GCS API. Service Accounts
            # are unique to the GCP project.
            key_json = json.loads(self.config['gcs_service_account_private_key_json'])
            cred = ServiceAccountCredentials.from_json_keyfile_dict(key_json)
            client = GCSClient(oauth_credentials=cred)
            target = GCSTarget
        else:
            raise EnvironmentError('Please add known file_storage value to config.')
        # Targets will be initialized many times in luigi tasks.
        # Subclass the chosen Target and add the Client to it, so
        # you don't have to pass the Client to the DAG.
        self.client = client
        class TargetWithClient(target):
            def __init__(self, path):
                super().__init__(path, client=client)
        self.target = TargetWithClient

    def get_target(self):
        return self.target

    def get_dataset_paths(self, dataset_type):
        '''Generator that returns a list of paths to the client's
        raw data files - either projects or consumption - that
        should be parsed and uploaded.'''
        path_to_raw_files = '{}/raw/'.format(dataset_type)
        if self.storage_service == 's3':
            client_path = 's3://{}/{}'.format(self.config['s3_bucket_name'],
                                              self.config.get('s3_client_path', ''))
        elif self.storage_service == 'gcs':
            client_path = 'gs://{}/'.format(self.config['gcs_bucket_name'])
        elif self.storage_service == 'local':
            client_path = 'data/'
            pass
        else:
            raise EnvironmentError('Please add known file_storage value to config.')
        dir_paths = self.client.listdir(client_path + path_to_raw_files)
        if self.storage_service == 's3' or self.storage_service == 'gcs':
            next(dir_paths)  # first path is parent dir, skip it.
        # Return a list instead of a generator because luigi needs to
        # iterate through paths multiple times.
        return list(dir_paths)

