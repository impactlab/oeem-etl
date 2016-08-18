"""
Load configuration using the "Parameters from config Ingestion" pattern.

http://luigi.readthedocs.io/en/stable/configuration.html#parameters-from-config-ingestion
"""

import luigi
from oeem_etl.storage import StorageClient

class oeem(luigi.Config):
    url                  = luigi.Parameter()
    access_token         = luigi.Parameter()
    project_owner        = luigi.IntParameter()
    file_storage         = luigi.Parameter()
    local_data_directory = luigi.Parameter()

    _target_class = None
    _flag_target_class = None
    _storage = None
    _datastore = None

    @property
    def storage(self):
        if self._storage is None:
            self._storage = StorageClient(self.__dict__)
        return self._storage

    @property
    def target_class(self):
        if self._target_class is None:
            self._target_class = self.storage.get_target_class()
        return self._target_class

    @property
    def flag_target_class(self):
        if self._flag_target_class is None:
            self._flag_target_class = self.storage.get_flag_target_class()
        return self._flag_target_class

    @property
    def datastore(self):
        # Legacy naming: `datastore` refers to the dict of datastore configuration
        # properties. These now live in the `config.oeem` section.
        if self._datastore is None:
            self._datastore = self.__dict__
        return self._datastore

oeem = oeem()
