import luigi
import yaml
from .parse import ParseFile
from ..uploader import upload_project_dataframe, upload_consumption_dataframe
import pandas as pd
import os

class UploadProject(luigi.Task):
    path = luigi.Parameter()
    storage = luigi.Parameter()
    parser = luigi.Parameter()
    datastore = luigi.Parameter()

    def requires(self):
        return ParseFile(self.path, self.storage.get_target(), self.parser)

    def load_dataset(self):
        df = pd.read_csv(self.input().open('r'),
                dtype={"zipcode": str, "weather_station": str})
        df.baseline_period_end = pd.to_datetime(df.baseline_period_end)
        df.reporting_period_start = pd.to_datetime(df.reporting_period_start)
        return df

    def write_flag(self):
        target = self.storage.get_target()(os.path.join(self._get_uploaded_file_path(), "_SUCCESS"))
        with target.open('w') as f:
            pass

    def run(self):
        parsed_project = self.load_dataset()
        project_results = upload_project_dataframe(parsed_project, self.datastore)
        self.write_flag()

    def _get_uploaded_file_path(self):
        split_path = self.path.split('/')

        # Change parent dir from 'raw' to 'parsed'.
        split_path[-2] = 'uploaded'

        # Change file extension to csv.
        filename = os.path.splitext(split_path[-1])[0]

        split_path[-1] = filename
        split_path.append("") # trailing slash
        return '/'.join(split_path)

    def output(self):
        return self.storage.get_flag_target()(self._get_uploaded_file_path())

class UploadConsumption(luigi.Task):
    path = luigi.Parameter()
    storage = luigi.Parameter()
    parser = luigi.Parameter()
    datastore = luigi.Parameter()

    def requires(self):
        return ParseFile(self.path, self.storage.get_target(), self.parser)

    def load_dataset(self):
        df = pd.read_csv(self.input().open('r'),
                dtype={"zipcode": str, "weather_station": str})
        df.start = pd.to_datetime(df.start)
        df.end = pd.to_datetime(df.end)
        return df

    def write_flag(self):
        target = self.storage.get_target()(os.path.join(self._get_uploaded_file_path(), "_SUCCESS"))
        with target.open('w') as f:
            pass

    def run(self):
        parsed_consumption = self.load_dataset()
        consumption_results = upload_consumption_dataframe(parsed_consumption, self.datastore)
        self.write_flag()

    def _get_uploaded_file_path(self):
        split_path = self.path.split('/')

        # Change parent dir from 'raw' to 'parsed'.
        split_path[-2] = 'uploaded'

        # Change file extension to csv.
        filename = os.path.splitext(split_path[-1])[0]

        split_path[-1] = filename
        split_path.append("") # trailing slash
        return '/'.join(split_path)

    def output(self):
        return self.storage.get_flag_target()(self._get_uploaded_file_path())

class UploadDatasets(luigi.Task):
    raw_project_paths = luigi.Parameter()
    raw_consumption_paths = luigi.Parameter()
    project_parser = luigi.Parameter()
    consumption_parser = luigi.Parameter()
    storage = luigi.Parameter()
    datastore = luigi.Parameter()

    def requires(self):
        parsed_projects = [
            UploadProject(path, self.storage, self.project_parser, self.datastore)
            for path in self.raw_project_paths
        ]
        parsed_consumption = [
            UploadConsumption(path, self.storage, self.consumption_parser, self.datastore)
            for path in self.raw_consumption_paths
        ]

        # Is there any guarantee of ordering here?
        return [parsed_projects, parsed_consumption]

    def run(self):
        pass
