import luigi
import yaml
from .parse import ParseFile
from ..uploader import upload_project_dataframe, upload_consumption_dataframe
import pandas as pd
import os

class UploadProject(luigi.Task):
    path = luigi.Parameter()
    target_class = luigi.Parameter()
    flag_target_class = luigi.Parameter()
    parser = luigi.Parameter()
    datastore = luigi.Parameter()

    def requires(self):
        return ParseFile(self.path, self.target_class, self.parser)

    def load_dataset(self):
        df = pd.read_csv(self.input().open('r'),
                dtype={"zipcode": str, "weather_station": str})
        df.baseline_period_end = pd.to_datetime(df.baseline_period_end)
        df.reporting_period_start = pd.to_datetime(df.reporting_period_start)
        return df

    def write_flag(self):
        target = self.target_class(os.path.join(self._get_uploaded_file_path(), "_SUCCESS"))
        with target.open('w') as f:
            pass

    def run(self):
        parsed_project = self.load_dataset()
        project_results = upload_project_dataframe(parsed_project, self.datastore)
        self.write_flag()

    def _get_uploaded_file_path(self):
        split_path = self.path.split('/')

        # Change parent dir from 'raw' to 'uploaded'.
        split_path[-2] = 'uploaded'

        # Drop file extension
        filename = os.path.splitext(split_path[-1])[0]

        split_path[-1] = filename
        split_path.append("") # trailing slash
        return '/'.join(split_path)

    def output(self):
        return self.flag_target_class(self._get_uploaded_file_path())

class UploadConsumption(luigi.Task):
    path = luigi.Parameter()
    target_class = luigi.Parameter()
    flag_target_class = luigi.Parameter()
    parser = luigi.Parameter()
    datastore = luigi.Parameter()

    def requires(self):
        return ParseFile(self.path, self.target_class, self.parser)

    def load_dataset(self):
        df = pd.read_csv(self.input().open('r'),
                dtype={"zipcode": str, "weather_station": str})
        df.start = pd.to_datetime(df.start)
        df.end = pd.to_datetime(df.end)
        return df

    def write_flag(self):
        target = self.target_class(os.path.join(self._get_uploaded_file_path(), "_SUCCESS"))
        with target.open('w') as f:
            pass

    def run(self):
        parsed_consumption = self.load_dataset()
        consumption_results = upload_consumption_dataframe(parsed_consumption, self.datastore)
        self.write_flag()

    def _get_uploaded_file_path(self):
        split_path = self.path.split('/')

        # Change parent dir from 'raw' to 'uploaded'.
        split_path[-2] = 'uploaded'

        # Drop file extension
        filename = os.path.splitext(split_path[-1])[0]

        split_path[-1] = filename
        split_path.append("") # trailing slash
        return '/'.join(split_path)

    def output(self):
        return self.flag_target_class(self._get_uploaded_file_path())

class UploadDatasets(luigi.WrapperTask):
    raw_project_paths = luigi.Parameter()
    raw_consumption_paths = luigi.Parameter()
    project_parser = luigi.Parameter()
    consumption_parser = luigi.Parameter()
    target_class = luigi.Parameter()
    flag_target_class = luigi.Parameter()
    datastore = luigi.Parameter()

    def requires(self):
        parsed_projects = [
            UploadProject(path, self.target_class, self.flag_target_class,
                self.project_parser, self.datastore)
            for path in self.raw_project_paths
        ]
        parsed_consumption = [
            UploadConsumption(path, self.target_class, self.flag_target_class,
                self.consumption_parser, self.datastore)
            for path in self.raw_consumption_paths
        ]

        # Is there any guarantee of ordering here?
        return [parsed_projects, parsed_consumption]
