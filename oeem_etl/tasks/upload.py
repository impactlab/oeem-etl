import luigi
import yaml
from .parse import ParseS3ToS3
from ..uploader import (
    upload_project_dataframe,
    upload_consumption_dataframe,
)
import pandas as pd

class UploadDatasets(luigi.Task):

    path_to_settings = luigi.Parameter(
            description="Path to settings YAML file")

    fetched_project_s3_key_generator = luigi.Parameter(
            description="Generator of parsed project s3 keys")
    fetched_consumption_s3_key_generator= luigi.Parameter(
            description="Generator of parsed consumption s3 keys")

    parsed_project_s3_key_generator = luigi.Parameter(
            description="Generator of parsed project s3 keys")
    parsed_consumption_s3_key_generator= luigi.Parameter(
            description="Generator of parsed consumption s3 keys")

    project_parser_callable = luigi.Parameter(
            description="Project parser callable")
    consumption_parser_callable = luigi.Parameter(
            description="Consumption parser callable")

    project_fetcher_callable = luigi.Parameter(
            description="Project parser callable")
    consumption_fetcher_callable = luigi.Parameter(
            description="Consumption parser callable")

    def requires(self):
        self.settings = yaml.load(open(self.path_to_settings, 'r').read())

        # get connected projects and consumptions
        parsed_projects = [ ParseS3ToS3(
            s3_key_fetched,
            s3_key_parsed,
            self.project_parser_callable,
            self.project_fetcher_callable,
            self.settings["s3_profile"],
        ) for s3_key_fetched, s3_key_parsed in zip(
            self.fetched_project_s3_key_generator(),
            self.parsed_project_s3_key_generator())]

        # get parsed consumptions
        parsed_consumption = [ ParseS3ToS3(
            s3_key_fetched,
            s3_key_parsed,
            self.consumption_parser_callable,
            self.consumption_fetcher_callable,
            self.settings["s3_profile"],
        ) for s3_key_fetched, s3_key_parsed in zip(
            self.fetched_consumption_s3_key_generator(),
            self.parsed_consumption_s3_key_generator())]

        return {
            'projects': parsed_projects,
            'consumption': parsed_consumption,
        }

    def load_datasets(self, input_dict, key):
        return pd.concat([pd.read_csv(target.open('r')) for target in input_dict[key]])

    def run(self):
        project_df = self.load_datasets(self.input(), 'projects')
        project_df.baseline_period_end = pd.to_datetime(project_df.baseline_period_end)
        project_df.reporting_period_start = pd.to_datetime(project_df.reporting_period_start)
        consumption_df = self.load_datasets(self.input(), 'consumption')
        consumption_df.start = pd.to_datetime(consumption_df.start)
        consumption_df.end = pd.to_datetime(consumption_df.end)

        url = self.settings["datastore_url"]
        access_token = self.settings["datastore_token"]
        project_owner = self.settings["project_owner"]

        project_results = upload_project_dataframe(project_df, url, access_token, project_owner)
        consumption_results = upload_consumption_dataframe(consumption_df, url, access_token)

        import pdb; pdb.set_trace()
