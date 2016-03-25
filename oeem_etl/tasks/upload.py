import luigi
import yaml
from .reconcile import Reconcile


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
        settings = yaml.load(open(self.path_to_settings, 'r').read())

        # get connected projects and consumptions
        datasets = Reconcile(
            self.fetched_project_s3_key_generator,
            self.fetched_consumption_s3_key_generator,
            self.parsed_project_s3_key_generator,
            self.parsed_consumption_s3_key_generator,
            self.project_parser_callable,
            self.consumption_parser_callable,
            self.project_fetcher_callable,
            self.consumption_fetcher_callable,
            settings['s3_bucket'],
            settings['aws_access_key_id'],
            settings['aws_secret_access_key'],
        )

        return datasets

    def run(self):
        datasets = json.loads(self.input().open('r').read())

        all_readings = self._convert_datetime(datasets['consumption'])
        just_projects = [p['project'] for p in datasets['projects']]

        project_df = pd.DataFrame(just_projects)
        consumption_df = pd.DataFrame(all_readings)

        project_df['baseline_period_end'] = pd.to_datetime(project_df.baseline_period_end)
        project_df['reporting_period_start'] = pd.to_datetime(project_df.reporting_period_start)

        # TODO: TRY UPLOAD DATAFRAMES2
        upload_dataframes(project_df, consumption_df.head(10000),
                                   self.config['DATASTORE_URL'],
                                   self.config['DATASTORE_TOKEN'],
                                   self.config['PROJECT_OWNER'],
                                   verbose=True)
