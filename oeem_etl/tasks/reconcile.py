import luigi
import json

from .parse import ParseS3ToS3


class Reconcile(luigi.Task):

    fetched_project_s3_key_generator = luigi.Parameter(description="Generator of fetched project s3 keys")
    fetched_consumption_s3_key_generator= luigi.Parameter(description="Generator of fetched consumption s3 keys")

    parsed_project_s3_key_generator = luigi.Parameter(description="Generator of parsed project s3 keys")
    parsed_consumption_s3_key_generator= luigi.Parameter(description="Generator of parsed consumption s3 keys")

    project_parser_callable = luigi.Parameter(description="Project parser callable")
    consumption_parser_callable = luigi.Parameter(description="Consumption parser callable")

    project_fetcher_callable = luigi.Parameter(description="Project fetcher callable")
    consumption_fetcher_callable = luigi.Parameter(description="Consumption fetcher callable")

    s3_bucket = luigi.Parameter()
    aws_access_key_id = luigi.Parameter()
    aws_secret_access_key = luigi.Parameter()

    def requires(self):

        # get parsed projects
        parsed_projects = [ ParseS3ToS3(
            s3_key_fetched,
            s3_key_parsed,
            self.project_parser_callable,
            self.project_fetcher_callable,
            self.aws_access_key_id,
            self.aws_secret_access_key,
        ) for s3_key_fetched, s3_key_parsed in zip(
            self.fetched_project_s3_key_generator(),
            self.parsed_project_s3_key_generator())]

        # get parsed consumptions
        parsed_consumption = [ ParseS3ToS3(
            s3_key_fetched,
            s3_key_parsed,
            self.consumption_parser_callable,
            self.consumption_fetcher_callable,
            self.aws_access_key_id,
            self.aws_secret_access_key,
        ) for s3_key_fetched, s3_key_parsed in zip(
            self.fetched_consumption_s3_key_generator(),
            self.parsed_consumption_s3_key_generator())]

        return {
            'projects': parsed_projects,
            'consumption': parsed_consumption,
        }

    def load_datasets(self, input_dict, key):
        return [pd.read_csv(target.open('r').read()) for target in input_dict[key]]

    def run(self):
        parsed_projects = self.load_datasets(self.input(), 'projects')
        parsed_consumption = self.load_datasets(self.input(), 'consumption')
        projects, consumption = self.link_projects_to_consumption(
                parsed_projects, parsed_consumption)

        out = {'projects': projects, 'consumption': consumption}
        with self.output().open('w') as target:
            target.write(json.dumps(out))

    def output(self):
        filename = 'data/parsed/datasets-danny'
        return luigi.LocalTarget(filename)
