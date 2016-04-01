import luigi
from luigi.s3 import S3Target, S3Client
import json
import pandas as pd



class Reconcile(luigi.Task):

    fetched_project_s3_key_generator = luigi.Parameter(description="Generator of fetched project s3 keys")
    fetched_consumption_s3_key_generator= luigi.Parameter(description="Generator of fetched consumption s3 keys")

    parsed_project_s3_key_generator = luigi.Parameter(description="Generator of parsed project s3 keys")
    parsed_consumption_s3_key_generator= luigi.Parameter(description="Generator of parsed consumption s3 keys")

    project_parser_callable = luigi.Parameter(description="Project parser callable")
    consumption_parser_callable = luigi.Parameter(description="Consumption parser callable")

    project_fetcher_callable = luigi.Parameter(description="Project fetcher callable")
    consumption_fetcher_callable = luigi.Parameter(description="Consumption fetcher callable")
    consumption_reconciler_callable = luigi.Parameter(description="Callable that reconciles consumption to projects")

    s3_profile = luigi.Parameter()

    def output(self):
        return {
            "projects": S3Target(self.reconciled_projects_s3_key, format=MixedUnicodeBytesFormat(), client=self._s3_client()),
            "consumption": S3Target(self.reconciled_consumption_s3_key, format=MixedUnicodeBytesFormat(), client=self._s3_client()),
        }
