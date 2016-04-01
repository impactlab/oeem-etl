import luigi
from luigi.s3 import S3Target, S3Client
from luigi.format import MixedUnicodeBytesFormat

from .fetch import FetchToS3

class ParseS3ToS3(luigi.Task):
    s3_key_fetched = luigi.Parameter()
    s3_key_parsed = luigi.Parameter()
    parser_callable = luigi.Parameter()
    fetcher_callable = luigi.Parameter()
    s3_profile = luigi.Parameter()

    def requires(self):
        return FetchToS3(
            self.s3_key_fetched,
            self.fetcher_callable,
            self.s3_profile,
        )

    def run(self):
        input_file = self.input().open('r')
        with self.output().open('w') as f:
            f.write(self.parser_callable(input_file))

    def output(self):
        return S3Target(self.s3_key_parsed, format=MixedUnicodeBytesFormat(), client=self._s3_client())

    def _s3_client(self):
        return S3Client(profile_name=self.s3_profile)

class ParseLocalToLocal(luigi.Task):
    filepath_fetched = luigi.Parameter()
    filepath_parsed = luigi.Parameter()
    parser_callable = luigi.Parameter()
    fetcher_callable = luigi.Parameter()

    def requires(self):
        return FetchToS3(
            self.filepath_fetched,
            self.fetcher_callable,
        )

    def run(self):
        input_file = self.input().open('r')
        with self.output().open('w') as f:
            f.write(self.parser_callable(input_file))

    def output(self):
        return luigi.LocalTarget(self.filepath_parsed)
