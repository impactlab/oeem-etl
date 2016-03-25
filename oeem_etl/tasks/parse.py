import luigi
from luigi.s3 import S3Target, S3Client
from luigi.format import MixedUnicodeBytesFormat

from .fetch import FetchToS3

class ParseS3ToS3(luigi.Task):
    s3_key_fetched = luigi.Parameter()
    s3_key_parsed = luigi.Parameter()
    parser_callable = luigi.Parameter()
    fetcher_callable = luigi.Parameter()
    aws_access_key_id = luigi.Parameter()
    aws_secret_access_key = luigi.Parameter()

    def requires(self):
        return FetchToS3(
            self.s3_key_fetched,
            self.fetcher_callable,
            self.aws_access_key_id,
            self.aws_secret_access_key
        )

    def run(self):
        input_file = self.input().open('r')
        with self.output().open('w') as f:
            f.write(self.parser_callable(input_file))

    def output(self):
        return S3Target(self.s3_key_parsed, format=MixedUnicodeBytesFormat(), client=self._s3_client())

    def _s3_client(self):
        return S3Client(self.aws_access_key_id, self.aws_secret_access_key)

