import luigi
from luigi.s3 import S3Target, S3Client
from luigi.format import MixedUnicodeBytesFormat

class FetchToS3(luigi.Task):

    s3_key = luigi.Parameter()
    fetch_callable = luigi.Parameter()
    s3_profile = luigi.Parameter()

    def run(self):
        with self.output().open('w') as target:
            target.write(self.fetch_callable())

    def output(self):
        return S3Target(self.s3_key, format=MixedUnicodeBytesFormat(), client=self._s3_client())

    def _s3_client(self):
        return S3Client(profile_name=self.s3_profile)

class FetchToLocal(luigi.Task):

    filepath = luigi.Parameter()
    fetch_callable = luigi.Parameter()

    def run(self):
        with self.output().open('w') as target:
            target.write(self.fetch_callable())

    def output(self):
        return luigi.LocalTarget(self.filepath)
