import luigi

class FetchGreenButtontoS3(luigi.Task):

    aws_access_key_id = luigi.Parameter()
    aws_secret_access_key = luigi.Parameter()
    s3_key_output = luigi.Parameter()
    start_date = luigi.Parameter()
    end_date = luigi.Parameter()

    def run(self):
        import pdb; pdb.set_trace()

    def output(self):
        return luigi.s3.S3Target(self.s3_key_output, self._s3_client())

    def _s3_client(self):
        return luigi.s3.S3Client(self.aws_access_key_id, self.aws_secret_access_key)
