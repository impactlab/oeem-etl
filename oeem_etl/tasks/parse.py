import luigi

class ParseS3toS3(luigi.Task):
    aws_access_key_id = luigi.Parameter()
    aws_secret_access_key = luigi.Parameter()
    s3_key_input = luigi.Parameter()
    s3_key_output = luigi.Parameter()
    parser_callable = luigi.Parameter()

    def input(self):
        return luigi.s3.S3Target(self.s3_key_input, self._s3_client())

    def run(self):
        input_file = self.input()

    def output(self):
        return luigi.s3.S3Target(self.s3_key_output, self._s3_client())

    def _s3_client(self):
        return luigi.s3.S3Client(self.aws_access_key_id, self.aws_secret_access_key)
