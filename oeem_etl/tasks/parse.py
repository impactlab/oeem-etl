import os
import luigi

from .shared import mangle_path

class File(luigi.ExternalTask):
    raw_file_path = luigi.Parameter()
    target_class = luigi.Parameter()

    def output(self):
        return self.target_class(self.raw_file_path)


class ParseFile(luigi.Task):
    raw_file_path = luigi.Parameter()
    target_class = luigi.Parameter()
    parser = luigi.Parameter()

    def requires(self):
        return File(self.raw_file_path, self.target_class)

    def run(self):
        raw_data = self.input().open('r')
        with self.output().open('w') as f:
            f.write(self.parser(self.raw_file_path, raw_data))

    def output(self):
        # Parsed files should have the same filename as raw ones,
        # but placed in a sibling 'parsed' directory, and saved to csv.
        parsed_file_path = mangle_path(self.raw_file_path, 'raw', 'parsed')
        return self.target_class(parsed_file_path)
