import os
import luigi


class File(luigi.ExternalTask):
    raw_file_path = luigi.Parameter()
    target_class = luigi.Parameter()

    def output(self):
        return self.target(self.raw_file_path)


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

    def _get_parsed_file_path(self, path):
        '''
        Create parsed file path that corresponds to raw file.

        Example:
        <client-bucket>/projects/raw/2015-06-01.xml
        <client-bucket>/projects/parsed/2015-06-01.csv
        '''
        split_path = path.split('/')
        # Change parent dir from 'raw' to 'parsed'.
        split_path[-2] = 'parsed'
        # Change file extension to csv.
        filename = os.path.splitext(split_path[-1])[0]
        split_path[-1] = filename + '.csv'
        return '/'.join(split_path)

    def output(self):
        # Parsed files should have the same filename as raw ones,
        # but placed in a sibling 'parsed' directory, and saved to csv.
        parsed_file_path = self._get_parsed_file_path(self.raw_file_path)
        return self.target_class(parsed_file_path)
