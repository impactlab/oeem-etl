import luigi
import yaml
from .parse import ParseFile
from ..uploader import upload_project_dataframe, upload_consumption_dataframe
import pandas as pd

# TODO WHAT HAPPENS IF THIS CRASHES BEFORE BEING DONE
# TODO WHAT HAPPENS IF YOU CALL THIS TWICE? says it's done, why?
# even when you update the code.

class UploadProject(luigi.Task):
    path = luigi.Parameter()
    target = luigi.Parameter()
    parser = luigi.Parameter()
    datastore = luigi.Parameter()

    def requires(self):
        return ParseFile(self.path, self.target, self.parser)

    def load_dataset(self):
        df = pd.read_csv(self.input().open('r'),
                dtype={"zipcode": str, "weather_station": str})
        df.baseline_period_end = pd.to_datetime(df.baseline_period_end)
        df.reporting_period_start = pd.to_datetime(df.reporting_period_start)
        return df

    def run(self):
        parsed_project = self.load_dataset()
        project_results = upload_project_dataframe(parsed_projects, self.datastore)

class UploadConsumption(luigi.Task):
    path = luigi.Parameter()
    target = luigi.Parameter()
    parser = luigi.Parameter()
    datastore = luigi.Parameter()

    def requires(self):
        return ParseFile(self.path, self.target, self.parser)

    def load_dataset(self):
        df = pd.read_csv(self.input().open('r'),
                dtype={"zipcode": str, "weather_station": str})
        df.start = pd.to_datetime(df.start)
        df.end = pd.to_datetime(df.end)
        return df

    def run(self):
        parsed_consumption = self.load_dataset()
        consumption_results = upload_consumption_dataframe(parsed_consumption, self.datastore)


class UploadDatasets(luigi.Task):
    raw_project_paths = luigi.Parameter()
    raw_consumption_paths = luigi.Parameter()
    project_parser = luigi.Parameter()
    consumption_parser = luigi.Parameter()
    target = luigi.Parameter()
    datastore = luigi.Parameter()

    def requires(self):
        parsed_projects = [
            UploadProject(path, self.target, self.project_parser, self.datastore)
            for path in self.raw_project_paths
        ]
        parsed_consumption = [
            UploadConsumption(path, self.target, self.consumption_parser, self.datastore)
            for path in self.raw_consumption_paths
        ]
        return {
            'projects': parsed_projects,
            'consumption': parsed_consumption
        }

    def run(self):
        pass
