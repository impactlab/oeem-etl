import luigi
import yaml
from .parse import ParseFile
from ..uploader import upload_project_dataframe, upload_consumption_dataframe
import pandas as pd

# TODO WHAT HAPPENS IF THIS CRASHES BEFORE BEING DONE
# TODO WHAT HAPPENS IF YOU CALL THIS TWICE? says it's done, why?
class UploadDatasets(luigi.WrapperTask):
    raw_project_paths = luigi.Parameter()
    raw_consumption_paths = luigi.Parameter()
    project_parser = luigi.Parameter()
    consumption_parser = luigi.Parameter()
    target = luigi.Parameter()
    datastore = luigi.Parameter()

    def requires(self):
        parsed_projects = [ParseFile(path, self.target,
                                     self.project_parser)
                           for path in self.raw_project_paths]
        # parsed_consumption = [ParseFile(path, self.target,
        #                                 self.consumption_parser)
        #                       for path in self.raw_consumption_paths]
        return {'projects': parsed_projects}
                #, 'consumption': parsed_projects}

    def load_datasets(self, targets, dataset_type):
        try:
            return pd.concat([pd.read_csv(target.open('r'))
                              for target in targets[dataset_type]])
        except ValueError:
            # TODO: raise error?
            print('No %s datasets to upload!' % dataset_type)  # TODO use logger

    def run(self):
        # Load projects into DataFrame and convert date columns to datetimes.
        parsed_projects = self.load_datasets(self.input(), 'projects')
        parsed_projects.baseline_period_end = pd.to_datetime(parsed_projects.baseline_period_end)
        parsed_projects.reporting_period_start = pd.to_datetime(parsed_projects.reporting_period_start)

        # Load consumption records and convert date columns to datetimes.
        # parsed_consumption = self.load_datasets(self.input(), 'consumption')
        # parsed_consumption.start = pd.to_datetime(parsed_consumption.start)
        # parsed_consumption.end = pd.to_datetime(parsed_consumption.end)

        # Upload parsed data to datastore.
        project_results = upload_project_dataframe(parsed_projects, self.datastore)
        # consumption_results = upload_consumption_dataframe(parsed_consumption, self.datastore)
