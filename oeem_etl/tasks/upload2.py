"""
Upload tasks to be imported in client-specific parsers.

Designed to replace the tasks in `upload.py`.

Expects files to be laid out in the following directory structure:

    /oeem-format
        /projects
        /consumptions

There can nested subdirectories in `projects` and `consumptions`.

Top-level tasks:

    UploadProjects
    UploadConsumptions
    LoadAll

"""

import os
import pandas as pd
import numpy as np
import luigi
from oeem_etl.uploader import upload_project_dataframe, upload_consumption_dataframe_faster
import oeem_etl.config as config


OEEM_FORMAT_OUTPUT_BASE_PATH = 'oeem-format'
OEEM_FORMAT_PROJECT_OUTPUT_DIR = os.path.join(OEEM_FORMAT_OUTPUT_BASE_PATH, 'projects')
OEEM_FORMAT_CONSUMPTIONS_OUTPUT_DIR = os.path.join(OEEM_FORMAT_OUTPUT_BASE_PATH, 'consumptions')

class FetchFile(luigi.ExternalTask):
    """Fetchs file from either local disk or cloud storage"""
    raw_file_path = luigi.Parameter()

    def output(self):
        return config.oeem.target_class(self.raw_file_path)

class UploadProject(luigi.Task):
    path = luigi.Parameter()

    def requires(self):
        return FetchFile(self.path)

    def load_dataset(self):
        df = pd.read_csv(self.input().open('r'),
                dtype={"zipcode": str, "weather_station": str})
        df.baseline_period_end = pd.to_datetime(df.baseline_period_end)
        df.reporting_period_start = pd.to_datetime(df.reporting_period_start)
        return df

    def write_flag(self):
        uploaded_path = formatted2uploaded_path(self.path)
        target = config.oeem.target_class(os.path.join(uploaded_path, "_SUCCESS"))
        with target.open('w') as f:
            pass

    def run(self):
        parsed_project = self.load_dataset()
        project_results = upload_project_dataframe(parsed_project, config.oeem.datastore)
        self.write_flag()

    def output(self):
        uploaded_path = formatted2uploaded_path(self.path)
        return config.oeem.flag_target_class(uploaded_path)


class UploadProjects(luigi.WrapperTask):
    project_paths = luigi.ListParameter()
    
    def requires(self):
        return [UploadProject(path) for path in self.project_paths]


def mirror_path(path, from_path, to_path):
    """Replaces the prefix `from_path` on `path` with `to_path`

    e.g.

    mirror_path("/home/user/data/client/raw/consumptions/000.csv", 
                "/home/user/data/client/raw",
                "/home/user/data/client/uploaded")
    
    returns

    "/home/user/data/client/uploaded/consumptions/000.csv"    

    """

    # Returns path with any directories nested under `raw` and the filename
    nested_path = os.path.relpath(path, from_path)    
    output_path = os.path.join(to_path, nested_path)
    return output_path

def formatted2uploaded_path(path):
    return mirror_path(path, 
                       config.oeem.full_path(config.oeem.formatted_base_path), 
                       config.oeem.full_path(config.oeem.uploaded_base_path))

class UploadConsumption(luigi.Task):
    path = luigi.Parameter()
    project_paths = luigi.ListParameter()
    
    def requires(self):
        return {
            'file': FetchFile(self.path),
            'projects': UploadProjects(self.project_paths)
        }

    def load_dataset(self):

        def maybe_float(value):
            try:
                return float(value)
            except:
                return np.nan

        df = pd.read_csv(self.input()['file'].open('r'), dtype={
            'project_id': str,
        }, converters={
            'value': maybe_float
        })
        df = df.dropna()
        return df

    def write_flag(self):
        uploaded_path = formatted2uploaded_path(self.path)
        target = config.oeem.target_class(os.path.join(uploaded_path, "_SUCCESS"))
        with target.open('w') as f:
            pass

    def run(self):
        parsed_consumption = self.load_dataset()
        consumption_results = upload_consumption_dataframe_faster(parsed_consumption, config.oeem.datastore)
        self.write_flag()

    def output(self):
        return config.oeem.flag_target_class(formatted2uploaded_path(self.path))


class LoadAll(luigi.WrapperTask):

    def requires(self):

        project_paths     = config.oeem.storage.get_existing_paths(OEEM_FORMAT_PROJECT_OUTPUT_DIR)
        consumption_paths = config.oeem.storage.get_existing_paths(OEEM_FORMAT_CONSUMPTIONS_OUTPUT_DIR)

        def filter_paths(paths):
            return [path for path in paths if not path.endswith('.DS_Store')]

        project_paths = filter_paths(project_paths)
        consumption_paths = filter_paths(consumption_paths)

        return [
            UploadConsumption(path, project_paths)
            for path in consumption_paths
        ]



