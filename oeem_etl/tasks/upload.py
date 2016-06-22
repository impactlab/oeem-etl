import luigi
import yaml
from .parse import ParseFile
from ..uploader import upload_project_dataframe, upload_consumption_dataframe, upload_consumption_dataframe_faster
import pandas as pd
import os

from .shared import mangle_path
#
# class UploadProject(luigi.Task):
#     path = luigi.Parameter()
#     target_class = luigi.Parameter()
#     flag_target_class = luigi.Parameter()
#     parser = luigi.Parameter()
#     datastore = luigi.Parameter()
#
#     def requires(self):
#         return ParseFile(self.path, self.target_class, self.parser)
#
#     def load_dataset(self):
#         df = pd.read_csv(self.input().open('r'),
#                 dtype={"zipcode": str, "weather_station": str})
#         df.baseline_period_end = pd.to_datetime(df.baseline_period_end)
#         df.reporting_period_start = pd.to_datetime(df.reporting_period_start)
#         return df
#
#     def write_flag(self):
#         uploaded_path = mangle_path(self.path, 'raw', 'uploaded')
#         target = self.target_class(os.path.join(uploaded_path, "_SUCCESS"))
#         with target.open('w') as f:
#             pass
#
#     def run(self):
#         parsed_project = self.load_dataset()
#         project_results = upload_project_dataframe(parsed_project, self.datastore)
#         self.write_flag()
#
#     def output(self):
#         uploaded_path = mangle_path(self.path, 'raw', 'uploaded') + "/"
#         return self.flag_target_class(uploaded_path)
#
# class UploadConsumption(luigi.Task):
#     path = luigi.Parameter()
#     target_class = luigi.Parameter()
#     flag_target_class = luigi.Parameter()
#     parser = luigi.Parameter()
#     datastore = luigi.Parameter()
#
#     def requires(self):
#         return ParseFile(self.path, self.target_class, self.parser)
#
#     def load_dataset(self):
#         df = pd.read_csv(self.input().open('r'),
#                 dtype={"zipcode": str, "weather_station": str})
#         df.start = pd.to_datetime(df.start)
#         df.end = pd.to_datetime(df.end)
#         return df
#
#     def write_flag(self):
#         uploaded_path = mangle_path(self.path, 'raw', 'uploaded')
#         target = self.target_class(os.path.join(uploaded_path, "_SUCCESS"))
#         with target.open('w') as f:
#             pass
#
#     def run(self):
#         parsed_consumption = self.load_dataset()
#         consumption_results = upload_consumption_dataframe_faster(parsed_consumption, self.datastore)
#         self.write_flag()
#
#     def output(self):
#         uploaded_path = mangle_path(self.path, 'raw', 'uploaded') + "/"
#         return self.flag_target_class(uploaded_path)
#
# class UploadDatasets(luigi.WrapperTask):
#     raw_project_paths = luigi.Parameter()
#     raw_consumption_paths = luigi.Parameter()
#     project_parser = luigi.Parameter()
#     consumption_parser = luigi.Parameter()
#     target_class = luigi.Parameter()
#     flag_target_class = luigi.Parameter()
#     datastore = luigi.Parameter()
#
#     def requires(self):
#         parsed_projects = [
#             UploadProject(path, self.target_class, self.flag_target_class,
#                 self.project_parser, self.datastore)
#             for path in self.raw_project_paths
#         ]
#         parsed_consumption = [
#             UploadConsumption(path, self.target_class, self.flag_target_class,
#                 self.consumption_parser, self.datastore)
#             for path in self.raw_consumption_paths
#         ]
#
#         # Ordering matters
#         return [parsed_projects, parsed_consumption]
#
#














class UploadProject(luigi.Task):
    path = luigi.Parameter()
    target_class = luigi.Parameter()
    flag_target_class = luigi.Parameter()
    parser = luigi.Parameter()
    datastore = luigi.Parameter()

    def requires(self):
        return ParseFile(self.path, self.target_class, self.parser)

    def load_dataset(self):
        df = pd.read_csv(self.input().open('r'),
                dtype={"zipcode": str, "weather_station": str})
        df.baseline_period_end = pd.to_datetime(df.baseline_period_end)
        df.reporting_period_start = pd.to_datetime(df.reporting_period_start)
        return df

    def write_flag(self):
        uploaded_path = mangle_path(self.path, 'raw', 'uploaded')
        target = self.target_class(os.path.join(uploaded_path, "_SUCCESS"))
        with target.open('w') as f:
            pass

    def run(self):
        parsed_project = self.load_dataset()
        project_results = upload_project_dataframe(parsed_project, self.datastore)
        self.write_flag()

    def output(self):
        uploaded_path = mangle_path(self.path, 'raw', 'uploaded') + "/"
        return self.flag_target_class(uploaded_path)


class UploadProjects(luigi.WrapperTask):
    target_class = luigi.Parameter()
    flag_target_class = luigi.Parameter()
    datastore = luigi.Parameter()
    raw_project_paths = luigi.Parameter()
    project_parser = luigi.Parameter()

    def requires(self):
        return [
            UploadProject(path, self.target_class, self.flag_target_class,
                self.project_parser, self.datastore)
            for path in self.raw_project_paths
        ]


class UploadConsumption(luigi.Task):
    path = luigi.Parameter()
    target_class = luigi.Parameter()
    flag_target_class = luigi.Parameter()
    parser = luigi.Parameter()
    datastore = luigi.Parameter()
    raw_project_paths = luigi.Parameter()
    project_parser = luigi.Parameter()

    def requires(self):
        return {
            'projects': UploadProjects(self.target_class, self.flag_target_class,
                self.datastore, self.raw_project_paths, self.project_parser),
            'file': ParseFile(self.path, self.target_class, self.parser)
        }

    def load_dataset(self):
        df = pd.read_csv(self.input()['file'].open('r'),
                dtype={"zipcode": str, "weather_station": str})
        df.start = pd.to_datetime(df.start)
        df.end = pd.to_datetime(df.end)
        return df

    def write_flag(self):
        uploaded_path = mangle_path(self.path, 'raw', 'uploaded')
        target = self.target_class(os.path.join(uploaded_path, "_SUCCESS"))
        with target.open('w') as f:
            pass

    def run(self):
        parsed_consumption = self.load_dataset()
        consumption_results = upload_consumption_dataframe_faster(parsed_consumption, self.datastore)
        self.write_flag()

    def output(self):
        uploaded_path = mangle_path(self.path, 'raw', 'uploaded') + "/"
        return self.flag_target_class(uploaded_path)

class UploadDatasets(luigi.WrapperTask):
    raw_project_paths = luigi.Parameter()
    raw_consumption_paths = luigi.Parameter()
    project_parser = luigi.Parameter()
    consumption_parser = luigi.Parameter()
    target_class = luigi.Parameter()
    flag_target_class = luigi.Parameter()
    datastore = luigi.Parameter()

    def requires(self):
        return [
            UploadConsumption(path, self.target_class, self.flag_target_class,
                self.consumption_parser, self.datastore, self.raw_project_paths,
                self.project_parser)
            for path in self.raw_consumption_paths
        ]







