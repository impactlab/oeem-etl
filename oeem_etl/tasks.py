#/usr/bin/env python
# -*- utf-8 -*-

import os
import json
from datetime import datetime

import luigi
import pandas as pd
import arrow
import pytz

from .fetchers import client_project_keys
from eemeter import parsers
from .parsers import project_parser, parse_usage_data, \
                             parse_account_data
from .reconcilers import link_projects_to_consumption
from .uploader import upload_dicts, upload_dataframes


def path_to_filename(path):
    '''
    Extract filename without file extension from
    path string.
    '''
    return os.path.basename(os.path.splitext(path)[0])

# TODO: fix unorderable types bug
# TODO: use s3 targets instead of local ones.
# TODO: rationalize s3 filename paths for the entire pipeline.
# TODO: go through oeem_etl functions, rename and find a final location for them. (especially reconcilers)
# TODO: related: move generic tasks to library, and rewrite this as importing a final task.
# TODO: go over reconciler algorithm with matt.
# TODO: finalize parsed file data structure - keys make sense? customer -> account.
# TODO: remove double list from consumption usage records.
# TODO: figure out what dates we get back from the API
# TODO: save data to daily files.
# TODO: get date-range parameter working?
# TODO: will backfilling work if service goes down for a few days?
# TODO: write tests.
# TODO: go through line by line and figure out what might go wrong. maybe do this in conjunction with code review.
# TODO: also think through what happens if API responses change - new customer usagepoint, for example. (log this)
# TODO: log key things.
class FetchS3File(luigi.Task):
    '''
    Get user-specified file from S3.
    '''
    s3_key = luigi.Parameter()

    def run(self):
        file_content = self.s3_key.get_contents_as_string(encoding='utf-8')
        with self.output().open('w') as target:
            target.write(file_content)

    def output(self):
        filename = os.path.basename(self.s3_key.name)
        return luigi.LocalTarget('data/project/raw/' + filename)


class ParseProjectFile(luigi.Task):
    '''
    Parse individual project file with known data format
    into oeem uploader json format.
    '''
    s3_key = luigi.Parameter()
    dataformat = luigi.Parameter()

    def requires(self):
        return FetchS3File(self.s3_key)

    def run(self):
        # TODO: comment this. Why do we need project id?
        project_id = self.input().path.split('/')[-1].split('.')[0]
        raw_project = self.input().open('r').read()
        parsed_project = project_parser[self.dataformat](project_id,
                                                         raw_project)
        with self.output().open('w') as target:
            target.write(json.dumps(parsed_project))

    def output(self):
        filename = path_to_filename(self.s3_key.name)
        return luigi.LocalTarget('data/project/parsed/' + filename + '.json')


class FetchCustomerAccount(luigi.Task):
    subscription_id = luigi.IntParameter()
    access_token = luigi.Parameter()
    espi = luigi.Parameter()

    def run(self):
        xml = espi.fetch_customer_account(self.subscription_id,
                                          self.access_token)
        with self.output().open('w') as target:
            target.write(xml)

    def output(self):
        filename = '{}.xml'.format(str(self.subscription_id))
        return luigi.LocalTarget('data/consumption/account/' + filename)


class FetchCustomerUsage(luigi.Task):
    subscription_id = luigi.Parameter()
    access_token = luigi.Parameter()
    usage_point_id = luigi.Parameter()
    usage_point_category = luigi.Parameter()
    espi = luigi.Parameter()
    date_range = luigi.Parameter(default='all_data')
    min_date = arrow.get(2015,3,21, 7)
    max_date = arrow.get(2016,3,21, 7)

    def find_data_range(self, readings):
        min_start = readings[0]["start"]
        max_end = readings[0]["end"]
        for reading in readings:
            if reading['start'] < min_start:
                min_start = reading['start']
            if reading['end'] > max_end:
                max_end = reading['end']
        print('OMG', arrow.get(min_start).strftime('%Y-%m-%dT%H:%M:%SZ'), arrow.get(max_end).strftime('%Y-%m-%dT%H:%M:%SZ'))

    def run(self):
        # TODO: SETTLE THIS.
        # xml = ESPI.fetch_usage_data(self.subscription_id,
        #                             self.usage_point_id,
        #                             self.access_token,
        #                             self.date_range)
        xml = self.espi.fetch_usage_data2(self.subscription_id,
                                          self.usage_point_id,
                                          self.access_token,
                                          self.min_date,
                                          self.max_date)

        readings = parse_usage_data(xml, self.usage_point_category)
        self.find_data_range(readings)

        with self.output().open('w') as target:
            target.write(xml)

    def output(self):
        filename = self.subscription_id + '-' + self.usage_point_category \
                   + '-' + self.date_range + '.xml'
        return luigi.LocalTarget('data/consumption/raw/' + filename)


class SaveCustomerUsage(luigi.Task):
    subscription_id = luigi.Parameter()
    usage_point_category = luigi.Parameter()
    min_date = luigi.DateParameter()
    max_date = luigi.DateParameter()
    xml = luigi.Parameter()

    def run(self):
        with self.output().open('w') as target:
            target.write(self.xml)

    def output(self):
        filename = '{}_{}_{}_{}.xml'.format(self.subscription_id,
                                            self.usage_point_category,
                                            self.min_date.for_json(),
                                            self.max_date.for_json())
        return local.Target('data/consumption/raw/' + filename)


class ParseCustomerUsage(luigi.Task):
    subscription_id = luigi.Parameter()
    usage_point_category = luigi.Parameter()
    min_date = luigi.DateParameter()
    max_date = luigi.DateParameter()
    xml = luigi.Parameter()

    def requires(self):
        return SaveCustomerUsage(self.subscription_id,
                                 self.usage_point_category,
                                 self.min_date, self.max_date,
                                 self.xml)

    def run(self):
        with self.output().open('w') as target:
            xml = self.input().open('r').read()
            gbp = parsers.ESPIUsageParser(xml)
            # ConsumptionData objects check for data errors, and return
            # energy values in correct units. It groups records by fuel type.
            # In our case, we only have one, so get records from the first
            # object returned.
            usage_records = list(gbp.get_consumption_data_objects())[0].records()
            target.write(json.dumps(usage_records))

    def output(self):
        filename = '{}_{}_{}_{}.json'.format(self.subscription_id,
                                             self.usage_point_category,
                                             self.min_date,
                                             self.max_date)
        return local.Target('data/consumption/parsed/' + filename)


class ParseCustomerAccount(luigi.Task):
    subscription_id = luigi.IntParameter()
    access_token = luigi.Parameter()
    espi = luigi.Parameter()

    def requires(self):
        return FetchCustomerAccount(self.subscription_id, self.access_token, self.espi)

    def run(self):
        with self.output().open('w') as target:
            xml = self.input().open('r').read()
            parsed_account = parse_account_data(xml)
            target.write(json.dumps(parsed_account))

    def output(self):
        filename = '{}.json'.format(self.subscription_id)
        return local.Target('data/consumption/accounts/' + filename)


class ParseAllCustomers(luigi.Task):
    config = luigi.Parameter()

    def requires(self):
        parsed_account_usage = []
        green_button = self.config['GREEN_BUTTON']
        espi = self.config['ESPI'],
        bucket = self.config['S3_BUCKET']
        for c in green_button.client_customers(self.config['CLIENT_ID']):
            for up_cat, min_date, max_date, xml in fetch_all_customer_usage(c['subscription_id'],
                                                                            c['active_access_token'],
                                                                            espi, bucket):
                parsed_account_usage.append((ParseCustomerAccount(c['subscription_id'],
                                                                  c['active_access_token'],
                                                                  espi),
                                             ParseCustomerUsage(c['subscription_id'],
                                                                p_cat, min_date,
                                                                max_date, xml)))
        return parsed_account_usage


class ParseConsumptionFile(luigi.Task):
    subscription_id = luigi.IntParameter()
    access_token = luigi.Parameter()
    espi = luigi.Parameter()
    date_range = luigi.Parameter(default='all_data')

    def requires(self):
        account = FetchCustomerAccount(self.subscription_id, self.access_token, self.espi)
        usage = [FetchCustomerUsage(self.subscription_id, self.access_token, up_id, up_cat, self.espi)
                 for up_id, up_cat in self.espi.fetch_usage_points(self.subscription_id,
                                                               self.access_token)]

        new_usage = [SaveCustomerUsage(self.subscrption_id, up_cat, min_date, max_date, usage_xml)
                     for up_cat, min_date, max_date, usage_xml
                     in fetch_all_customer_usage(self.subscription_id, self.access_token, self.espi)]
        # TODO: rename to customer
        return {'account': account, 'usage': usage}

    def parse_data(self, target):
        xml = target.open('r').read()
        if 'account' in target.path:
            return parse_account_data(xml)
        else:
            # Parse consumption data.
            gbp = parsers.ESPIUsageParser(xml)
            # ConsumptionData objects check for data errors, and return
            # energy values in correct units. It groups records by fuel type.
            # In our case, we only have one, so get records from the first
            # object returned.
            return list(gbp.get_consumption_data_objects())[0].records()
            # usage_point_cat = os.path.basename(target.path).split('-')[1]
            # return parse_usage_data(xml, usage_point_cat)

    def run(self):
        with self.output().open('w') as target:
            input_files = self.input()
            out = {}
            out['account'] = self.parse_data(input_files['account'])
            out['usage'] = [self.parse_data(target) for target in input_files['usage']]
            target.write(json.dumps(out))

    def output(self):
        filename = self.subscription_id + '-' + self.date_range + '.json'
        return luigi.LocalTarget('data/consumption/parsed/' + filename)


class ConnectProjectsToConsumption(luigi.Task):
    config = luigi.Parameter()

    def requires(self):
        # TODO: break if either projects or consumption is 0.
        parsed_projects = [ParseProjectFile(key, self.config['PROJECT_DATA_FORMAT'])
                           for key in client_project_keys(self.config['S3_BUCKET'],
                                                          self.config['S3_CLIENT_PATH'])]
        green_button = self.config['GREEN_BUTTON']
        parsed_consumption = [ParseConsumptionFile(c['subscription_id'],
                                                   c['active_access_token'],
                                                   self.config['ESPI'])
                              for c in green_button.client_customers(self.config['CLIENT_ID'])]
        return {'projects': parsed_projects, 'consumption': parsed_consumption}

    def load_datasets(self, input_dict, key):
        return [json.loads(target.open('r').read()) for target in input_dict[key]]

    def run(self):
        parsed_projects = self.load_datasets(self.input(), 'projects')
        parsed_consumption = self.load_datasets(self.input(), 'consumption')
        projects, consumption = link_projects_to_consumption(parsed_projects,
                                                             parsed_consumption)
        out = {'projects': projects, 'consumption': consumption}
        with self.output().open('w') as target:
            target.write(json.dumps(out))

    def output(self):
        filename = 'data/parsed/datasets-danny'
        return luigi.LocalTarget(filename)


# TODO: refactor this task to not be insane.
class UploadDatasets(luigi.Task):
    config = luigi.Parameter()

    def requires(self):
        return ConnectProjectsToConsumption(self.config)

    def convert_datetime(self, consumption):
        readings = []
        for person in consumption:
            for service_cat in person:
                for reading in service_cat:
                    pacific = pytz.timezone('US/Pacific')
                    reading['end'] = datetime.fromtimestamp(reading['end'], pacific).astimezone(pytz.UTC).replace(tzinfo=None)
                    reading['start'] = datetime.fromtimestamp(reading['start'], pacific).astimezone(pytz.UTC).replace(tzinfo=None)
                    reading['estimated'] = False
                    readings.append(reading)
        return readings

    def resample_consumption(self, consumption_df):
        # Turn consumption into a monthly timeseries.
        consumption_df_ts = consumption_df.set_index('start')
        def resample_monthly(series):
            return series.astype(int).resample('M', how='sum')

        result = consumption_df_ts.groupby('fuel_type').value.apply(resample_monthly).reset_index().rename(columns={0: 'value'})
        # Add fields back in.
        result['estimated'] = False
        result['unit_name'] = None
        has_electricity = result['fuel_type'] == 'electricity'
        result.loc[has_electricity, 'unit_name'] = 'kWh'
        has_natural_gas = result['fuel_type'] == 'natural_gas'
        result.loc[has_natural_gas, 'unit_name'] = 'therms'
        result['project_d'] = consumption_df['project_id'][0]
        return result


    def run(self):
        datasets = json.loads(self.input().open('r').read())
        all_readings = self.convert_datetime(datasets['consumption'])
        just_projects = [p['project'] for p in datasets['projects']]
        # upload_dicts(just_projects, all_readings,
        #                       DATASTORE_URL, DATASTORE_TOKEN, PROJECT_OWNER,
        #                       verbose=True)
        # Use upload dataframes until upload_dicts is working.
        global project_df
        global consumption_df
        project_df = pd.DataFrame(just_projects)
        consumption_df = pd.DataFrame(all_readings)
        project_df['baseline_period_end'] = pd.to_datetime(project_df.baseline_period_end)
        project_df['reporting_period_start'] = pd.to_datetime(project_df.reporting_period_start)
        # TODO: TRY UPLOAD DATAFRAMES2
        upload_dataframes(project_df, consumption_df.head(10000),
                                   self.config['DATASTORE_URL'],
                                   self.config['DATASTORE_TOKEN'],
                                   self.config['PROJECT_OWNER'],
                                   verbose=True)
