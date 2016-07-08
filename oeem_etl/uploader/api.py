from oeem_etl.uploader import constants
from oeem_etl.uploader.requester import Requester
from datetime import date, datetime
from eemeter.consumption import ConsumptionData
import dateutil.parser
import pandas as pd
import numpy as np
import pytz
import re
import json
import logging

__all__ = [
    'upload_project_dicts',
    'upload_consumption_dicts',

    'upload_project_csv',
    'upload_consumption_csv',

    'upload_project_dataframe',
    'upload_consumption_dataframe',
    'upload_consumption_dataframe_faster',
]


def upload_project_dicts(project_dict, url, access_token, project_owner):
    """Uploads project data in python dict format to a datastore instance.

    Parameters
    ----------
    project_dict : list of dicts
        List of dictionaries with contents something like the following::

            [
                {
                    "project_id": "ID_1",
                    "zipcode": "01234",
                    "weather_station": "012345",
                    "latitude": 89.0,
                    "longitude": -42.0,
                    "baseline_period_end": datetime(2015, 1, 1),
                    "reporting_period_start": datetime(2015, 2, 1),
                },
                ...
            ]

        Extra columns will be treated as project attributes.

    url : str
        URL of the target datastore, e.g. `https://datastore.openeemeter.org`
    access_token : str
        Access token for the target datastore.
    project_owner : int
        Primary key of project_owner for datastore.
    """

    df = pd.DataFrame(project_dict)
    return upload_project_dataframe(df, url, access_token, project_owner)

def upload_consumption_dicts(consumption_dict, url, access_token):
    """Uploads project data in python dict format to a datastore instance.

    Parameters
    ----------
    consumption_dict : list of dicts
        List of dictionaries with contents something like the following::

            [
                {
                    "project_id": "ID_1",
                    "start": datetime(2015, 1, 1),
                    "end": datetime(2015, 1, 2),
                    "fuel_type": "electricity",
                    "unit_name": "kWh",
                    "value": 0,
                    "estimated": True,
                },
                ...
            ]

    url : str
        URL of the target datastore, e.g. `https://datastore.openeemeter.org`
    access_token : str
        Access token for the target datastore.
    """

    df = pd.DataFrame(consumption_dict)
    return upload_consumption_dataframe_faster(df, url, access_token)

def upload_project_csv(project_csv_file, url, access_token, project_owner):
    """Uploads project data in CSV format to a datastore instance.

    Parameters
    ----------
    project_csv_file : file object
        File pointer to a CSV with the following columns::

            project_id,zipcode,weather_station,latitude,longitude,baseline_period_end,reporting_period_start

        Extra columns will be treated as project attributes.
    url : str
        Base URL of the target datastore.
    access_token : str
        Access token for the target datastore.
    project_owner : int
        Primary key of project_owner for datastore.
    """

    project_df = pd.read_csv(project_csv_file)
    project_df.baseline_period_end = pd.to_datetime(project_df.baseline_period_end)
    project_df.reporting_period_start = pd.to_datetime(project_df.reporting_period_start)
    return upload_project_dataframe(project_df, url, access_token, project_owner)

def upload_consumption_csv(consumption_csv_file, url, access_token):
    """Uploads consumption data in CSV format to a datastore instance.

    Parameters
    ----------
    consumption_csv_file : file object
        File pointer to a CSV with the following columns::

            project_id,start,end,fuel_type,unit_name,value,estimated

        Extra columns will be treated as project attributes.
    url : str
        Base URL of the target datastore.
    access_token : str
        Access token for the target datastore.
    """

    consumption_df = pd.read_csv(consumption_csv_file, dtype={'value': np.float})
    consumption_df.start = pd.to_datetime(consumption_df.start)
    consumption_df.end = pd.to_datetime(consumption_df.end)
    return upload_consumption_dataframe(consumption_df, url, access_token)

def upload_project_dataframe(project_df, datastore):
    """Uploads project data in pandas DataFrame format to a datastore instance.

    Parameters
    ----------
    project_df : pandas.DataFrame
        DataFrame with the following columns::

            project_id,zipcode,weather_station,latitude,longitude,baseline_period_end,reporting_period_start

        Extra columns will be treated as project attributes.
    url : str
        Base URL of the target datastore.
    access_token : str
        Access token for the target datastore.
    project_owner : int
        Primary key of project_owner for datastore.
    """
    requester = Requester(datastore['url'], datastore['access_token'])

    project_attribute_key_records = _get_project_attribute_keys_data(project_df)

    project_records = []
    project_attribute_records = []
    for project_data, project_attributes_data in \
            _get_project_data(project_df, project_attribute_key_records):

        project_data["project_owner_id"] = datastore['project_owner']
        project_records.append(project_data)
        project_attribute_records.extend(project_attributes_data)

    project_attribute_key_responses = _bulk_sync(requester, project_attribute_key_records,
            constants.PROJECT_ATTRIBUTE_KEY_SYNC_URL, 2000)

    project_responses = _bulk_sync(requester, project_records,
            constants.PROJECT_SYNC_URL, 1000)

    project_attribute_responses = _bulk_sync(requester, project_attribute_records,
            constants.PROJECT_ATTRIBUTE_SYNC_URL, 2000)

    return {
        "projects": project_responses,
        "project_attribute_keys": project_attribute_key_responses,
        "project_attributes": project_attribute_responses,
    }

def upload_consumption_dataframe(consumption_df, datastore):
    """Uploads consumption data in pandas DataFrame format to a datastore instance.

    Parameters
    ----------
    consumption_df : pandas.DataFrame
        DataFrame with the following columns::

            project_id,start,end,fuel_type,unit_name,value,estimated

    url : str
        Base URL of the target datastore.
    access_token : str
        Access token for the target datastore.
    """
    requester = Requester(datastore['url'], datastore['access_token'])

    consumption_metadata_records = []
    consumption_record_records = []
    for consumption_metadata_data, consumption_records_data in \
            _get_consumption_data(consumption_df):
        consumption_metadata_records.append(consumption_metadata_data)
        consumption_record_records.extend(consumption_records_data)

    consumption_metadata_responses = _bulk_sync(requester, consumption_metadata_records,
            constants.CONSUMPTION_METADATA_SYNC_URL, 2000)

    consumption_record_responses = _bulk_sync(requester, consumption_record_records,
            constants.CONSUMPTION_RECORD_SYNC_URL, 3000)

    return {
        "consumption_metadatas": consumption_metadata_responses,
        "consumption_records": consumption_record_responses,
    }

def upload_consumption_dataframe_faster(consumption_df, datastore):
    """Uploads consumption data in pandas DataFrame format to a datastore instance using `bulk_sync`.

    Parameters
    ----------
    consumption_df : pandas.DataFrame
        DataFrame with the following columns::

            project_id,start,end,fuel_type,unit_name,value,estimated

    url : str
        Base URL of the target datastore.
    access_token : str
        Access token for the target datastore.
    """
    requester = Requester(datastore['url'], datastore['access_token'])

    consumption_metadata_records = []
    consumption_record_records = []
    for consumption_metadata_data, consumption_records_data in \
            _get_consumption_data(consumption_df):
        consumption_metadata_records.append(consumption_metadata_data)
        consumption_record_records.extend(consumption_records_data)

    consumption_metadata_responses = _bulk_sync(
            requester,
            consumption_metadata_records,
            constants.CONSUMPTION_METADATA_SYNC_URL,
            2000)

    consumption_record_responses = _bulk_sync_faster(
            requester,
            consumption_record_records,
            consumption_metadata_responses,
            constants.CONSUMPTION_RECORD_SYNC_FASTER_URL,
            3000)

    return {
        "consumption_metadatas": consumption_metadata_responses,
        "consumption_records": consumption_record_responses,
    }

def _bulk_sync(requester, records, url, n):
    """ Syncs records using a sync endpoint on the datastore by batching in
    groups of n and collecting responses.
    """
    responses = []
    n_records = len(records)
    for i in range(0, n_records, n):
        batch = records[i:i+n]
        response = requester.post(url, batch)

        if response.status_code != 200:
            if "no Project found" in response.json()[0].get("status", ""):
                logging.warning("Tried to upload consumption data for a Project that doesn't exist. Skipping it.")
                continue
            else:
                raise Exception("Unknown error when attempting to sync ConsumptionMetedata")

        json_response = response.json()

        if len(json_response) > 0:
            try:
                sample_response = json_response[0]
            except:
                print(json_response)
                raise
            else:
                n_ = min(n_records, i+n)
                print("Synced {} of {}".format(n_, n_records))
                print("    Sample response: {}".format(sample_response))

        responses.extend(json_response)
    return responses

def _bulk_sync_faster(requester, records, metadatas, url, n):
    """Syncs records using the bulk_sync endpoint on the datastore

    Ignore batching for now.
    """

    # Lookup table for matching metadata values to the corresponding key.
    # This will break if additional fields are added to ConsumptionMetadata
    #
    # Also turns out that `energy_unit` and `fuel_type` are just placeholders
    # (See comment in `_process_raw_consumption_records_data`). So, we just
    # ignore the `energy_unit`
    metadatas_dict = {}
    for m in metadatas:
        fuel_type = m['fuel_type']
        project_id = m['project']['project_id']
        key = (str(fuel_type), str(project_id))
        metadatas_dict[key] = m['id']

    # Replace consumption metadata in each record with the id of the
    # corresponding DB object
    def metadata_key(record):
        return (str(record['fuel_type']), str(record['project_id']))
    def has_matching_metadata(record):
        matched = metadata_key(record) in metadatas_dict
        return matched
    def trim_record(record):
        record['metadata_id'] = metadatas_dict[metadata_key(record)]
        del record['fuel_type']
        del record['project_id']
        return record
    len_records_before = len(records)
    records = map(trim_record, filter(has_matching_metadata, records))
    if len_records_before != len(records):
        logging.warning("At least one ConsumptionMetadata id was not matched to a ConsumptionRecord. Skipping it.")

    response = requester.post(url, records)

    assert response.status_code == 200

    json_response = response.json()

    return json_response


def _get_project_attribute_keys_data(project_df):
    """ Gets project attribute keys by looking at extra columns in dataframe.
    """

    project_attribute_keys_data = []
    for column_name in project_df.columns:

        if column_name in constants.STANDARD_PROJECT_DATA_COLUMN_NAMES:
            continue

        if column_name in constants.STANDARD_PROJECT_ATTRIBUTE_KEYS:
            project_attribute_key = constants.STANDARD_PROJECT_ATTRIBUTE_KEYS[column_name]
            project_attribute_key_data = {
                "name": project_attribute_key["name"],
                "display_name": project_attribute_key["display_name"],
                "data_type": project_attribute_key["data_type"],
            }
        else:
            project_attribute_key_data = _infer_project_attribute_key_data(
                    column_name, project_df[column_name])

        project_attribute_keys_data.append(project_attribute_key_data)

    return project_attribute_keys_data

def _infer_project_attribute_key_data(column_name, column):
    """ Infers project attribute data including display name and datatype from
    dataframe column and column name.
    """
    project_attribute_key_data = {
        "name": column_name,
        "display_name": _infer_display_name(column_name),
        "data_type": _infer_data_type(column),
    }
    return project_attribute_key_data

def _infer_data_type(column):
    """ Infers datatype from pandas column type and format of first few rows.
    """
    if column.shape[0] == 0:
        return None

    if column.dtype == "float64":
        return "FLOAT"
    elif column.dtype == "int64":
        return "INTEGER"
    elif column.dtype == "bool":
        return "BOOLEAN"
    elif column.dtype == "object":
        try:
            pd.to_datetime(column[:10])
        except ValueError:
            return "CHAR"
        try:
            datetime.strptime(column[0], "%Y-%m-%d")
            return "DATE"
        except ValueError:
            return "DATETIME"
    else:
        return None

def _infer_display_name(column_name):
    """ Gets nicely capitalized display name from name.
    """
    # first standardize to underscored_column_name
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', column_name)
    underscored = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    # then convert to Not Underscored Column Name
    display_name = " ".join([ str.capitalize(w) for w in underscored.split("_")])
    return display_name

def _get_project_data(project_df, project_attribute_keys_data):
    """ Yields a project record and project attribute records grouped by project
    """
    for i, row in project_df.iterrows():
        project_data = {
            "project_id": row.project_id,
            "zipcode": str(row.zipcode) if pd.notnull(row.zipcode) else None,
            "weather_station": str(row.weather_station) if pd.notnull(row.weather_station) else None,
            "latitude": row.latitude if pd.notnull(row.latitude) else None,
            "longitude": row.longitude if pd.notnull(row.longitude) else None,
            "baseline_period_start": None,
            "reporting_period_end": None,
        }

        assert pd.notnull(project_data["project_id"])

        baseline_period_end_localized = pytz.UTC.localize(row.baseline_period_end)
        if pd.isnull(baseline_period_end_localized):
            project_data["baseline_period_end"] = None
        else:
            project_data["baseline_period_end"] = baseline_period_end_localized.strftime("%Y-%m-%dT%H:%M:%S%z")

        reporting_period_start_localized = pytz.UTC.localize(row.reporting_period_start)
        if pd.isnull(reporting_period_start_localized):
            project_data["reporting_period_start"] = None
        else:
            project_data["reporting_period_start"] = reporting_period_start_localized.strftime("%Y-%m-%dT%H:%M:%S%z")

        project_attributes_data = []
        for project_attribute_key_data in project_attribute_keys_data:
            project_attribute_data = _get_project_attribute_data(row, project_attribute_key_data)
            project_attributes_data.append(project_attribute_data)

        yield project_data, project_attributes_data

def _get_project_attribute_data(row, project_attribute_key_data):
    """ Gets a single formatted project attribute record
    """

    name = project_attribute_key_data["name"]
    value = row[name]

    project_attribute_data = {
        "project_project_id": row["project_id"],
        "project_attribute_key_name": name,
        "value": value if pd.notnull(value) else None,
    }

    return project_attribute_data

def _get_consumption_data(consumption_df):
    """ Yields consumption records and metadata grouped by project and fuel type
    """
    for project_id, project_consumption in consumption_df.groupby("project_id"):
        for fuel_type, fuel_type_consumption in project_consumption.groupby("fuel_type"):
            unique_unit_names = fuel_type_consumption.unit_name.unique()
            assert unique_unit_names.shape[0] == 1

            consumption_metadata_data = {
                "project_project_id": project_id,
                "fuel_type": constants.FUEL_TYPES[fuel_type],
                "energy_unit": constants.ENERGY_UNIT[unique_unit_names[0]],
            }
            consumption_records_data = _get_consumption_records_data(
                    fuel_type_consumption)

            yield consumption_metadata_data, consumption_records_data

def _get_consumption_records_data(consumption_df):
    """ Get consumption records from single project_id/fuel_type group. """
    raw_consumption_records_data = _get_raw_consumption_records_data(
            consumption_df)
    consumption_records_data = _process_raw_consumption_records_data(
            raw_consumption_records_data)
    return consumption_records_data

def _get_raw_consumption_records_data(consumption_df):
    """ Get raw consumption records from single project_id/fuel_type group. """
    raw_consumption_records_data = []
    for i, row in consumption_df.iterrows():
        consumption_record_data = {
            "project_id": row.project_id,
            "fuel_type": constants.FUEL_TYPES[row.fuel_type],
            "start": row.start,
            "end": row.end,
            "value": row.value,
            "estimated": row.estimated,
        }
        raw_consumption_records_data.append(consumption_record_data)
    return raw_consumption_records_data

def _process_raw_consumption_records_data(records):
    """ Turn records into "start" oriented records, make UTC. """

    # assume all from the same project and fuel_type
    if len(records) > 0:
        project_id = records[0]["project_id"]
        fuel_type = records[0]["fuel_type"]

    # dumb hack - the fuel_type and unit_name are actually just placeholders
    # and don't actually affect the processing. This an indication that (TODO),
    # this logic should be factored out of the ConsumptionData object.
    consumption_data = ConsumptionData(records, "electricity", "kWh",
                                       record_type="arbitrary")

    consumption_records_data = []
    for (d1, value), (d2, estimated) in zip(consumption_data.data.iteritems(), consumption_data.estimated.iteritems()):
        assert d1 == d2
        record = {
            "start": pytz.UTC.localize(d1.to_datetime()).strftime("%Y-%m-%dT%H:%M:%S%z"),
            "project_id": project_id,
            "fuel_type": fuel_type,
            "value": value if pd.notnull(value) else None,
            "estimated": bool(estimated)
        }
        consumption_records_data.append(record)
    return consumption_records_data
