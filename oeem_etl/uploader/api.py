from oeem_etl.uploader import constants
from oeem_etl.uploader.requester import Requester
from datetime import date, datetime
import dateutil.parser
import pandas as pd
import numpy as np
import pytz
import re
import json
import logging

try:
    # 0.3
    from eemeter.consumption import ConsumptionData
except:
    # 0.4
    pass


__all__ = [
    'upload_project_dataframe',
    'upload_consumption_dataframe',
    'upload_consumption_dataframe_faster',
]

def upload_project_dataframe(project_df, datastore):
    """Uploads project data in pandas DataFrame format to a datastore instance.

    Parameters
    ----------
    project_df : pandas.DataFrame
        DataFrame with the following columns::

            project_id,
            zipcode,
            baseline_period_end,
            reporting_period_start

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
            constants.PROJECT_ATTRIBUTE_KEY_SYNC_URL, 200)

    project_responses = _bulk_sync(requester, project_records,
            constants.PROJECT_SYNC_URL, 200)

    project_attribute_responses = _bulk_sync(requester, project_attribute_records,
            constants.PROJECT_ATTRIBUTE_SYNC_URL, 200)

    return {
        "projects": project_responses,
        "project_attribute_keys": project_attribute_key_responses,
        "project_attributes": project_attribute_responses,
    }

def upload_consumption_dataframe(consumption_df, datastore):
    raise DeprecationWarning("Please use upload_consumption_dataframe_faster")

def upload_consumption_dataframe_faster(consumption_df, datastore):
    """Uploads consumption data in pandas DataFrame format to a datastore instance using `bulk_sync_faster`.

    Parameters
    ----------
    consumption_df : pandas.DataFrame
        DataFrame with the following columns::

            project_id,
            start,
            unit,
            label,
            interpretation,
            value,
            estimated
    
    datastore: dict
        Dict with the following properties
        url : str
            Base URL of the target datastore.
        access_token : str
            Access token for the target datastore.
    """
    requester = Requester(datastore['url'], datastore['access_token'])

    # Marshall consumption dataframe to list of dicts
    consumption_record_records = []

    def map_choice(choice_mapping, value):
        if value in choice_mapping.values():
            return value
        assert value in choice_mapping
        return choice_mapping.get(value)


    for _, row in consumption_df.iterrows():
        consumption_record_records.append({
            "project_id": row.project_id,
            "start": pytz.UTC.localize(row.start.to_datetime()).strftime("%Y-%m-%dT%H:%M:%S%z"),
            "interpretation": map_choice(constants.INTERPRETATION_CHOICES, row.interpretation),
            "value": row.value,
            "estimated": row.estimated,
            "label": row.label,
            "unit": map_choice(constants.UNIT_CHOICES, row.unit)
        })


    # The datastore expects certain fields to be split out of consumption records
    # and synced as ConsumptionMetadata.

    # Extract unique metadata records
    consumption_metadata_records = {}
    for row in consumption_record_records:
        record = {
            "project_project_id": row['project_id'],
            "unit": row['unit'],
            "interpretation": row['interpretation'],
            "label": row['label'],
        }
        consumption_metadata_records[tuple(record.values())] = record
    consumption_metadata_records = consumption_metadata_records.values()

    # Sync ConsumptionMetadata
    consumption_metadata_responses = _bulk_sync(
            requester,
            consumption_metadata_records,
            constants.CONSUMPTION_METADATA_SYNC_URL,
            2000)

    # Update consumption records to point to id of ConsumptionMetadata records just created
    metadatas = consumption_metadata_responses
    metadatas_dict = {}
    metadata_key_fields = ['interpretation', 'unit', 'project_id', 'label']
    def metadata_key(record):
        return tuple([str(record[key]) for key in metadata_key_fields])

    # Build mapping from fields to metadata id
    for m in metadatas:
        m['project_id'] = m['project']['project_id']
    metadatas_dict = {
        metadata_key(m): m['id'] for m in metadatas
    }
    def has_matching_metadata(record):
        return metadata_key(record) in metadatas_dict
    def trim_record(record):
        record['metadata_id'] = metadatas_dict[metadata_key(record)]
        for field in metadata_key_fields:
            del record[field]
        return record

    # Try to match consumption records
    records = consumption_record_records
    len_records_before = len(records)
    records = map(trim_record, filter(has_matching_metadata, records))
    if len_records_before != len(records):
        logging.warning("At least one ConsumptionMetadata id was not matched to a ConsumptionRecord. Skipping it.")

    # Upload the trimmed ConsumptionRecords
    consumption_record_responses = _bulk_sync_faster(
            requester,
            records,
            constants.CONSUMPTION_RECORD_SYNC_FASTER_URL)

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

def _bulk_sync_faster(requester, records, url):
    """Syncs records using the bulk_sync endpoint on the datastore"""
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
