PROJECT_ATTRIBUTE_KEY_URL = 'project_attribute_keys/'
PROJECT_ATTRIBUTE_KEY_SYNC_URL = 'project_attribute_keys/sync/'
PROJECT_URL = 'projects/'
PROJECT_SYNC_URL = 'projects/sync/'
PROJECT_ATTRIBUTE_URL = 'project_attributes/'
PROJECT_ATTRIBUTE_SYNC_URL = 'project_attributes/sync/'
CONSUMPTION_METADATA_URL = 'consumption_metadatas/'
CONSUMPTION_METADATA_SYNC_URL = 'consumption_metadatas/many/'
CONSUMPTION_RECORD_URL = 'consumption_records/'
CONSUMPTION_RECORD_SYNC_URL = 'consumption_records/sync/'
CONSUMPTION_RECORD_SYNC_FASTER_URL = 'consumption_records/bulk_insert/'

STANDARD_PROJECT_DATA_COLUMN_NAMES = [
    "project_id",
    "zipcode",
    "weather_station",
    "latitude",
    "longitude",
    "baseline_period_start", # handle this specially? it won't appear in most project dataframes
    "baseline_period_end",
    "reporting_period_start",
    "reporting_period_end", # handle this specially? it won't appear in most project dataframes
]

STANDARD_PROJECT_ATTRIBUTE_KEYS = {
    "predicted_electricity_savings": {
        "name": "predicted_electricity_savings",
        "display_name": "Estimated Electricity Savings",
        "data_type": "FLOAT",
    },
    "predicted_natural_gas_savings": {
        "name": "predicted_natural_gas_savings",
        "display_name": "Estimated Natural Gas Savings",
        "data_type": "FLOAT",
    },
    "project_cost": {
        "name": "project_cost",
        "display_name": "Project Cost",
        "data_type": "FLOAT",
    },
}

# Maps human-readable values to value the datastore expects
INTERPRETATION_CHOICES = [
    ('E_C_S', 'ELECTRICITY_CONSUMPTION_SUPPLIED'),
    ('E_C_T', 'ELECTRICITY_CONSUMPTION_TOTAL'),
    ('E_C_N', 'ELECTRICITY_CONSUMPTION_NET'),
    ('E_OSG_T', 'ELECTRICITY_ON_SITE_GENERATION_TOTAL'),
    ('E_OSG_C', 'ELECTRICITY_ON_SITE_GENERATION_CONSUMED'),
    ('E_OSG_U', 'ELECTRICITY_ON_SITE_GENERATION_UNCONSUMED'),
    ('NG_C_S', 'NATURAL_GAS_CONSUMPTION_SUPPLIED'),
]
INTERPRETATION_CHOICES = {value:key for key, value in INTERPRETATION_CHOICES}

# Maps human-readable values to value the datastore expects
UNIT_CHOICES = [
    ('KWH', 'kWh'),
    ('THM', 'therm'),
    ('THM', 'THERM'),
]
UNIT_CHOICES = {value:key for key, value in UNIT_CHOICES}

