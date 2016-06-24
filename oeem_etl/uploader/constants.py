PROJECT_ATTRIBUTE_KEY_URL = 'project_attribute_keys/'
PROJECT_ATTRIBUTE_KEY_SYNC_URL = 'project_attribute_keys/sync/'
PROJECT_URL = 'projects/'
PROJECT_SYNC_URL = 'projects/sync/'
PROJECT_ATTRIBUTE_URL = 'project_attributes/'
PROJECT_ATTRIBUTE_SYNC_URL = 'project_attributes/sync/'
CONSUMPTION_METADATA_URL = 'consumption_metadatas/'
CONSUMPTION_METADATA_SYNC_URL = 'consumption_metadatas/sync/'
CONSUMPTION_RECORD_URL = 'consumption_records/'
CONSUMPTION_RECORD_SYNC_URL = 'consumption_records/sync/'
CONSUMPTION_RECORD_SYNC_FASTER_URL = 'consumption_records/sync2/'

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

FUEL_TYPES = {
    "electricity": "E",
    "natural_gas": "NG",
}

ENERGY_UNIT = {
    "kWh": "KWH",
    "therms": "THM",
    "therm": "THM",
}
