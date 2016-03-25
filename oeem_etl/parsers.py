#!/usr/bin/env python
# -*- coding: utf-8 -*-

from eemeter import location
from xml.etree import cElementTree

# TODO: FIGURE OUT WHICH IS WHICH.
SERVICE_CAT_TO_FUEL_TYPE = {'0': ('electricity', 'kWh'),
                            '1': ('natural_gas', 'therms')}

# TODO: catch if you didn't get an XML response, or fields are missing.
def parse_usage_data(usage_xml, service_cat):
    '''
    Parse EPSI usage XML data into format suitable for OEEM uploader.
    '''
    root = cElementTree.XML(usage_xml)  # Parse XML element tree.
    # IntervalReading elements house all energy data.
    reading_tag = './/{http://naesb.org/espi}IntervalReading'
    interval_readings = root.findall(reading_tag)
    return [parse_reading(reading, service_cat) for reading
            in interval_readings]

# TODO: catch if you didn't get an XML response, or fields are missing.
# TODO: figure out which customer name / address to get, since there can be one for each usagepoint.
# TODO: deal with customers that have solar data?
def parse_account_data(account_xml):
    '''Parse customer name and address for ESPI API XML response.'''
    print(account_xml)
    root = cElementTree.XML(account_xml)  # Parse XML element tree.
    # XML may have multiple customer names and addresses. Parse the first ones.
    customer_name = root.findall('.//{http://naesb.org/espi/customer}Customer')[0].find('{http://naesb.org/espi/customer}name').text.strip().lower()
    main_address = root.findall('.//{http://naesb.org/espi/customer}mainAddress')[0]
    address = main_address.find('.//{http://naesb.org/espi/customer}addressGeneral').text.strip().lower()
    zipcode = main_address.find('.//{http://naesb.org/espi/customer}code').text.strip().lower()
    city = main_address.find('.//{http://naesb.org/espi/customer}name').text.strip().lower()
    state = main_address.find('.//{http://naesb.org/espi/customer}stateOrProvince').text.strip().lower()
    return {'customer_name': customer_name,
            'address': address,
            'zipcode': zipcode,
            'city': city,
            'state': state}


# TODO: what happens if fields are missing?
# TODO: correct the energy values into kWh and therms.
# TODO: Pay particular attention to Power of ten multiplier and Unit of Measure (uom) so as to ensure correct usage reading
# TODO: check that commodity, flow direction, interval length, power of ten, and uom codes are correct.
# Thoughts: will ever xml file contain at least one intervalblock? does it always represent 24 hours during a particular day? When does the day start? Is it always the same?
# If so, then you can write the code to find the days in each file.
def parse_reading(reading, service_cat):
    '''
    Parse EPSI IntervalReading XML element into OEEM uploader-compliant
    dictionary.
    '''
    start = int(reading.find('.//{http://naesb.org/espi}start').text)
    duration = int(reading.find('.//{http://naesb.org/espi}duration').text)
    end = start + duration
    fuel_type, unit_name = SERVICE_CAT_TO_FUEL_TYPE[service_cat]
    value = reading.find('{http://naesb.org/espi}value').text
    return {'project_id': False,
            'start': start,
            'end': end,
            'fuel_type': fuel_type,
            'unit_name': unit_name,
            'value': value}

# TODO: catch if you didn't get an XML response, or fields are missing.
# TODO: Check if missing energy estimates should really be missing, or if it's just not finding them.
# TODO: "fix address problem"
def hpxml_to_project(project_id, hpxml):
    root = cElementTree.fromstring(hpxml)
    ns = 'http://hpxmlonline.com/2014/6'  # XML namespace
    first_name = root.find('.//{%s}FirstName' % ns).text.lower()
    last_name = root.find('.//{%s}LastName' % ns).text.lower()

    building = root.find('{%s}Building' % ns)
    address = building.find('.//{%s}Address1' % ns).text.lower()
    city = building.find('.//{%s}CityMunicipality' % ns).text.lower()
    state = building.find('.//{%s}StateCode' % ns).text.lower()
    zipcode = building.find('.//{%s}ZipCode' % ns).text.lower()

    project_tag = root.find('.//{%s}Project' % ns)
    project_start_date = project_tag.find('.//{%s}StartDate' % ns).text
    project_end_date = project_tag.find('.//{%s}CompleteDateActual' % ns).text

    # TODO: Check that energy savings are actually estimated?
    fuel_savings = root.findall('.//{%s}Project/{%s}ProjectDetails/{%s}EnergySavingsInfo/{%s}FuelSavings' % (ns, ns, ns, ns))
    # TODO: comment this
    for savings in fuel_savings:
        fuel_type = savings.find('{%s}Fuel' % ns).text
        predicted_natural_gas_savings = False
        predicted_electricity_savings = False
        energy_savings = savings.find('{%s}TotalSavings' % ns).text
        if fuel_type == 'natural gas':
            predicted_natural_gas_savings = energy_savings
        elif fuel_type == 'electricity':
            predicted_electricity_savings = energy_savings

    # Use eemeter package to get lat/lon and weather station.
    lat, lon = location.zipcode_to_lat_lng(zipcode)
    station = location.zipcode_to_station(zipcode)
    out = {}
    out['customer'] = {'customer_name': first_name + ' ' + last_name,
                       'address': address,
                       'zipcode': zipcode,
                       'city': city,
                       'state': state}

    out['project'] = {'project_id': project_id,
                      'baseline_period_start': False,  # Get later
                      'baseline_period_end': project_start_date,
                      'reporting_period_start': project_end_date,
                      'reporting_period_end': False,  # Get later.
                      'latitude': lat,
                      'longitude': lon,
                      'zipcode': zipcode,
                      'weather_station': station,
                      'predicted_electricity_savings': predicted_electricity_savings,
                      'predicted_natural_gas_savings': predicted_natural_gas_savings}

    return out


def client_project_keys(s3_bucket, client_path):
    '''
    Return key objects in s3 bucket that are stored
    under given path, associated with a particular client.
    '''
    for k in s3_bucket:  # boto bucket object.
        if client_path in str(k.key):
            yield k

project_parser = {'hpxml': hpxml_to_project}
