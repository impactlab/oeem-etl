import pytest
import arrow
import xml.etree.cElementTree as etree
from oeem_etl.fetchers import ESPICustomer
from oeem_etl.storage import StorageClient
from oeem_etl.tasks.shared import mangle_path

# NOTE: ESPICustomer methods current untested include: _check_dates,
# _fetch_usage_points, _fetch_usage_data, _should_run_now,
# _get_max_date_if_run_now, _fetch_last_run_max_date, get_min_date,
# usage_points, and fetch_usage.
# These methods all involve getting data from an ESPI API, from
# the storage service, or are based on the current time.

@pytest.fixture
def customer_instance():
    customer_cred = {'project_id': 'None',
                     'subscription_id': 'None',
                     'access_token': 'None'}
    config = {'existing_paths': 'TK',
              'base_directory': 'TK',
              'espi_api_url': 'TK',
              'cert_file': 'TK',
              'privkey_file': 'TK',
              'file_storage': 'local'}
    storage = StorageClient(config)
    target_class = storage.get_target_class()
    config['target'] = target_class
    return ESPICustomer(customer_cred, config)


def test_make_headers(customer_instance):
    output = {"Authorization": "Bearer 134jnmn123cgd1243"}
    assert customer_instance._make_headers('134jnmn123cgd1243') == output


def test_path_to_run_num(customer_instance):
    path = '/path/to/my/file/is/winding/omg_heres_the_file_14.biz'
    assert customer_instance._path_to_run_num(path) == 14


def test_parse_usage_points(customer_instance):
    xml = '<ns1:feed xmlns:ns0="http://naesb.org/espi" xmlns:ns1="http://www.w3.org/2005/Atom"><ns1:entry xmlns:ns1="http://www.w3.org/2005/Atom"> <ns1:id xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ns1:idType">e9267d9e-5b92-47bd-87ed-29fb5b86abdf</ns1:id> <ns1:link xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" href="https://api.pge.com/GreenButtonConnect/espi/1_1/resource/Subscription/94666/UsagePoint" rel="up" xsi:type="ns1:linkType"/> <ns1:link xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" href="https://api.pge.com/GreenButtonConnect/espi/1_1/resource/Subscription/94666/UsagePoint/9026920544" rel="self" xsi:type="ns1:linkType"/> <ns1:link xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" href="https://api.pge.com/GreenButtonConnect/espi/1_1/resource/Subscription/94666/UsagePoint/9026920544/MeterReading" rel="related" xsi:type="ns1:linkType"/> <ns1:link xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" href="https://api.pge.com/GreenButtonConnect/espi/1_1/resource/Subscription/94666/UsagePoint/9026920544/UsageSummary" rel="related" xsi:type="ns1:linkType"/> <ns1:link xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" href="https://api.pge.com/GreenButtonConnect/espi/1_1/resource/LocalTimeParameters/1" rel="related" xsi:type="ns1:linkType"/> <ns1:title xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" type="text" xsi:type="ns1:textType">Green Button Data File</ns1:title> <ns1:published xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ns1:dateTimeType">2016-04-15T21:06:49.843Z</ns1:published> <ns1:updated xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ns1:dateTimeType">2016-04-15T21:06:49.845Z</ns1:updated> <ns1:content xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ns1:contentType"> <ns0:UsagePoint xmlns:ns0="http://naesb.org/espi"> <ns0:ServiceCategory> <ns0:kind>1</ns0:kind> </ns0:ServiceCategory> </ns0:UsagePoint> </ns1:content> </ns1:entry> </ns1:feed>'
    output = [('9026920544', '1')]
    assert list(customer_instance._parse_usage_points(xml)) == output


def test_parse_usage_data(customer_instance):
    customer_instance.usage_point_cat = '0'
    usage_xml = '<ns0:IntervalBlock xmlns:ns0="http://naesb.org/espi"><ns0:IntervalReading> <ns0:ReadingQuality> <ns0:quality>19</ns0:quality> </ns0:ReadingQuality> <ns0:timePeriod> <ns0:duration>3600</ns0:duration> <ns0:start>1426989600</ns0:start> </ns0:timePeriod> <ns0:value>906600</ns0:value> </ns0:IntervalReading><ns0:IntervalReading> <ns0:ReadingQuality> <ns0:quality>19</ns0:quality> </ns0:ReadingQuality> <ns0:timePeriod> <ns0:duration>3600</ns0:duration> <ns0:start>1426993200</ns0:start> </ns0:timePeriod> <ns0:value>905300</ns0:value> </ns0:IntervalReading></ns0:IntervalBlock>'
    output = [{'project_id': False,
              'start': 1426989600,
              'end': 1426993200,
              'fuel_type': 'electricity',
              'unit_name': 'kWh',
               'value': '906600'},
              {'project_id': False,
              'start': 1426993200,
              'end': 1426996800,
              'fuel_type': 'electricity',
              'unit_name': 'kWh',
              'value': '905300'}]
    assert customer_instance._parse_usage_data(usage_xml) == output


def test_parse_reading(customer_instance):
    customer_instance.usage_point_cat = '0'
    reading_string = '<ns0:IntervalBlock xmlns:ns0="http://naesb.org/espi"><ns0:IntervalReading> <ns0:ReadingQuality> <ns0:quality>19</ns0:quality> </ns0:ReadingQuality> <ns0:timePeriod> <ns0:duration>3600</ns0:duration> <ns0:start>1426989600</ns0:start> </ns0:timePeriod> <ns0:value>906600</ns0:value> </ns0:IntervalReading></ns0:IntervalBlock>'
    root = etree.XML(reading_string)
    reading_tags = './/{http://naesb.org/espi}IntervalReading'
    reading = root.findall(reading_tags)[0]
    output = {'project_id': False,
              'start': 1426989600,
              'end': 1426993200,
              'fuel_type': 'electricity',
              'unit_name': 'kWh',
              'value': '906600'}
    assert customer_instance._parse_reading(reading) == output

def test_find_reading_date_range(customer_instance):
    readings =  [{'project_id': False,
                  'start': 1426993200,
                  'end': 1426996800,
                  'fuel_type': 'electricity',
                  'unit_name': 'kWh',
                  'value': '905300'},
                 {'project_id': False,
                  'start': 1426989600,
                  'end': 1426993200,
                  'fuel_type': 'electricity',
                  'unit_name': 'kWh',
                  'value': '906600'}]
    output = (arrow.get(1426989600), arrow.get(1426996800))
    assert customer_instance._find_reading_date_range(readings) == output


def test_get_max_date(customer_instance):
    customer_instance.usage_point_cat = '0'
    output = arrow.get(2014,1,8,6,59,59,0)
    assert customer_instance._get_max_date(current_datetime=arrow.get(2014,1,10), end_day_hour=7) == output

    customer_instance.usage_point_cat = '1'
    output = arrow.get(2014,1,8,7,0,0,0)
    assert customer_instance._get_max_date(current_datetime=arrow.get(2014,1,10), end_day_hour=7) == output


def test_mangle_path():
    with pytest.raises(ValueError):
        assert mangle_path("/", "raw", "parsed") is None

    assert mangle_path("raw/", "raw", "parsed") == "parsed/"
    assert mangle_path("raw/raw", "raw", "parsed") == "parsed/raw"
    assert mangle_path("parsed/raw", "raw", "parsed") == "parsed/parsed"
    assert mangle_path("/raw", "raw", "parsed") == "/parsed"
    assert mangle_path("/stuff/rawww/raw/morestuff/", "raw", "parsed") == "/stuff/rawww/parsed/morestuff/"
    assert mangle_path("raw/morestuff.xml", "raw", "parsed") == "parsed/morestuff.csv"
    assert mangle_path("raw/blah/morestuff.xml", "raw", "parsed") == "parsed/blah/morestuff.csv"
