import os
import arrow
import urllib
import requests
import logging
import py
from dateutil import parser
import xml
import xml.etree.cElementTree as etree

from .parsers import parse_usage_data


class GreenButtonAPI():
    '''
    Get ESPI access tokens for utility customers who have authorized our
    acess through GreenButton.
    '''

    def __init__(self, api_url, oauth_token):
        self.api_url = api_url
        self.headers = {"Authorization": "Bearer {}".format(oauth_token)}

    def fetch_client_customer_ids(self, client_id):
        '''
        Get customer ids for customers of given client that
        have authorized us to get their energy data.
        '''
        params = urllib.parse.urlencode({'oeeclient': client_id,
                                         'has_active_refresh_token': True,
                                         'id_only': True})
        customers = requests.get(self.api_url, params=params,
                                 headers=self.headers).json()
        return [str(customer['id']) for customer in customers]

    def client_customers(self, client_id):
        '''
        Return Green Button access token, subscriber id, join date,
        and other info for all customers of given client.
        '''
        for customer_id in self.fetch_client_customer_ids(client_id):
            customer_url = urllib.parse.urljoin(self.api_url, customer_id)
            customer = requests.get(customer_url, headers=self.headers).json()
            # Prevent ESPI API calls for customers with no access tokens.
            if customer['active_access_token'] is not None:
                out = []
                # Customer's subcription id lives in resource uri.
                out['subscription_id'] = customer['resource_uri'].split('/')[-1]
                out['project_id'] = customer['project_id']
                out['access_token'] = customer['active_access_token']
                yield out


class ESPIAPI():
    '''
    Get customer account and energy usage data from any ESPI-compliant
    utility API. Requires API url and ssl certificates, and customer-specific
    subscription ids and oauth access tokens.
    '''

    def __init__(self, api_url, cert):
        self.api_url = api_url
        self.cert = cert


    def make_headers(self, access_token):
        return {"Authorization": "Bearer {}".format(access_token)}


    def fetch_usage_points(self, customer):
        '''
        Get the id and ServiceCategory kind of customer's usage points
        from ESPI API.
        '''
        print(customer)
        endpoint = 'Subscription/{}/UsagePoint'.format(customer['subscription_id'])
        subscription_url = self.api_url + endpoint
        usage_point_xml = requests.get(subscription_url,
                                       headers=self.make_headers(customer['access_token']),
                                       cert=self.cert).text
        try:
            for usage_point_id, usage_point_cat \
            in self.parse_usage_points(usage_point_xml):
                yield {'id': usage_point_id, 'cat': usage_point_cat}

        except xml.etree.ElementTree.ParseError:
            print("Couldn't parse xml!")
            print(usage_point_xml)


    def parse_usage_points(self, usage_point_xml):
        '''
        Extract UsagePoint id and ServiceCategory kind from
        ESPI API XML response.
        '''
        # Parse XML element tree.
        root = etree.XML(usage_point_xml)
        # UsagePoint data is stored under entry elements in the Atom feed.
        for entry_tag in root.findall('{http://www.w3.org/2005/Atom}entry'):
            # UsagePoint id only provided in entry link elements.
            # Always grab it from the href attribute of the second link.
            usage_point_id = entry_tag.findall('{http://www.w3.org/2005/Atom}link')[1].attrib['href'].split('/')[-1]
            # Grab UsagePoint kind from the text inside entry.content.UsagePoint.ServiceCategory.kind
            usage_point_kind = entry_tag.find('.//{http://naesb.org/espi}kind').text
            yield usage_point_id, usage_point_kind

    def fetch_usage_data(self, subscription_id, usage_point_id, access_token, min_date, max_date):
        '''
        Get customer usage point's usage data from ESPI API.
        '''
        params = urllib.parse.urlencode({'published-min': min_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                         'published-max': max_date.strftime('%Y-%m-%dT%H:%M:%SZ')})
        endpoint = 'Batch/Subscription/{}/UsagePoint/{}'.format(subscription_id,
                                                                usage_point_id)
        usage_url = self.api_url + endpoint + '?' + params
        return requests.get(usage_url,
                            headers=self.make_headers(access_token),
                            cert=self.cert).text

    def fetch_customer_usage(self, subscription_id, access_token, date_range):
        '''
        Get all usage data for customer, across all usage points, from
        ESPI API. Save files to disk.
        # TODO: SINGLE OUTPUT FILE FOR ALL USAGE POINTS?
        '''
        for usage_point in self.fetch_usage_points(subscription_id, access_token):
            yield self.fetch_usage_data(subscription_id, usage_point['id'],
                                         access_token, date_range)

    def fetch_customer_account(self, subscription_id, access_token):
        '''Get customer name and address for ESPI API.'''
        endpoint = 'Batch/RetailCustomer/'
        account_url = self.api_url + endpoint + subscription_id
        return requests.get(account_url, headers=self.make_headers(access_token),
                            cert=self.cert).text


def find_reading_date_range(readings):
    '''
    Find the date range represents by a series of readings:
    the earliest min-date to the latest max-date.
    '''
    min_start = readings[0]["start"]
    max_end = readings[0]["end"]
    for reading in readings:
        if reading['start'] < min_start:
            min_start = reading['start']
        if reading['end'] > max_end:
            max_end = reading['end']
    return arrow.get(min_start), arrow.get(max_end)


def check_dates(request_date, response_date, date_type, subscription_id, up_cat):
    '''Check whether the date your asked for is the date you got.'''
    try:
        # Data params in the API are inclusive. If your day starts at 0700,
        # and you set max-date to 0700, you'll get back the day.
        # To get the previous day, set it to 06:59:59. The max end date
        # returned will be 0700, however, so you must add 1 second to
        # the request time to see if you get the date you asked for.
        if date_type == 'max':
            request_date = request_date.replace(seconds=+1)
        # Also, usage point cat 1 starts one second after the hour.
        assert request_date == response_date
    except AssertionError:
        msg = "Request %s_date for subscription %s, usage point %s \
               doesn't match response: %s %s" % (date_type, subscription_id,
                                                 up_cat, request_date,
                                                 response_date)
        logging.warn(msg)


def get_max_date(usage_point_category, current_datetime=arrow.utcnow(), end_of_day_hour_in_local_time=7, hour_data_updated=18):
    '''
    Return the max date for which we can get data: either
    yesterday's data, if it has been made available already,
    or the previous day's data.

    By default, assume that data was gathered in US/Pacific,
    so the max date for yesterday's data it today at 06:59 hours, since US/Pacific is usually UTC - 7 hours. Also,
    assume that data is made available 2 hours after
    day ends, by default.
    TODO: make this daylight savings resilient...
    TODO: check that a day of data always end at 0700,
    regardless of time of year. (looks like it doesn't.. used to end at 0800.)
    TODO: double check when hour data is updated. (looks like it doesn't all get updated at once, and not until sometime after 3pm..)
    '''
    # Time today that yesterday's data is made available.
    data_refresh_time = current_datetime.replace(hour=hour_data_updated, minute=0, second=0, microsecond=0)
    # If yesterday's data has been made available...
    if current_datetime > data_refresh_time:
        # Return max date that will retrieve yesterday's data, likely uploaded
        # this morning.
        max_date = data_refresh_time.replace(hour=end_of_day_hour_in_local_time - 1, minute=59, second=59)  # If you set max-date one second after this, you might get the next day.
    # If it hasn't been made available yet...
    else:
        # Return max date for will retrieve the day before yesterday's data,
        # likely uploaded yesterday.
        max_date = data_refresh_time.replace(days=-1, hour=end_of_day_hour_in_local_time - 1, minute=59, second=59)
    # Usage category 1 starts at 07:00:01, so set max date to 7:00:00.
    if usage_point_category == '1':
        max_date = max_date.replace(seconds=+1)
    return max_date


def get_previous_downloads(project_id, up_id, bucket):
    '''
    Return full paths of usage dataset in storage service bucket
    that belong to the given project-usage point combination.
    e.g. These are the paths to all the files we've downloaded of
    J Smith's gas usage.
    '''
    for path in bucket:
        # Parse filename, see if it belongs to project-usage point.
        filename = os.path.basename(path)
        path_proj_id, path_up_id, run_num = filename.split('_')
        if str(project_id) == path_project_id \
           and str(up_id) == path_up_id:
            yield path


def path_to_run_num(path):
    '''Extract the run number from a usage file path.'''
    return py.path.local(path).purebasename.split('_')[-1]


def get_last_run_num(paths):
    '''Get the most recent max_date for a set of usage file paths.'''
    run_numbers = [path_to_num_num(path) for path in paths]
    return sorted(run_numbers)[-1]


def get_min_date(subscription_id, usage_point_category, run_rum, target):
    '''
    Get min date for API call. If we've previously downloaded
    data for the subscriber/usage point, pick up where we left off.
    Otherwise, go back as far as you can.
    '''
    if run_num != 0:
        # If we already downloaded data, current request should ask for
        # period starting immediately after the end of the last request.
        filename = '{}_{}_{}.xml'.format(project_id, usage_point['id'], run_rum)
        # TODO fix path
        with target('gs://oeem-renew-financial-data/' + 'consumption/raw/' + filename).open('r') as f:
            usage_xml = f.read()

        readings = parse_usage_data(usage_xml, usage_point['cat'])
        actual_min_date, actual_max_date = find_reading_date_range(readings)

        # Assumes that previous API calls always get full 24 hours
        # of data, from 07:00:00 (or 7:00:01, if usage cat 1) on
        # day t, to the same time on day t+1.
        assert actual_max_date.hour == 7 and actual_max_date.minute == 0 and (actual_max_date.second == 0 or actual_max_date.second == 1)  # TODO: generalize for all ESPI APIs.
        return actual_max_date

        # If not, get everything we can. (4 years from PG&E).
    else:
        # Time in max date is one second before next day, must
        # add 1s so it can serve as min date.
        # i.e. 06:59:59 (end of one day) -> 07:00:00 (start of next one)
        return get_max_date(usage_point_category).replace(years=-4, seconds=+1)


def fetch_customer_usage(customer, usage_point, run_num, espi, target):
    subscription_id = customer['subscription_id']
    access_token = customer['access_token']
    # Figure out what date range to pull for this subscriber usage point.
    # Will download last 4 years for new customers, new data since
    # last download for existing customers.
    # TODO rewrite min_date
    min_date = get_min_date(customer['project_id'], usage_point['id'], run_num, target)
    max_date = get_max_date(usage_point['cat'])
    # Get the usage data.
    usage_xml = espi.fetch_usage_data(subscription_id, usage_point['id'],
                                      access_token, min_date, max_date)
    # Find actual min and max date.
    readings = parse_usage_data(usage_xml, usage_point['cat'])
    # Only proceed if you got readings back.
    if len(readings) > 0:
        actual_min_date, actual_max_date = find_reading_date_range(readings)

        # Log if we got back usage data for a different day than we asked for.
        check_dates(min_date, actual_min_date, 'min', subscription_id, usage_point['cat'])
        check_dates(max_date, actual_max_date, 'max', subscription_id, usage_point['cat'])
        # Return actual dates you got data back for, just in case
        # customer doesn't have data going all the way back, or
        # your max-date is earlier than you asked for because
        # data isn't available yet.
        yield usage_xml
    else:
        logging.warn('No readings for {} {} from {} to {}'.format(subscription_id,
                                                                  usage_point['cat'],
                                                                  min_date, max_date))


def get_run_num(customer, usage_point, raw_consumption_paths):
    previous_downloads = list(get_previous_downloads(customer['project_id'],
                                                     usage_point['cat'],
                                                     raw_consumption_paths))
    if len(previous_downloads) > 0:
        return get_last_run_num(previous_downloads) + 1
    else:
        return 0
