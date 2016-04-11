import os
import arrow
import urllib
import requests
import logging
import py
from dateutil import parser
import xml.etree.cElementTree as etree

from .parsers import parse_usage_data


def client_project_keys(s3_bucket, client_path):
    '''
    Return key objects in s3 bucket that are stored
    under given path, associated with a particular client.
    '''
    for k in s3_bucket:  # boto bucket object.
        if client_path in str(k.key):
            yield k


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
        return [str(customer['id']) for customer in customers[:10]]

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
                # Datetime needed for downstream date comparison.
                customer['date_joined'] = parser.parse(customer['date_joined'])
                # Customer's subcription id lives in resource uri.
                customer['subscription_id'] = customer['resource_uri'].split('/')[-1]
                yield customer


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

    def fetch_usage_points(self, subscription_id, access_token):
        '''
        Get the id and ServiceCategory kind of customer's usage points
        from ESPI API.
        '''
        endpoint = 'Subscription/{}/UsagePoint'.format(subscription_id)
        subscription_url = self.api_url + endpoint
        usage_point_xml = requests.get(subscription_url,
                                       headers=self.make_headers(access_token),
                                       cert=self.cert).text
        for usage_point_id, usage_point_kind in self.parse_usage_points(usage_point_xml):
            yield usage_point_id, usage_point_kind

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
        for usage_point_id, usage_point_cat in self.fetch_usage_points(subscription_id, access_token):
            yield self.fetch_usage_data(subscription_id, usage_point_id,
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
    return min_start, max_end


def check_dates(request_date, response_date, date_type, subscription_id, up_cat):
    '''Check whether the date your asked for is the date you got.'''
    try:
        # Data params in the API are inclusive. If your day starts at 0700,
        # and you set max-date to 0700, you'll get back the day.
        # To get the previous day, set it to 0659. The max end date returned
        # will be 0700, however, so you must adjust the request time
        # to do a correct comparison in this case.
        if date_type == 'max':
            request_date = request_date.replace(seconds=+1)
        # Also, usage point cat 1 starts one second after the hour.
        if up_cat == '1':
            request_date = request_date.replace(seconds=+1)
        assert request_date == response_date
    except AssertionError:
        msg = "Request %s_date for subscription %s, usage point %s \
               doesn't match response: %s %s" % (date_type, subscription_id,
                                                 up_cat, request_date,
                                                 response_date)
        logging.warn(msg)


def get_max_date(current_datetime=arrow.utcnow(), end_of_day_hour_in_local_time=7, hour_data_updated=9):
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
        # Return max date that will retrieve yesterday's data.
        return data_refresh_time.replace(hour=end_of_day_hour_in_local_time - 1, minute=59, second=59)
    # If it hasn't been made available yet...
    else:
        # Return max date for will retrieve the day before yesterday's data.
        return data_refresh_time.replace(days=-1, hour=end_of_day_hour_in_local_time - 1, minute=59, second=59)


def get_previous_downloads(subscription_id, up_cat, bucket):
    '''
    Return full paths of usage dataset in s3 bucket
    that belong to the given subscriber-usage point combination.
    e.g. These are the paths to all the files we've downloaded of
    J Smith's gas usage.
    '''
    for path in bucket:
        # Parse filename, see if it belongs to subscriber-usage point.
        filename = os.path.basename(path)
        path_sub_id, path_up_cat, min_date, max_date = filename.split('_')
        if str(subscription_id) == path_sub_id \
           and str(up_cat) == path_up_cat:
            yield path


def path_to_max_date(path):
    '''Extract the max date from a usage file path.'''
    max_date_str = py.path.local(path).purebasename.split('_')[-1]
    return arrow.get(max_date_str)


def get_last_download_date(paths):
    '''Get the most recent max_date for a set of usage file paths.'''
    parsed_filenames = [path_to_max_date(path) for path in paths]
    return sorted(parsed_filenames)[-1]


def get_min_date(subscription_id, usage_point_category, bucket):
    '''
    Get min date for API call. If we've previously downloaded
    data for the subscriber/usage point, pick up where we left off.
    Otherwise, go back as far as you can.
    '''
    # Check S3 for previous usage data downloads for this
    # subscriber and usage point.
    previous_downloads = list(get_previous_downloads(subscription_id,
                                                     usage_point_category,
                                                     bucket))
    # If we already downloaded data, current request should ask for
    # period starting immediately after the end of the last request.
    if len(previous_downloads) > 0:
        last_dt = get_last_download_date(previous_downloads)
        # Assumes that previous API calls always get full 24 hours
        # of data, from 7am on day t, to 6:59 on day t+1.
        assert last_dt.hour == 7 and (last_dt.second == 0 or last_dt.second == 1)  # TODO: generalize for all ESPI APIs.
        return last_dt.replace(hour=7, minute=0)
    # If not, get everything we can. (4 years from PG&E).
    else:
        max_date = get_max_date()
        min_date = arrow.get(max_date.year - 4, max_date.month, max_date.day, 7)
        if usage_point_category == '1':
            min_date.replace(seconds=+1)
        return min_date


def fetch_all_customer_usage(subscription_id, access_token, espi, bucket):
    for up_id, up_cat in espi.fetch_usage_points(subscription_id, access_token):
        # Figure out what date range to pull for this subscriber usage point.
        # Will download last 4 years for new customers, new data since
        # last download for existing customers.
        # TODO: TEST THIS.
        min_date = get_min_date(subscription_id, up_cat, bucket)
        max_date = get_max_date()
        usage_xml = espi.fetch_usage_data(subscription_id, up_id,
                                          access_token, min_date, max_date)
        # Find actual min and max date.
        readings = parse_usage_data(usage_xml, up_cat)
        # Only proceed if you got readings back.
        if len(readings) > 0:
            actual_min_date, actual_max_date = find_reading_date_range(readings)

            # Log if we got back usage data for a different day than we asked for.
            check_dates(min_date, actual_min_date, 'min', subscription_id, up_cat)
            check_dates(max_date, actual_max_date, 'max', subscription_id, up_cat)

            yield up_cat, actual_min_date, actual_max_date
        else:
            logging.warn('No readings for {} {} from {} to {}'.format(subscription_id,
                                                                      up_cat,
                                                                      min_date,
                                                                      max_date))

